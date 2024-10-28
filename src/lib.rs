use std::collections::BTreeMap;
use std::process::exit;

use easy_parallel::Parallel;
use ipc_channel::ipc::{
    self, IpcOneShotServer, IpcReceiver, IpcReceiverSet, IpcSelectionResult, IpcSender,
};
use nix::unistd::{fork, ForkResult};
use rand::RngCore;
use thiserror::Error;
pub use uuid::Uuid;

pub mod sources;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    ChannelRecv(#[from] flume::RecvError),
    #[error("cannot send data on closed channel")]
    ChannelSend,
    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),
    #[error("fork failed: {0}")]
    Fork(nix::errno::Errno),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Ipc(#[from] ipc_channel::Error),
    #[error(transparent)]
    TaskSink(anyhow::Error),
    #[error(transparent)]
    TaskSource(anyhow::Error),
    #[error("one or more errors encountered during task processing")]
    Process(Vec<Self>),
}

impl Error {
    pub fn source<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TaskSource(anyhow::Error::from(e))
    }

    pub fn sink<E>(e: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        Self::TaskSink(anyhow::Error::from(e))
    }
}

pub trait TaskData: serde::Serialize + serde::de::DeserializeOwned + Send {}
impl<T> TaskData for T where T: serde::Serialize + serde::de::DeserializeOwned + Send {}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TaskItem<T> {
    id: Uuid,
    payload: T,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct TaskResult<T, E> {
    id: Uuid,
    result: Result<T, E>,
}

pub trait TaskSource: Send {
    type TaskInput: Clone + TaskData;
    type Error: std::error::Error + Send + Sync + 'static;

    fn next_task(&mut self, id: Uuid) -> Result<Option<Self::TaskInput>, Self::Error>;
}

pub trait TaskProcessor: Send {
    type TaskInput: Clone + TaskData;
    type TaskOutput: TaskData;
    type TaskError: TaskData;

    fn process_task(
        &mut self,
        id: Uuid,
        input: Self::TaskInput,
    ) -> Result<Self::TaskOutput, Self::TaskError>;
}

pub trait TaskSink: Send {
    type TaskOutput: TaskData;
    type TaskError: TaskData;

    type Error: std::error::Error + Send + Sync + 'static;

    fn process_task_result(
        &mut self,
        id: Uuid,
        result: Result<Self::TaskOutput, Self::TaskError>,
    ) -> Result<(), Self::Error>;
}

pub fn run<T, U, E>(
    source: &mut impl TaskSource<TaskInput = T>,
    processor: &mut impl TaskProcessor<TaskInput = T, TaskOutput = U, TaskError = E>,
    sink: &mut impl TaskSink<TaskOutput = U, TaskError = E>,
) -> Result<(), Error>
where
    T: Clone + TaskData,
    U: TaskData,
    E: TaskData,
{
    let (result_tx, result_rx) = flume::unbounded::<TaskResult<U, E>>();
    let (child_tx, child_rx) = flume::unbounded::<IpcSender<Option<TaskItem<T>>>>();

    let mut rxs = IpcReceiverSet::new()?;
    let mut txs = BTreeMap::new();

    for _ in 0..num_cpus::get() {
        // NOTE: this is a workaround for a bug in ipc-channel on MacOS. As far as I can tell, if
        // we do not do this, then when a child process constructs a `IpcOneShotServer`, we will
        // have a name clash, since the name is based on the thread_rng which is duplicated and not
        // reseeded across the fork. In theory, this should be handled by retrying with a new name,
        // but for some reason the error returned in the child is not `BOOTSTRAP_NAME_IN_USE`, and
        // rather some other error, which means that the child process bails due to failing to
        // create a server.
        //
        rand::thread_rng().next_u64();

        let (psrv, pn) = IpcOneShotServer::<(String, IpcReceiver<Option<U>>)>::new()?;

        match unsafe { fork() }.map_err(Error::Fork)? {
            ForkResult::Child => {
                // NOTE: we first set-up bi-directional IPC to obtain work and return
                // results.
                //
                let (csrv, cn) = IpcOneShotServer::<IpcReceiver<Option<TaskItem<T>>>>::new()
                    .map_err(Error::Io)?;

                let results = IpcSender::connect(pn)?;

                let (results_tx, results_rx) = ipc::channel::<Option<Vec<u8>>>()?;

                results.send((cn, results_rx))?;

                let (_, task_rx) = csrv.accept()?;

                // NOTE: now we can begin to handle tasks.
                //
                while let Ok(Some(task)) = task_rx.recv() {
                    let results = TaskResult {
                        id: task.id,
                        result: processor.process_task(task.id, task.payload),
                    };
                    results_tx.send(rmp_serde::to_vec(&results).ok())?;
                }

                exit(0);
            }
            ForkResult::Parent { .. } => {
                let (_, (cn, rx)) = psrv.accept()?;

                let tx = IpcSender::<IpcReceiver<Option<TaskItem<T>>>>::connect(cn)?;

                let (ctx, crx) = ipc::channel()?;

                tx.send(crx)?;

                if !child_tx.send(ctx.clone()).is_err() {
                    let child = rxs.add(rx)?;
                    txs.insert(child, Some(ctx));
                }
            }
        }
    }

    let (task_tx, task_rx) = flume::bounded::<TaskItem<T>>(num_cpus::get());

    let (rs, r) = Parallel::new()
        .add(move || {
            let mut id = Uuid::now_v7();
            while let Some(payload) = source.next_task(id).map_err(Error::source)? {
                task_tx
                    .send(TaskItem { id, payload })
                    .map_err(|_| Error::ChannelSend)?;
                id = Uuid::now_v7();
            }

            drop(task_tx);

            Ok::<(), Error>(())
        })
        .add(move || {
            while let Ok(TaskResult { id, result }) = result_rx.recv() {
                sink.process_task_result(id, result).map_err(Error::sink)?;
            }

            Ok::<(), Error>(())
        })
        .add(move || {
            while let Ok(task) = task_rx.recv() {
                loop {
                    let child = child_rx.recv()?;

                    if let Err(_) = child.send(Some(task.clone())) {
                        // cannot send task to child
                        continue;
                    }

                    break;
                }
            }

            for r in child_rx.drain() {
                r.send(None).ok();
                drop(r);
            }

            drop(child_rx);
            drop(task_rx);

            Ok(())
        })
        .finish(move || {
            while let Ok(events) = rxs.select() {
                for event in events {
                    match event {
                        IpcSelectionResult::ChannelClosed(ref child) => {
                            txs.remove(child);
                        }
                        IpcSelectionResult::MessageReceived(ref child, msg) => {
                            let result = msg
                                .to::<Option<Vec<u8>>>()?
                                .map(|v| rmp_serde::from_slice::<TaskResult<U, E>>(&v))
                                .transpose()?;

                            if let Some(ref child) = txs[child] {
                                if child_tx.send(child.clone()).is_err() {
                                    // NOTE: after the first failure, we will not process more
                                    // work, so we clear txs and wait for results.
                                    txs.values_mut().for_each(|ch| {
                                        if let Some(ch) = ch.take() {
                                            ch.send(None).ok();
                                        }
                                    });
                                }
                            }

                            if let Some(result) = result {
                                result_tx.send(result).map_err(|_| Error::ChannelSend)?;
                            }
                        }
                    }
                }

                if txs.is_empty() {
                    // NOTE: all children are complete; we're done!
                    break;
                }
            }

            drop(child_tx);
            drop(txs);
            drop(rxs);

            Ok::<(), Error>(())
        });

    let mut errs = Vec::new();

    if let Err(e) = r {
        errs.push(e);
    }

    errs.extend(rs.into_iter().filter_map(Result::err));

    if errs.is_empty() {
        Ok(())
    } else {
        Err(Error::Process(errs))
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use super::*;

    #[test]
    fn test_simple() -> Result<(), Error> {
        struct Source {
            counter: usize,
            limit: usize,
        }

        struct Process {
            counter: usize,
        }

        struct Sink {
            counter: usize,
        }

        #[derive(Debug, serde::Deserialize, serde::Serialize)]
        struct Payload {
            summary: String,
            count: usize,
        }

        impl TaskSource for Source {
            type TaskInput = String;

            type Error = Infallible;

            fn next_task(&mut self, id: Uuid) -> Result<Option<String>, Self::Error> {
                if self.counter >= self.limit {
                    return Ok(None);
                }

                self.counter += 1;

                Ok(Some(format!("{}:{id}", self.counter)))
            }
        }

        impl TaskSink for Sink {
            type TaskOutput = Payload;
            type TaskError = String;

            type Error = Infallible;

            fn process_task_result(
                &mut self,
                id: Uuid,
                result: Result<Payload, String>,
            ) -> Result<(), Self::Error> {
                match result {
                    Ok(Payload { summary, count }) => {
                        self.counter += 1;
                        println!("task {id} processed successfully (payload: {summary} / {count})")
                    }
                    Err(v) => {
                        println!("task {id} failed (reason: {v})")
                    }
                }
                Ok(())
            }
        }

        impl TaskProcessor for Process {
            type TaskInput = String;
            type TaskOutput = Payload;
            type TaskError = String;

            fn process_task(&mut self, _id: Uuid, payload: String) -> Result<Payload, String> {
                self.counter += 1;

                Ok(Payload {
                    summary: format!("hello, task:{payload}"),
                    count: self.counter,
                })
            }
        }

        let mut source = Source {
            counter: 0,
            limit: 1_000,
        };
        let mut sink = Sink { counter: 0 };
        let mut process = Process { counter: 0 };

        run(&mut source, &mut process, &mut sink)?;

        assert_eq!(sink.counter, 1_000);

        Ok(())
    }
}
