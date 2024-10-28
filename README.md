# fugue-mptp

Generic (multi-process) task processor. Tasks are processed on the main thread
of each worker process; useful for parallelising IDALIB, Python/Rust-based
analyses, etc.

## Example

```rust
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
```
