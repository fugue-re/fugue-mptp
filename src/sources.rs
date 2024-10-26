use std::path::{Path, PathBuf};

use thiserror::Error;
use walkdir::WalkDir;

use crate::TaskSource;

pub struct DirectorySource {
    follow_symlinks: bool,
    filter: Option<Box<dyn FnMut(&Path) -> bool + Send>>,
    root: PathBuf,
    walker: Option<walkdir::IntoIter>,
}

#[derive(Debug, Error)]
pub enum DirectorySourceError {
    #[error(transparent)]
    WalkDir(#[from] walkdir::Error),
}

impl DirectorySource {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            follow_symlinks: false,
            filter: None,
            root: PathBuf::from(root.as_ref()),
            walker: None,
        }
    }

    pub fn new_with(
        root: impl AsRef<Path>,
        filter: impl FnMut(&Path) -> bool + Send + 'static,
    ) -> Self {
        Self {
            follow_symlinks: false,
            filter: Some(Box::new(filter) as _),
            root: PathBuf::from(root.as_ref()),
            walker: None,
        }
    }

    pub fn follow_symlinks(&mut self, toggle: bool) {
        self.follow_symlinks = toggle;
    }
}

impl TaskSource for DirectorySource {
    type TaskInput = PathBuf;
    type Error = DirectorySourceError;

    fn next_task(&mut self, _id: uuid::Uuid) -> Result<Option<PathBuf>, Self::Error> {
        let walker = self.walker.get_or_insert_with(|| {
            WalkDir::new(&self.root)
                .follow_links(self.follow_symlinks)
                .into_iter()
        });

        loop {
            let Some(entry) = walker.next() else {
                // reset
                self.walker = None;
                return Ok(None);
            };

            let entry = entry?;

            if !entry.file_type().is_file() {
                continue;
            }

            let path = entry.into_path();

            if self.filter.as_mut().map(|f| f(&path)).unwrap_or(true) {
                return Ok(Some(path));
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::convert::Infallible;

    use super::*;

    use crate::{Error, TaskProcessor, TaskSink};

    #[test]
    fn test_dir() -> Result<(), Error> {
        struct Process;
        struct Sink;

        impl TaskSink for Sink {
            type TaskOutput = String;
            type TaskError = String;

            type Error = Infallible;

            fn process_task_result(
                &mut self,
                id: uuid::Uuid,
                result: Result<String, String>,
            ) -> Result<(), Self::Error> {
                match result {
                    Ok(v) => {
                        println!("task {id} processed successfully (payload: {v})")
                    }
                    Err(v) => {
                        println!("task {id} failed (reason: {v})")
                    }
                }
                Ok(())
            }
        }

        impl TaskProcessor for Process {
            type TaskInput = PathBuf;
            type TaskOutput = String;
            type TaskError = String;

            fn process_task(&mut self, _id: uuid::Uuid, payload: PathBuf) -> Result<String, String> {
                Ok(format!("hello, task:{}", payload.display()))
            }
        }

        let mut source = DirectorySource::new("target");
        let mut sink = Sink;
        let mut process = Process;

        crate::run(&mut source, &mut process, &mut sink)?;

        Ok(())
    }
}
