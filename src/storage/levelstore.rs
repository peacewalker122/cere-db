use std::path::PathBuf;

use tokio::sync::mpsc;

use crate::storage::constant::MAXIMUM_LEVEL_FILES;

#[allow(dead_code)]
pub struct Levelstore {
    store: Vec<Vec<PathBuf>>, // This is the in-memory representation of the levelstore, which holds the SSTable filenames as byte vectors. [0] is the first level, [1] is the second level, and so on.
}

#[allow(dead_code)]
impl Levelstore {
    pub fn new() -> Self {
        Levelstore { store: Vec::new() }
    }

    pub fn add_file(
        &mut self,
        level: usize,
        filename: &PathBuf,
        level_signal: mpsc::Sender<Vec<PathBuf>>,
    ) -> Result<(), String> {
        // Ensure the level exists in the store
        while self.store.len() <= level {
            self.store.push(Vec::new());
        }
        self.store[level].push(filename.clone());

        if self.store[level].len() > MAXIMUM_LEVEL_FILES {
            log::info!(
                "Level {} has exceeded the maximum file count, triggering compaction",
                level
            );
            let files_to_compact = self.store[level].clone();

            tokio::spawn(async move {
                if let Err(err) = level_signal.send(files_to_compact).await {
                    log::error!("Failed to send compaction signal: {}", err);
                }
            });
        }

        Ok(())
    }

    pub fn get_files(&self, level: usize) -> Option<&Vec<PathBuf>> {
        self.store.get(level)
    }

    pub fn remove_file(&mut self, level: usize, filename: &[u8]) {
        if let Some(files) = self.store.get_mut(level) {
            let filename_str = String::from_utf8_lossy(filename).to_string();
            files.retain(|path| path.to_string_lossy() != filename_str);
        }
    }
}
