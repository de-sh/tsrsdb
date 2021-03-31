use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

const NORM_CHUNK_SIZE: usize = 1000;

struct TsDbConfig {
    pub db_path: Option<PathBuf>,
    pub wal_path: Option<PathBuf>,
    pub max_chunk_size: Option<usize>,
}

/// Maintains all necessary info about the system
struct TsDb<T, V> {
    /// A chunk of memory to hold incoming data
    head: Vec<(T, V)>,
    /// A peristant Write-ahead-log to keep track of all incoming data
    wal: File,
    /// A pointer for mem-mapped peristant storage
    mmap: File,
    /// To figure if the chunk in head is reaching capacity
    chunk_len: usize,
    /// Configuration details used in the creation of the database
    config: TsDbConfig,
}

impl<T: Clone, V: Clone> TsDb<T, V> {
    pub fn new(mut config: TsDbConfig) -> Self {
        let wal_path = match config.wal_path.clone() {
            Some(config) => config.clone(),
            None => "tmp/wal".parse().unwrap(),
        };

        let db_path = match config.db_path.clone() {
            Some(config) => config.clone(),
            None => "tmp/db".parse().unwrap(),
        };

        if config.max_chunk_size.is_none() {
            config.max_chunk_size = Some(NORM_CHUNK_SIZE);
        }

        Self {
            config,
            head: vec![],
            mmap: match OpenOptions::new().append(true).open(db_path.clone()) {
                Ok(file) => file,
                Err(_) => OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(db_path)
                    .unwrap(),
            },
            wal: match OpenOptions::new().append(true).open(wal_path.clone()) {
                Ok(file) => file,
                Err(_) => OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(wal_path)
                    .unwrap(),
            },
            chunk_len: 0,
        }
    }

    /// Add data to the chunk and if necessary, push chunk into mem-mapped peristant storage
    pub fn insert(&mut self, time: T, value: V) {
        if self.config.max_chunk_size > Some(self.chunk_len) {
            self.head.push((time, value));
        } else {
            // TODO: Write contents to file/persistant storage
            // self.mmap.write(self.head.as_bytes());
            self.head = vec![(time, value)]
        }
    }
}

#[test]
fn create_tsdb_and_insert_test() {
    let mut db = TsDb::<f64, i32>::new(TsDbConfig {
        db_path: Some("tmp/db".parse().unwrap()),
        wal_path: Some("tmp/wal".parse().unwrap()),
        max_chunk_size: Some(1000),
    });

    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64();
    db.insert(time, 1000);
}
