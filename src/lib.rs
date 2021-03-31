use std::{fs::File, path::PathBuf, time::{SystemTime, UNIX_EPOCH}};

const NORM_CHUNK_SIZE: usize = 1000;

struct TsDbConfig {
    pub fs_path: Option<PathBuf>,
    pub wal_path: Option<PathBuf>,
    pub max_chunk_size: Option<usize>,
}

struct TsDb<T, V> {
    head: Vec<(T, V)>,
    wal: File,
    mmap: Vec<Vec<(T, V)>>,
    chunk_len: usize,
    config: TsDbConfig,
}

impl<T: Clone, V: Clone> TsDb<T, V> {
    pub fn new(mut config: TsDbConfig) -> Self {
        let wal_path = match config.wal_path.clone() {
            Some(config) => config.clone(),
            None => "tmp/wal".parse().unwrap()
        };

        if config.max_chunk_size.is_none() {
            config.max_chunk_size = Some(NORM_CHUNK_SIZE);
        }

        Self {
            config,
            head: vec![],
            mmap: vec![],
            wal: match File::open(wal_path.clone()) {
                Ok(file) => file,
                Err(_) => File::create(wal_path).unwrap()
            },
            chunk_len: 0
        }
    }

    pub fn insert(&mut self, time: T, value: V) {
        if self.config.max_chunk_size > Some(self.chunk_len) {
            self.head.push((time, value)); 
        } else {
            self.mmap.push(self.head.clone());
            self.head = vec![(time, value)]
        }
    }
}

#[test]
fn create_tsdb_test() {
    let mut db = TsDb::<f64, i32>::new(TsDbConfig {
        fs_path: Some("tmp/db".parse().unwrap()),
        wal_path: Some("tmp/wal".parse().unwrap()),
        max_chunk_size: Some(1000)
    });

    let time = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs_f64();
    db.insert(time, 1000);
}
