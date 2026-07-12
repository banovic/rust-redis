use std::path::Path;

use tokio::fs::{self, File};

use crate::Config;

#[derive(Debug)]
pub struct Aof {
    dir: String,
    appendonly: String,
    appenddirname: String,
    appendfilename: String,
    appendfsync: String,
}

impl Aof {
    pub fn from_config(config: &Config) -> Self {
        Aof {
            dir: config.dir.clone(),
            appendonly: config.appendonly.clone(),
            appenddirname: config.appenddirname.clone(),
            appendfilename: config.appendfilename.clone(),
            appendfsync: config.appendfsync.clone(),
        }
    }

    pub async fn init(&self) {
        if self.appendonly == "yes" {
            let dirname = format!("{}/{}/", self.dir, self.appenddirname);
            let path = Path::new(&dirname);
            if Path::is_dir(path) {
                println!("[aof] dir {:?} already exist", dirname);
            } else {
                let _ = fs::create_dir(path)
                    .await
                    .expect(&format!("[aof] could not create dir {}", dirname));
            }

            // Create file
            let filename = format!("{}{}.1.incr.aof", dirname, self.appendfilename);
            // println!("[aof] self.appendfilename: {}", self.appendfilename);
            // println!("[aof] creating initial aof: {}", filename);
            let mut file = File::create(filename).await.unwrap();
        }
    }
}
