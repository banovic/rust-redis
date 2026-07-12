use std::path::Path;

use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncWriteExt},
};

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
            let base_filename = format!("{}.1.incr.aof", self.appendfilename);
            let filename = format!("{}{}", dirname, base_filename);
            if !Path::exists(&Path::new(&filename)) {
                let mut file = File::create(filename).await.unwrap();
            }

            // Create manifest file
            let mf_base_filename = format!("{}.manifest", self.appendfilename);
            let mf_filename = format!("{}{}", dirname, mf_base_filename);
            if !Path::exists(&Path::new(&mf_filename)) {
                let line = format!("file {} seq 1 type i", base_filename);
                let mut mf_file = File::create(mf_filename.clone()).await.unwrap();
                let _ = mf_file.write_all(line.as_bytes()).await.unwrap();
            }

            // Read manifest file
            let mut mf_file = File::open(&Path::new(&mf_filename)).await.unwrap();
            let mut buffer = String::new();
            let _ = mf_file.read_to_string(&mut buffer).await.unwrap();
            let aof_filename = buffer.split(' ').nth(1).unwrap();
            println!("[aof] read aof file from manifest: {}", aof_filename);
        }
    }
}
