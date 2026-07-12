use std::path::Path;

use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{Config, resp::Resp};

#[derive(Debug)]
pub struct Aof {
    dir: String,
    appendonly: String,
    appenddirname: String,
    appendfilename: String,
    appendfsync: String,
    aof: Option<File>,
}

impl Aof {
    pub async fn from_config(config: &Config) -> Self {
        let (aof_filename, aof_handle) = if config.appendonly == "yes" {
            // Create dir if not exists
            let dirname = format!("{}/{}/", config.dir, config.appenddirname);
            Aof::create_dir(&dirname).await;

            // Create file if not exists
            let base_filename = format!("{}.1.incr.aof", config.appendfilename);
            let filename = format!("{}{}", dirname, base_filename);
            Aof::create_aof_file(&filename).await;

            // Create manifest file if not exists
            let mf_base_filename = format!("{}.manifest", config.appendfilename);
            let mf_filename = format!("{}{}", dirname, mf_base_filename);
            Aof::create_manifest_file(&mf_filename, &base_filename).await;

            // Read AOF filename from manifest:
            let aof_filename = Aof::get_aof_filename(&mf_filename).await;
            // Open file for writing, appending:
            let file = OpenOptions::new()
                .write(true)
                .append(true)
                .create(true)
                .open(Path::new(&aof_filename))
                .await
                .unwrap();
            (aof_filename, Some(file))
        } else {
            (config.appendfilename.clone(), None)
        };

        Aof {
            dir: config.dir.clone(),
            appendonly: config.appendonly.clone(),
            appenddirname: config.appenddirname.clone(),
            appendfilename: aof_filename,
            appendfsync: config.appendfsync.clone(),
            aof: aof_handle,
        }
    }

    pub async fn create_dir(dirname: &str) {
        let path = Path::new(&dirname);
        if Path::is_dir(path) {
            println!("[aof] dir {:?} already exist", dirname);
        } else {
            let _ = fs::create_dir(path)
                .await
                .expect(&format!("[aof] could not create dir {}", dirname));
        }
    }

    pub async fn create_aof_file(filename: &str) {
        if !Path::exists(&Path::new(&filename)) {
            let _ = File::create(filename).await.unwrap();
        }
    }

    pub async fn create_manifest_file(mf_filename: &str, aof_base_filename: &str) {
        if !Path::exists(&Path::new(&mf_filename)) {
            let line = format!("file {} seq 1 type i", aof_base_filename);
            let mut mf_file = File::create(mf_filename.clone()).await.unwrap();
            let _ = mf_file.write_all(line.as_bytes()).await.unwrap();
        }
    }

    pub async fn get_aof_filename(mf_filename: &str) -> String {
        let mut mf_file = File::open(&Path::new(&mf_filename)).await.unwrap();
        let mut buffer = String::new();
        let _ = mf_file.read_to_string(&mut buffer).await.unwrap();
        let aof_filename = buffer.split(' ').nth(1).unwrap();
        println!("[aof] read aof file from manifest: {}", aof_filename);
        aof_filename.to_string()
    }

    pub async fn debug_file(&mut self) {
        let mut s = String::new();
        let r = self.aof.as_mut().unwrap().read_to_string(&mut s).await;
        println!("[aof] DEBUG: r = {:?}", r);
        println!("[aof] DEBUG: content = {}", s);
    }

    pub async fn append(&mut self, r: Resp) {
        match self.aof {
            Some(ref mut file) => {
                let bytes = r.to_bytes();
                let res = file.write_all(&bytes).await;
                println!("[aof] writing resp: {:?}, bytes: {:?}", r, bytes);
                println!("[aof] writing resp: success: {:?}", res);
                self.debug_file().await;
            }
            None => {
                println!("[aof] no aof file - no append, this is ok");
            }
        }
    }
}
