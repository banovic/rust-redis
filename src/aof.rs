use std::path::Path;

use tokio::{
    fs::{self, File, OpenOptions, read},
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::{
    Config,
    command::Command,
    resp::{Resp, parse_resp},
};

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
            let aof_filename = Aof::get_aof_filename(&mf_filename).await.unwrap();
            // Open file for writing, appending:
            let aof_absolute_filename =
                format!("{}/{}/{}", config.dir, config.appenddirname, aof_filename);
            let file = OpenOptions::new()
                .write(true)
                .read(true)
                .append(true)
                .create(true)
                .open(Path::new(&aof_absolute_filename))
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
        // Create only if it does not exits:
        if !Path::is_dir(path) {
            fs::create_dir(path)
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
            let mut mf_file = File::create(mf_filename).await.unwrap();
            let _ = mf_file.write_all(line.as_bytes()).await.unwrap();
        }
    }

    pub async fn get_aof_filename(mf_filename: &str) -> Option<String> {
        let mut mf_file = File::open(&Path::new(&mf_filename)).await.unwrap();
        let mut buffer = String::new();
        let _ = mf_file.read_to_string(&mut buffer).await.unwrap();
        for l in buffer.lines() {
            let parts = l.split(' ').collect::<Vec<_>>();
            match (parts.get(1), parts.get(5)) {
                (Some(aof_filename), Some(t)) if *t == "i" => {
                    return Some(aof_filename.to_string());
                }
                _ => {}
            }
        }
        None
    }

    pub async fn append(&mut self, r: Resp) {
        if let Some(ref mut file) = self.aof {
            let bytes = r.to_bytes();
            file.write_all(&bytes)
                .await
                .expect("Failed to write to file");
            let _ = file.flush().await;
        }
    }

    pub async fn get_initial_commands(&self) -> Vec<Command> {
        if self.appendonly == "yes" {
            // `appendfilename` already holds the resolved AOF data filename
            // (from the manifest) as set in `from_config`.
            let aof_filename = format!(
                "{}/{}/{}",
                self.dir, self.appenddirname, self.appendfilename
            );
            let bytes = read(aof_filename).await.unwrap();
            let (resps, _) = parse_resp(&bytes).unwrap();
            resps
                .iter()
                .map(|r| Command::from_resp(r).unwrap())
                .collect::<Vec<Command>>()
        } else {
            vec![]
        }
    }
}
