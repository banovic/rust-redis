pub type Bytes = Vec<u8>;

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub struct Key(pub Vec<u8>);

impl Key {
    pub fn to_str(&self) -> &str {
        str::from_utf8(&self.0).unwrap()
    }
}

pub type StreamKey = (u64, u64);
