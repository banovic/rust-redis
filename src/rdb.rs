use std::{collections::HashMap, io::Error};

use tokio::fs;

use crate::{
    parser::*,
    types::{Bytes, Key},
};

#[derive(Debug)]
pub enum RdbValueExpiration {
    None,
    Seconds(u32),
    Milliseconds(u64),
}

#[derive(Debug)]
pub struct RdbString {
    pub encoding: u8,
    pub value: Bytes,
    pub expire: RdbValueExpiration,
}

impl RdbString {
    pub fn serialize(&self, key: &Key) -> Vec<u8> {
        // Expire, if any
        let mut out = match self.expire {
            RdbValueExpiration::None => Vec::new(),
            RdbValueExpiration::Milliseconds(ms) => {
                let mut v = vec![0xFC];
                v.extend(ms.to_le_bytes());
                v
            }
            RdbValueExpiration::Seconds(secs) => {
                let mut v = vec![0xFD];
                v.extend(secs.to_le_bytes());
                v
            }
        };
        // Type and encoding byte
        out.push(self.encoding);
        // Key and value
        out.extend(le_encode_size(key.0.len()));
        out.extend_from_slice(&key.0);
        out.extend(le_encode_size(self.value.len()));
        out.extend_from_slice(&self.value);
        out
    }
}

#[derive(Debug)]
pub struct Rdb {
    pub metadata: HashMap<String, String>,
    pub data: HashMap<Key, RdbString>,
}

fn parse_rdb_entry<'a>() -> impl Parser<'a, (Key, RdbString)> {
    move |input: ParserInput<'a>| {
        if let Ok(((encoding, key, value), rest)) =
            and!(byte(0x00), le_bytes(), le_bytes()).parse(input)
        {
            return Ok((
                (
                    Key(key),
                    RdbString {
                        encoding,
                        value,
                        expire: RdbValueExpiration::None,
                    },
                ),
                rest,
            ));
        }

        if let Ok(((_, expire, encoding, key, value), rest)) =
            and!(byte(0xFC), take(8), byte(0x00), le_bytes(), le_bytes()).parse(input)
        {
            return Ok((
                (
                    Key(key),
                    RdbString {
                        encoding,
                        value,
                        expire: RdbValueExpiration::Milliseconds(u64::from_le_bytes(
                            expire.try_into().unwrap(),
                        )),
                    },
                ),
                rest,
            ));
        }

        if let Ok(((_, expire, encoding, key, value), rest)) =
            and!(byte(0xFD), take(4), byte(0x00), le_bytes(), le_bytes()).parse(input)
        {
            return Ok((
                (
                    Key(key),
                    RdbString {
                        encoding,
                        value,
                        expire: RdbValueExpiration::Seconds(u32::from_le_bytes(
                            expire.try_into().unwrap(),
                        )),
                    },
                ),
                rest,
            ));
        }

        Err(ParseError {
            message: "[parse_rdb_entry] not found".to_string(),
        })
    }
}

fn parse_db_subsection<'a>() -> impl Parser<'a, Vec<(Key, RdbString)>> {
    move |input: ParserInput<'a>| {
        // Database sub-section, starts with FE
        let (_db_section, rest) = byte(0xFE).parse(input)?;
        let (_db_index, rest) = le_integer().parse(rest)?;
        let (_db_hash_section, rest) = byte(0xFB).parse(rest)?;
        let (_db_kvs_size, rest) = le_integer().parse(rest)?;
        let (_db_expires_size, rest) = le_integer().parse(rest)?;
        let (db_entries, rest) = many0(parse_rdb_entry()).parse(rest)?;
        Ok((db_entries, rest))
    }
}

fn parse_metadata_subsection<'a>() -> impl Parser<'a, (String, String)> {
    move |input: ParserInput<'a>| {
        let (_metadata_section, rest) = byte(0xFA).parse(input)?;
        let (metadata_kv, rest) = and!(le_string(), le_string()).parse(rest)?;
        Ok((metadata_kv, rest))
    }
}

// Length-Encoded (LE) string; this can be more complex than le_integer()
pub fn le_string<'a>() -> impl Parser<'a, String> {
    move |input: ParserInput<'a>| {
        let first = input[0];
        match (first & 0b1100_0000) >> 6 {
            0b00 => {
                let start = 1;
                let length = (first & 0b0011_1111) as usize;
                let end = start + length;
                assert!(input.len() >= end);
                let s = String::from_utf8(input[start..end].to_vec()).unwrap();
                Ok((s, &input[end..]))
            }
            0b01 => {
                let start = 2;
                let a = first & 0b0011_1111;
                let b = input[1];
                let length = ((a as usize) << 8) | (b as usize);
                let end = start + length;
                assert!(input.len() >= end);
                let s = String::from_utf8(input[start..end].to_vec()).unwrap();
                Ok((s, &input[end..]))
            }
            0b10 => {
                let start = 5;
                assert!(input.len() >= 5);
                let length = ((input[1] as usize) << 24)
                    | ((input[2] as usize) << 16)
                    | ((input[3] as usize) << 8)
                    | (input[4] as usize);
                let end = start + length;
                assert!(input.len() >= end);
                let s = String::from_utf8(input[start..end].to_vec()).unwrap();
                Ok((s, &input[end..]))
            }
            0b11 if first == 0b1100_0000 => {
                // String is integer of length 1
                assert!(input.len() >= 2);
                let s = String::from_utf8(vec![input[1]]).unwrap();
                Ok((s, &input[2..]))
            }
            0b11 if first == 0b1100_0001 => {
                // String is integer of length 2
                assert!(input.len() >= 3);
                let s = String::from_utf8(vec![input[2], input[1]]).unwrap();
                Ok((s, &input[3..]))
            }
            0b11 if first == 0b1100_0010 => {
                // String is integer of length 4
                assert!(input.len() >= 5);
                let s = String::from_utf8(vec![input[4], input[3], input[2], input[1]]).unwrap();
                Ok((s, &input[5..]))
            }
            _ => Err(ParseError {
                message: format!("[le_string] unknown length prefix: {:?}", first),
            }),
        }
    }
}

fn le_encode_string(s: &str) -> Vec<u8> {
    let mut out = le_encode_size(s.as_bytes().len());
    out.extend_from_slice(s.as_bytes());
    out
}

fn le_encode_size(n: usize) -> Vec<u8> {
    if n <= 0b11_1111 {
        return vec![n as u8];
    }

    if n <= 0b11_1111_1111_1111 {
        let a = 0b0100_0000 | (n >> 8) as u8;
        let b = n as u8;
        return vec![a, b];
    }

    let m = 0b10_00_0000;
    let l = (n as u32).to_be_bytes();
    let mut enc = vec![m];
    enc.extend(l);
    enc
}

impl Rdb {
    pub fn new() -> Self {
        Rdb {
            metadata: HashMap::new(),
            data: HashMap::new(),
        }
    }

    pub async fn read_from_file(path: &String) -> Result<Self, Error> {
        let bytes = fs::read(path).await?;
        Rdb::deserialize(&bytes[..])
    }

    pub fn deserialize(input: &[u8]) -> Result<Self, Error> {
        // Read header: REDIS + 4 bytes version
        let ((_redis, _version), input) = and!(tag_str("REDIS"), take(4)).parse(input).unwrap();

        let (metadata_sub_sections, input) =
            many0(parse_metadata_subsection()).parse(input).unwrap();
        let metadata: HashMap<String, String> = metadata_sub_sections.into_iter().collect();

        let (db_sub_sections, input) = many0(parse_db_subsection()).parse(input).unwrap();

        // End of file section
        let (_eof_section, input) = byte(0xFF).parse(input).unwrap();
        let (_crc64, _) = take(8).parse(input).unwrap();

        let mut data_parts = Vec::new();
        for db in db_sub_sections {
            data_parts.extend(db);
        }
        let data = data_parts.into_iter().collect();

        Ok(Rdb { metadata, data })
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::new();
        // REDIS + version 4 bytes
        out.extend_from_slice(b"REDIS0011");

        // Metadata
        for (k, v) in &self.metadata {
            out.push(0xFA);
            let key_le = le_encode_string(k);
            let value_le = le_encode_string(v);
            out.extend(key_le);
            out.extend(value_le);
        }

        // DB sub sections. Can by many databases in single file, but here only 1 or 0.
        if !self.data.is_empty() {
            out.push(0xFE); // Marks db section
            out.push(0x00); // DB index - corresponds to db

            // Calculate all key values hash size, and only those with expire
            let kvs_size = le_encode_size(self.data.len());
            let expires_size = le_encode_size(
                self.data
                    .values()
                    .filter(|v| !matches!(v.expire, RdbValueExpiration::None))
                    .count(),
            );
            out.push(0xFB); // Marks hash sizes section
            out.extend(kvs_size);
            out.extend(expires_size);

            for (k, v) in &self.data {
                out.extend(v.serialize(k));
            }
        }

        // End of file section
        out.push(0xFF);
        // CRC - no real crc calculation needed
        out.extend(vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        out
    }

    pub fn set(&mut self, key: Key, value: RdbString) -> Option<RdbString> {
        self.data.insert(key, value)
    }
}
