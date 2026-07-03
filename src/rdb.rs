use std::{collections::HashMap, io::Error};

use tokio::fs;

use crate::parser::*;

#[derive(Debug)]
pub enum RdbEntryExpiration {
    None,
    Seconds(u32),
    Milliseconds(u64),
}

#[derive(Debug)]
pub struct RdbEntry {
    encoding: u8,
    name: String,
    value: String,
    expire: RdbEntryExpiration,
}

#[derive(Debug)]
pub struct Rdb {
    data: Vec<RdbEntry>,
}

fn parse_rdb_entry<'a>() -> impl Parser<'a, RdbEntry> {
    move |input: ParserInput<'a>| {
        if let Ok(((encoding, name, value), rest)) =
            and!(byte(0x00), le_string(), le_string()).parse(input)
        {
            return Ok((
                RdbEntry {
                    encoding,
                    name,
                    value,
                    expire: RdbEntryExpiration::None,
                },
                rest,
            ));
        }

        if let Ok(((_, expire, encoding, name, value), rest)) =
            and!(byte(0xFC), take(8), byte(0x00), le_string(), le_string()).parse(input)
        {
            return Ok((
                RdbEntry {
                    encoding,
                    name,
                    value,
                    expire: RdbEntryExpiration::Milliseconds(u64::from_le_bytes(
                        expire.try_into().unwrap(),
                    )),
                },
                rest,
            ));
        }

        if let Ok(((_, expire, encoding, name, value), rest)) =
            and!(byte(0xFD), take(4), byte(0x00), le_string(), le_string()).parse(input)
        {
            return Ok((
                RdbEntry {
                    encoding,
                    name,
                    value,
                    expire: RdbEntryExpiration::Seconds(u32::from_le_bytes(
                        expire.try_into().unwrap(),
                    )),
                },
                rest,
            ));
        }

        Err(ParseError {
            message: "[parse_rdb_entry] not found".to_string(),
        })
    }
}

impl Rdb {
    pub fn new() -> Self {
        Rdb { data: Vec::new() }
    }

    pub async fn read_from_file(path: &String) -> Result<Self, Error> {
        let bytes = fs::read(path).await?;
        let input = &bytes[..];

        // Read header: REDIS + 4 bytes version
        let ((redis, version), rest) = and!(tag_str("REDIS"), take(4)).parse(input).unwrap();
        println!("[RDB] REDIS            : {}", redis);
        println!("[RDB] VERSION          : {:?}", version);

        // Metadata section, starts with FA
        let (metadata_section, rest) = byte(0xFA).parse(rest).unwrap();
        println!("[RDB] MD Section       : {}", metadata_section);
        let (metadata_kvs, rest) = many0(and!(le_string(), le_string())).parse(rest).unwrap();
        println!("[RDB] MD KVs           : {:?}", metadata_kvs);

        // Database section, starts with FE
        let (db_section, rest) = byte(0xFE).parse(rest).unwrap();
        println!("[RDB] DB Section       : {:?}", db_section);
        let (db_index, rest) = le_integer().parse(rest).unwrap();
        println!("[RDB] DB Index         : {:?}", db_index);
        let (db_hash_section, rest) = byte(0xFB).parse(rest).unwrap();
        println!("[RDB] DB Hash Section  : {:?}", db_hash_section);
        let (db_kvs_size, rest) = le_integer().parse(rest).unwrap();
        println!("[RDB] DB KVS Size      : {:?}", db_kvs_size);
        let (db_expires_size, rest) = le_integer().parse(rest).unwrap();
        println!("[RDB] DB expires size  : {:?}", db_expires_size);
        //let (db_kvs, rest) = many0(and!(le_string(), le_string())).parse(rest).unwrap();
        let (db_entries, rest) = many0(parse_rdb_entry()).parse(rest).unwrap();
        println!("[RDB] DB entries       : {:?}", db_entries);

        // End of file section
        let (eof_section, rest) = byte(0xFF).parse(rest).unwrap();
        println!("[RDB] End Section      : {:?}", eof_section);
        let (crc64, rest) = take(8).parse(rest).unwrap();
        println!("[RDB] CRC64            : {:?}", crc64);

        Ok(Rdb { data: db_entries })
    }

    pub fn keys(&self) -> Vec<String> {
        self.data.iter().map(|e| e.name.clone()).collect()
    }
}
