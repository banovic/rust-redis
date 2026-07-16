use std::{
    collections::{BTreeMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{command::Command::ReplconfGetAck, types::*};
use crate::{parser::*, resp::Resp};

// milliseconds-seqeunce id
#[derive(Debug, Clone, Copy)]
pub enum XaddStreamIdInput {
    Explicit(u64, u64),
    AutoGenSeq(u64),
    AugoGen,
}
#[derive(Debug, Clone, Copy)]
pub enum XreadStreamIdInput {
    Explicit(u64, u64),
    DollarId,
}

type StreamKey = (u64, u64);

fn parse_input_stream_id<'a>(id: &'a Vec<u8>) -> Option<XaddStreamIdInput> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((tid, _, sid), _)) => Some(XaddStreamIdInput::Explicit(tid, sid)),
        _ => match and!(integer::<u64>(), byte(b'-'), byte(b'*')).parse(id) {
            Ok(((tid, _, _), _)) => Some(XaddStreamIdInput::AutoGenSeq(tid)),
            _ => match byte(b'*').parse(id) {
                Ok(_) => Some(XaddStreamIdInput::AugoGen),
                _ => None,
            },
        },
    }
}

fn parse_xread_stream_id_input<'a>(id: &'a Vec<u8>) -> Option<XreadStreamIdInput> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((tid, _, sid), _)) => Some(XreadStreamIdInput::Explicit(tid, sid)),
        _ => match byte(b'$').parse(id) {
            Ok(_) => Some(XreadStreamIdInput::DollarId),
            _ => None,
        },
    }
}

pub fn next_stream_id(
    ski: XaddStreamIdInput,
    stream: &BTreeMap<StreamKey, Vec<Bytes>>,
) -> Option<(u64, u64)> {
    let latest = stream.last_key_value();
    match ski {
        XaddStreamIdInput::Explicit(tid, sid) => Some((tid, sid)),
        XaddStreamIdInput::AutoGenSeq(tid) => {
            if latest.is_some() {
                let (&(orig_tid, orig_sid), _) = latest.unwrap();
                if tid > orig_tid {
                    Some((tid, 0))
                } else if tid == orig_tid {
                    Some((tid, orig_sid + 1))
                } else {
                    None
                }
            } else {
                if tid == 0 {
                    Some((tid, 1))
                } else {
                    Some((tid, 0))
                }
            }
        }
        XaddStreamIdInput::AugoGen => {
            let tid = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("Time went backwards")
                .as_millis() as u64;
            match latest {
                Some((&(orig_tid, _), _)) if orig_tid < tid => Some((tid, 0)),
                Some((&(orig_tid, orig_sid), _)) if orig_tid == tid => Some((tid, orig_sid + 1)),
                Some((&(orig_tid, _), _)) if orig_tid > tid => None,
                None => Some((tid, 0)),
                _ => panic!("No idea?"),
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum Command {
    Echo {
        message: Bytes,
    },
    Ping {
        message: Option<Bytes>,
    },
    Set {
        key: Key,
        value: Bytes,
        ex: Option<u64>,
        px: Option<u64>,
    },
    Get {
        key: Key,
    },
    Rpush {
        key: Key,
        elements: Vec<Bytes>,
    },
    Lrange {
        key: Key,
        start: i32,
        end: i32,
    },
    Lpush {
        key: Key,
        elements: Vec<Bytes>,
    },
    Llen {
        key: Key,
    },
    Lpop {
        key: Key,
        count: Option<u32>,
    },
    Blpop {
        keys: Vec<Key>,
        timeout: f64,
    },
    Type {
        key: Key,
    },
    Xadd {
        key: Key,
        id: XaddStreamIdInput,
        field_values: Vec<Bytes>, // field1 value1 ... fieldN valueN
    },
    Xrange {
        key: Key,
        start: (u64, u64),
        end: (u64, u64),
    },
    Xread {
        keys: Vec<Key>,
        milliseconds: Option<u64>,
        ids: Vec<XreadStreamIdInput>,
    },
    Incr {
        key: Key,
    },
    Multi,
    Exec,
    Discard,
    Watch {
        keys: Vec<Key>,
    },
    Unwatch,
    InternalExecuteTx {
        commands: Vec<Command>,
    },
    Info {
        section: Option<Bytes>,
    },
    ReplconfListeningPort {
        port: u16,
    },
    ReplconfCapa {
        capabilites: Vec<Bytes>,
    },
    Psync {
        replication_id: String,
        offset: i64,
    },
    ReplconfGetAck,
    ReplconfAck {
        ack_bytes: u64,
    },
    Wait {
        numreplicas: u64,
        timeout: u64,
    },
    ConfigGet {
        parameter: String,
    },
    Keys {
        pattern: String,
    },
    Subscribe {
        channels: Vec<String>,
    },
    Publish {
        channel: String,
        message: String,
    },
    Unsubscribe {
        channels: Vec<String>,
    },
    // Sorted sets
    Zadd {
        key: String,
        score: f64,
        member: String,
    },
}

impl Command {
    /// Canonical command name, lowercase, for error messages.
    pub fn name(&self) -> &'static str {
        match self {
            Command::Echo { .. } => "echo",
            Command::Ping { .. } => "ping",
            Command::Set { .. } => "set",
            Command::Get { .. } => "get",
            Command::Rpush { .. } => "rpush",
            Command::Lrange { .. } => "lrange",
            Command::Lpush { .. } => "lpush",
            Command::Llen { .. } => "llen",
            Command::Lpop { .. } => "lpop",
            Command::Blpop { .. } => "blpop",
            Command::Type { .. } => "type",
            Command::Xadd { .. } => "xadd",
            Command::Xrange { .. } => "xrange",
            Command::Xread { .. } => "xread",
            Command::Incr { .. } => "incr",
            Command::Multi => "multi",
            Command::Exec => "exec",
            Command::Discard => "discard",
            Command::Watch { .. } => "watch",
            Command::Unwatch => "unwatch",
            Command::InternalExecuteTx { .. } => "exec",
            Command::Info { .. } => "info",
            Command::ReplconfListeningPort { .. }
            | Command::ReplconfCapa { .. }
            | Command::ReplconfGetAck
            | Command::ReplconfAck { .. } => "replconf",
            Command::Psync { .. } => "psync",
            Command::Wait { .. } => "wait",
            Command::ConfigGet { .. } => "config|get",
            Command::Keys { .. } => "keys",
            Command::Subscribe { .. } => "subscribe",
            Command::Publish { .. } => "publish",
            Command::Unsubscribe { .. } => "unsubscribe",
            Command::Zadd { .. } => "zadd",
        }
    }

    /// DEPRECATED
    // pub fn from_bytes(mut bs: VecDeque<Bytes>) -> Option<Command> {
    //     let name = bs.pop_front()?;

    //     match &name[..] {
    //         b"ECHO" => match bs.pop_front() {
    //             Some(message) => Some(Command::Echo { message }),
    //             None => None,
    //         },
    //         b"PING" => Some(Command::Ping {
    //             message: bs.pop_front(),
    //         }),
    //         b"SET" => match bs.len() {
    //             2 => {
    //                 let key = Key(bs.pop_front().unwrap());
    //                 let value = bs.pop_front().unwrap();
    //                 Some(Command::Set {
    //                     key,
    //                     value,
    //                     ex: None,
    //                     px: None,
    //                 })
    //             }
    //             4 => {
    //                 let key = Key(bs.pop_front().unwrap());
    //                 let value = bs.pop_front().unwrap();
    //                 let expx = bs.pop_front().unwrap();
    //                 let tmp = bs.pop_front().unwrap();
    //                 let (ttl, _) = integer::<u64>().parse(&tmp[..]).unwrap();
    //                 let (ex, px) = match &expx[..] {
    //                     b"EX" => (Some(ttl), None),
    //                     b"PX" => (None, Some(ttl)),
    //                     _ => (None, None),
    //                 };
    //                 Some(Command::Set { key, value, ex, px })
    //             }
    //             _ => None,
    //         },
    //         b"GET" => match bs.pop_front() {
    //             Some(key) => Some(Command::Get { key: Key(key) }),
    //             None => None,
    //         },
    //         // Lists
    //         b"RPUSH" => match bs.len() {
    //             0 | 1 => None,
    //             _ => Some(Command::Rpush {
    //                 key: Key(bs.pop_front().unwrap()),
    //                 elements: Vec::from(bs),
    //             }),
    //         },
    //         b"LRANGE" => {
    //             let key = Key(bs.pop_front().unwrap());
    //             let (start, _) = integer::<i32>().parse(&bs[0][..]).unwrap();
    //             let (end, _) = integer::<i32>().parse(&bs[1][..]).unwrap();
    //             Some(Command::Lrange { key, start, end })
    //         }
    //         b"LPUSH" => match bs.len() {
    //             0 | 1 => None,
    //             _ => Some(Command::Lpush {
    //                 key: Key(bs.pop_front().unwrap()),
    //                 elements: Vec::from(bs),
    //             }),
    //         },
    //         b"LLEN" => match bs.pop_front() {
    //             Some(key) => Some(Command::Llen { key: Key(key) }),
    //             None => None,
    //         },
    //         b"LPOP" => {
    //             let key = Key(bs.pop_front().unwrap());
    //             let count = if bs.len() > 0 {
    //                 let (c, _) = integer::<u32>().parse(&bs[0][..]).unwrap();
    //                 Some(c)
    //             } else {
    //                 None
    //             };
    //             Some(Command::Lpop { key, count })
    //         }
    //         b"BLPOP" => {
    //             let tmp = bs.pop_back().unwrap();
    //             let (timeout, _) = float::<f64>().parse(&tmp[..]).unwrap();
    //             let keys = bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>();
    //             Some(Command::Blpop { keys, timeout })
    //         }
    //         // Streams
    //         b"TYPE" => Some(Command::Type {
    //             key: Key(bs.pop_front().unwrap()),
    //         }),
    //         b"XADD" => {
    //             let key = Key(bs.pop_front().unwrap());
    //             let id = parse_input_stream_id(&bs.pop_front().unwrap()).unwrap();
    //             Some(Command::Xadd {
    //                 key,
    //                 id,
    //                 field_values: Vec::from(bs),
    //             })
    //         }
    //         b"XRANGE" => {
    //             let key = Key(bs.pop_front().unwrap());
    //             let s = &bs.pop_front().unwrap()[..];
    //             let e = &bs.pop_front().unwrap()[..];
    //             let start = if s.len() == 1 && s[0] == b'-' {
    //                 (0, 1)
    //             } else {
    //                 let ((start_tid, _, start_sid), _) =
    //                     and!(integer::<u64>(), byte(b'-'), integer::<u64>())
    //                         .parse(s)
    //                         .unwrap();
    //                 (start_tid, start_sid)
    //             };
    //             let end = if e.len() == 1 && e[0] == b'+' {
    //                 (u64::MAX, u64::MAX)
    //             } else {
    //                 let ((end_tid, _, end_sid), _) =
    //                     and!(integer::<u64>(), byte(b'-'), integer::<u64>())
    //                         .parse(e)
    //                         .unwrap();
    //                 (end_tid, end_sid)
    //             };
    //             Some(Command::Xrange { key, start, end })
    //         }
    //         b"XREAD" => {
    //             let block = bs[0].to_ascii_uppercase() == b"BLOCK";
    //             let milliseconds = if block {
    //                 bs.pop_front(); // BLOCK
    //                 let m = bs.pop_front().unwrap();
    //                 let (ms, _) = integer::<u64>().parse(&m[..]).unwrap();
    //                 Some(ms)
    //             } else {
    //                 None
    //             };

    //             assert!(
    //                 bs[0].to_ascii_uppercase() == b"STREAMS",
    //                 "Must have literal STREAM arg"
    //             );
    //             bs.pop_front(); // STREAMS

    //             // keys
    //             let l = bs.len();

    //             assert!(l % 2 == 0, "Must have even number of keys and ids");

    //             let ids = bs
    //                 .split_off(l / 2)
    //                 .iter()
    //                 .map(|id| parse_xread_stream_id_input(id).unwrap())
    //                 .collect::<Vec<_>>();

    //             let keys = bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>();

    //             assert!(
    //                 ids.len() == keys.len(),
    //                 "Must have same count of keys and ids"
    //             );

    //             Some(Command::Xread {
    //                 keys,
    //                 milliseconds,
    //                 ids,
    //             })
    //         }
    //         // Transactions
    //         b"INCR" => Some(Command::Incr {
    //             key: Key(bs.pop_front().unwrap()),
    //         }),
    //         b"MULTI" => Some(Command::Multi),
    //         b"EXEC" => Some(Command::Exec),
    //         b"DISCARD" => Some(Command::Discard),
    //         // Optimistic locking
    //         b"WATCH" => Some(Command::Watch {
    //             keys: bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>(),
    //         }),
    //         b"UNWATCH" => Some(Command::Unwatch),
    //         b"INFO" => Some(Command::Info {
    //             section: bs.pop_front(),
    //         }),
    //         b"REPLCONF" => {
    //             let next_token = bs.pop_front().unwrap();
    //             match &next_token[..] {
    //                 b"listening-port" => {
    //                     let port_part = bs.pop_front().unwrap();
    //                     let (port, _) = integer::<u16>().parse(&port_part).unwrap();
    //                     Some(Command::ReplconfListeningPort { port })
    //                 }
    //                 b"capa" => Some(Command::ReplconfCapa {
    //                     capabilites: bs.into(),
    //                 }),
    //                 b"GETACK" => {
    //                     let star = bs.pop_front().unwrap();
    //                     if star.len() == 1 && star[0] == b'*' {
    //                         Some(Command::ReplconfGetAck)
    //                     } else {
    //                         None
    //                     }
    //                 }
    //                 b"ACK" => {
    //                     let ack_bytes_field = bs.pop_front().unwrap();
    //                     let (ack_bytes, _) = integer::<u64>().parse(&ack_bytes_field).unwrap();
    //                     Some(Command::ReplconfAck { ack_bytes })
    //                 }
    //                 _ => panic!("Unknown REPLCONF shape"),
    //             }
    //         }
    //         b"PSYNC" => {
    //             let replication_id = String::from_utf8(bs.pop_front().unwrap()).unwrap();
    //             let offset_part = bs.pop_front().unwrap();
    //             let (offset, _) = integer::<i64>().parse(&offset_part).unwrap();
    //             Some(Command::Psync {
    //                 replication_id,
    //                 offset,
    //             })
    //         }
    //         b"WAIT" => {
    //             let numreplicas_field = bs.pop_front().unwrap();
    //             let timeout_field = bs.pop_front().unwrap();
    //             let (numreplicas, _) = integer::<u64>().parse(&numreplicas_field).unwrap();
    //             let (timeout, _) = integer::<u64>().parse(&timeout_field).unwrap();
    //             Some(Command::Wait {
    //                 numreplicas,
    //                 timeout,
    //             })
    //         }
    //         _ => None,
    //     }
    // }

    pub fn from_resp(resp: &Resp) -> Option<Command> {
        //print!("Command from resp: {:?}", resp);
        if let Resp::Array(els) = resp {
            assert!(els.len() > 0);

            let name = &els[0].get_str().unwrap().to_ascii_uppercase()[..];
            match name {
                "ECHO" => Some(Command::Echo {
                    message: els[1].get_bytes().unwrap(),
                }),
                "PING" => Some(Command::Ping {
                    message: els.get(1).and_then(|el| el.get_bytes()),
                }),
                "SET" => match els.len() {
                    3 => Some(Command::Set {
                        key: Key(els[1].get_bytes().unwrap()),
                        value: els[2].get_bytes().unwrap(),
                        ex: None,
                        px: None,
                    }),
                    5 => {
                        let ttl = els[4].get_str().unwrap().parse::<u64>().unwrap();
                        let (ex, px) = match els[3].get_str().unwrap().to_ascii_uppercase().as_str()
                        {
                            "EX" => (Some(ttl), None),
                            "PX" => (None, Some(ttl)),
                            _ => (None, None),
                        };
                        Some(Command::Set {
                            key: Key(els[1].get_bytes().unwrap()),
                            value: els[2].get_bytes().unwrap(),
                            ex,
                            px,
                        })
                    }
                    _ => None,
                },
                "GET" => Some(Command::Get {
                    key: Key(els[1].get_bytes().unwrap()),
                }),
                // Lists
                "RPUSH" => match els.len() {
                    0 | 1 | 2 => None,
                    _ => Some(Command::Rpush {
                        key: Key(els[1].get_bytes().unwrap()),
                        elements: els[2..].iter().map(|e| e.get_bytes().unwrap()).collect(),
                    }),
                },
                "LRANGE" => Some(Command::Lrange {
                    key: Key(els[1].get_bytes().unwrap()),
                    start: els[2].get_str().unwrap().parse().unwrap(),
                    end: els[3].get_str().unwrap().parse().unwrap(),
                }),
                "LPUSH" => match els.len() {
                    0 | 1 | 2 => None,
                    _ => Some(Command::Lpush {
                        key: Key(els[1].get_bytes().unwrap()),
                        elements: els[2..].iter().map(|e| e.get_bytes().unwrap()).collect(),
                    }),
                },
                "LLEN" => Some(Command::Llen {
                    key: Key(els[1].get_bytes().unwrap()),
                }),
                "LPOP" => Some(Command::Lpop {
                    key: Key(els[1].get_bytes().unwrap()),
                    count: els.get(2).map(|e| e.get_str().unwrap().parse().unwrap()),
                }),
                "BLPOP" => {
                    let timeout = els
                        .last()
                        .unwrap()
                        .get_str()
                        .unwrap()
                        .parse::<f64>()
                        .unwrap();
                    let keys = els[1..els.len() - 1]
                        .iter()
                        .map(|k| Key(k.get_bytes().unwrap()))
                        .collect::<Vec<_>>();
                    Some(Command::Blpop { keys, timeout })
                }
                // Streams
                "TYPE" => Some(Command::Type {
                    key: Key(els[1].get_bytes().unwrap()),
                }),
                "XADD" => {
                    let id_bytes = els[2].get_bytes().unwrap();
                    Some(Command::Xadd {
                        key: Key(els[1].get_bytes().unwrap()),
                        id: parse_input_stream_id(&id_bytes).unwrap(),
                        field_values: els[3..].iter().map(|e| e.get_bytes().unwrap()).collect(),
                    })
                }
                "XRANGE" => {
                    let key = Key(els[1].get_bytes().unwrap());
                    let s = els[2].get_bytes().unwrap();
                    let e = els[3].get_bytes().unwrap();
                    let start = if s == b"-" {
                        (0, 1)
                    } else {
                        let ((start_tid, _, start_sid), _) =
                            and!(integer::<u64>(), byte(b'-'), integer::<u64>())
                                .parse(&s)
                                .unwrap();
                        (start_tid, start_sid)
                    };
                    let end = if e == b"+" {
                        (u64::MAX, u64::MAX)
                    } else {
                        let ((end_tid, _, end_sid), _) =
                            and!(integer::<u64>(), byte(b'-'), integer::<u64>())
                                .parse(&e)
                                .unwrap();
                        (end_tid, end_sid)
                    };
                    Some(Command::Xrange { key, start, end })
                }
                "XREAD" => {
                    let mut i = 1;
                    let milliseconds = if els[i].get_str().unwrap().eq_ignore_ascii_case("BLOCK") {
                        i += 1;
                        let ms = els[i].get_str().unwrap().parse().unwrap();
                        i += 1;
                        Some(ms)
                    } else {
                        None
                    };

                    assert!(
                        els[i].get_str().unwrap().eq_ignore_ascii_case("STREAMS"),
                        "Must have literal STREAMS arg"
                    );
                    i += 1;

                    let rest = &els[i..];
                    assert!(rest.len() % 2 == 0, "Must have even number of keys and ids");
                    let half = rest.len() / 2;

                    let keys = rest[..half]
                        .iter()
                        .map(|k| Key(k.get_bytes().unwrap()))
                        .collect::<Vec<_>>();
                    let ids = rest[half..]
                        .iter()
                        .map(|id| parse_xread_stream_id_input(&id.get_bytes().unwrap()).unwrap())
                        .collect::<Vec<_>>();

                    Some(Command::Xread {
                        keys,
                        milliseconds,
                        ids,
                    })
                }
                // Transactions
                "INCR" => Some(Command::Incr {
                    key: Key(els[1].get_bytes().unwrap()),
                }),
                "MULTI" => Some(Command::Multi),
                "EXEC" => Some(Command::Exec),
                "DISCARD" => Some(Command::Discard),
                // Optimistic locking
                "WATCH" => Some(Command::Watch {
                    keys: els[1..]
                        .iter()
                        .map(|k| Key(k.get_bytes().unwrap()))
                        .collect(),
                }),
                "UNWATCH" => Some(Command::Unwatch),
                "INFO" => Some(Command::Info {
                    section: els.get(1).and_then(|e| e.get_bytes()),
                }),
                "REPLCONF" => match els[1].get_str().unwrap().to_ascii_uppercase().as_str() {
                    "LISTENING-PORT" => Some(Command::ReplconfListeningPort {
                        port: els[2].get_str().unwrap().parse().unwrap(),
                    }),
                    "CAPA" => Some(Command::ReplconfCapa {
                        capabilites: els[2..].iter().map(|e| e.get_bytes().unwrap()).collect(),
                    }),
                    "GETACK" => Some(Command::ReplconfGetAck),
                    "ACK" => Some(Command::ReplconfAck {
                        ack_bytes: els[2].get_str().unwrap().parse().unwrap(),
                    }),
                    _ => panic!("Unknown REPLCONF shape"),
                },
                "PSYNC" => Some(Command::Psync {
                    replication_id: els[1].get_str().unwrap().to_string(),
                    offset: els[2].get_str().unwrap().parse().unwrap(),
                }),
                "WAIT" => Some(Command::Wait {
                    numreplicas: els[1].get_str().unwrap().parse().unwrap(),
                    timeout: els[2].get_str().unwrap().parse().unwrap(),
                }),
                "CONFIG" => {
                    if let Some(subcommand) = els[1].get_str() {
                        if subcommand == "GET" && els.len() == 3 {
                            Some(Command::ConfigGet {
                                parameter: els[2].get_str().unwrap().to_string(),
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                "KEYS" => Some(Command::Keys {
                    pattern: els[1].get_str().unwrap().to_string(),
                }),
                "SUBSCRIBE" => {
                    let channels = els[1..]
                        .iter()
                        .map(|e| String::from_utf8(e.get_bytes().unwrap()).unwrap())
                        .collect::<Vec<_>>();
                    Some(Command::Subscribe { channels })
                }
                "PUBLISH" => {
                    let channel = els[1].get_str().unwrap().to_string();
                    let message = els[2].get_str().unwrap().to_string();
                    Some(Command::Publish { channel, message })
                }
                "UNSUBSCRIBE" => {
                    let channels = els[1..]
                        .iter()
                        .map(|e| String::from_utf8(e.get_bytes().unwrap()).unwrap())
                        .collect::<Vec<_>>();
                    Some(Command::Unsubscribe { channels })
                }
                "ZADD" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let score = els[2].get_str().unwrap().parse::<f64>().unwrap();
                    let member = els[3].get_str().unwrap().to_string();
                    Some(Command::Zadd { key, score, member })
                }
                _ => None,
            }
        } else {
            None
        }
    }

    pub fn to_resp(&self) -> Option<Resp> {
        match self {
            Command::Set { key, value, ex, px } => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("SET"));
                resp.push(Resp::bulk_string(key.to_str()));
                resp.push(Resp::BulkString(value.clone()));
                if let Some(ex) = ex {
                    resp.push(Resp::bulk_string("EX"));
                    resp.push(Resp::bulk_string(&ex.to_string()));
                }
                if let Some(px) = px {
                    resp.push(Resp::bulk_string("PX"));
                    resp.push(Resp::bulk_string(&px.to_string()));
                }
                Some(Resp::Array(resp))
            }
            Command::ReplconfGetAck => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("REPLCONF"));
                resp.push(Resp::bulk_string("GETACK"));
                resp.push(Resp::bulk_string("*"));
                Some(Resp::Array(resp))
            }
            Command::ReplconfListeningPort { port } => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("REPLCONF"));
                resp.push(Resp::bulk_string("listening-port"));
                resp.push(Resp::bulk_string(&port.to_string()));
                Some(Resp::Array(resp))
            }
            Command::ReplconfCapa { capabilites } => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("REPLCONF"));
                resp.push(Resp::bulk_string("capa"));
                for c in capabilites {
                    resp.push(Resp::BulkString(c.clone()));
                }
                Some(Resp::Array(resp))
            }
            Command::Psync {
                replication_id,
                offset,
            } => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("PSYNC"));
                resp.push(Resp::bulk_string(replication_id));
                resp.push(Resp::bulk_string(&offset.to_string()));
                Some(Resp::Array(resp))
            }
            Command::Ping { message } => {
                let mut resp: Vec<Resp> = Vec::new();
                resp.push(Resp::bulk_string("PING"));
                if let Some(msg) = message {
                    resp.push(Resp::BulkString(msg.clone()));
                }
                Some(Resp::Array(resp))
            }
            _ => None,
        }
    }

    pub fn modified_keys(&self) -> Vec<Key> {
        match self {
            Command::Set {
                key,
                value: _,
                ex: _,
                px: _,
            } => vec![key.clone()],
            Command::Lpush { key, elements: _ } => vec![key.clone()],
            Command::Rpush { key, elements: _ } => vec![key.clone()],
            Command::Incr { key } => vec![key.clone()],
            Command::Xadd {
                key,
                id: _,
                field_values: _,
            } => vec![key.clone()],
            _ => vec![],
        }
    }

    // in  milliseconds , for all timeouts
    pub fn block_timeout(&self) -> Option<u64> {
        match self {
            Command::Blpop { keys: _, timeout } => {
                if *timeout == 0. {
                    Some(u64::MAX)
                } else {
                    Some((timeout * 1_000.) as u64)
                }
            }
            Command::Xread {
                keys: _,
                milliseconds: Some(ms),
                ids: _,
            } => {
                if *ms == 0 {
                    Some(u64::MAX)
                } else {
                    Some(*ms)
                }
            }
            _ => None,
        }
    }

    pub fn is_replicatable(&self) -> bool {
        match self {
            Command::Set {
                key: _,
                value: _,
                ex: _,
                px: _,
            } => true,
            Command::ReplconfGetAck => true,
            _ => false,
        }
    }

    // fn encode_to_bytes(&self) -> Option<Vec<u8>> {
    //     let mut out = Vec::new();

    //     match self {
    //         Command::Set { key, value, ex, px } => {
    //             write_bytes(&mut out, &[b'*', b'3', b'\r', b'\n']);
    //             write_bytes(&mut out, &"$3\r\nSET\r\n".as_bytes().to_vec());

    //             // Key
    //             write_bytes(
    //                 &mut out,
    //                 &format!("${}\r\n", key.0.len()).as_bytes().to_vec(),
    //             );
    //             write_bytes(&mut out, &key.0);
    //             write_bytes(&mut out, &"\r\n".as_bytes().to_vec());

    //             // Value
    //             write_bytes(
    //                 &mut out,
    //                 &format!("${}\r\n", value.len()).as_bytes().to_vec(),
    //             );
    //             write_bytes(&mut out, &value);
    //             write_bytes(&mut out, &"\r\n".as_bytes().to_vec());

    //             // ex
    //             if let Some(ex) = ex {
    //                 write_bytes(&mut out, &"$2\r\nEX\r\n".as_bytes().to_vec());
    //                 let ex_s = format!("{}", ex);
    //                 write_bytes(
    //                     &mut out,
    //                     &format!("${}\r\n{}\r\n", ex_s.len(), ex_s)
    //                         .as_bytes()
    //                         .to_vec(),
    //                 );
    //             }

    //             // px
    //             if let Some(px) = px {
    //                 write_bytes(&mut out, &"$2\r\nPX\r\n".as_bytes().to_vec());
    //                 let px_s = format!("{}", px);
    //                 write_bytes(
    //                     &mut out,
    //                     &format!("${}\r\n{}\r\n", px_s.len(), px_s)
    //                         .as_bytes()
    //                         .to_vec(),
    //                 );
    //             }

    //             Some(out)
    //         }
    //         Command::ReplconfGetAck => {
    //             write_bytes(&mut out, &[b'*', b'3', b'\r', b'\n']);
    //             write_bytes(&mut out, &"$8\r\nREPLCONF\r\n".as_bytes().to_vec());
    //             write_bytes(&mut out, &"$6\r\nGETACK\r\n".as_bytes().to_vec());
    //             write_bytes(&mut out, &"$1\r\n*\r\n".as_bytes().to_vec());
    //             Some(out)
    //         }
    //         _ => None,
    //     }
    // }
}
