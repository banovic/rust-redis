use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::types::*;
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
    Zrank {
        key: String,
        member: String,
    },
    Zrange {
        key: String,
        start: i32,
        stop: i32,
    },
    Zcard {
        key: String,
    },
    Zscore {
        key: String,
        member: String,
    },
    Zrem {
        key: String,
        member: String,
    },
    // Geo
    Geoadd {
        key: String,
        longitude: f64,
        latitude: f64,
        member: String,
    },
    Geopos {
        key: String,
        members: Vec<String>,
    },
    Geodist {
        key: String,
        member1: String,
        member2: String,
    },
    Geosearch {
        // supports only FROMLONLAT + BYRADUIS right now
        key: String,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: String,
    },
    // Acl
    AclWhoami,
    AclGetuser {
        username: String,
    },
    AclSetuser {
        username: String,
        password: String,
    },
    Auth {
        username: String,
        password: String,
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
            Command::Zrank { .. } => "zrank",
            Command::Zrange { .. } => "zrange",
            Command::Zcard { .. } => "zcard",
            Command::Zscore { .. } => "zscore",
            Command::Zrem { .. } => "zrem",
            Command::Geoadd { .. } => "geoadd",
            Command::Geopos { .. } => "geopos",
            Command::Geodist { .. } => "geodist",
            Command::Geosearch { .. } => "geosearch",
            Command::AclWhoami => "aclwhoami",
            Command::AclGetuser { .. } => "aclgetuser",
            Command::AclSetuser { .. } => "aclsetuser",
            Command::Auth { .. } => "auth",
        }
    }

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
                "ZRANK" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let member = els[2].get_str().unwrap().to_string();
                    Some(Command::Zrank { key, member })
                }
                "ZRANGE" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let start = els[2].get_str().unwrap().parse::<i32>().unwrap();
                    let stop = els[3].get_str().unwrap().parse::<i32>().unwrap();
                    Some(Command::Zrange { key, start, stop })
                }
                "ZCARD" => {
                    let key = els[1].get_str().unwrap().to_string();
                    Some(Command::Zcard { key })
                }
                "ZSCORE" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let member = els[2].get_str().unwrap().to_string();
                    Some(Command::Zscore { key, member })
                }
                "ZREM" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let member = els[2].get_str().unwrap().to_string();
                    Some(Command::Zrem { key, member })
                }
                "GEOADD" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let longitude = els[2].get_str().unwrap().parse::<f64>().unwrap();
                    let latitude = els[3].get_str().unwrap().parse::<f64>().unwrap();
                    let member = els[4].get_str().unwrap().to_string();
                    Some(Command::Geoadd {
                        key,
                        longitude,
                        latitude,
                        member,
                    })
                }
                "GEOPOS" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let members = els[2..]
                        .iter()
                        .map(|e| String::from_utf8(e.get_bytes().unwrap()).unwrap())
                        .collect::<Vec<_>>();
                    Some(Command::Geopos { key, members })
                }
                "GEODIST" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let member1 = els[2].get_str().unwrap().to_string();
                    let member2 = els[3].get_str().unwrap().to_string();
                    Some(Command::Geodist {
                        key,
                        member1,
                        member2,
                    })
                }
                "GEOSEARCH" => {
                    let key = els[1].get_str().unwrap().to_string();
                    let fromlonlat = els[2].get_str().unwrap().to_string();
                    assert!(fromlonlat == "FROMLONLAT");
                    let longitude = els[3].get_str().unwrap().parse::<f64>().unwrap();
                    let latitude = els[4].get_str().unwrap().parse::<f64>().unwrap();
                    let byradius = els[5].get_str().unwrap().to_string();
                    assert!(byradius == "BYRADIUS");
                    let radius = els[6].get_str().unwrap().parse::<f64>().unwrap();
                    let unit = els[7].get_str().unwrap().to_string().to_uppercase();
                    assert!(unit == "M" || unit == "KM" || unit == "FT" || unit == "MI");
                    Some(Command::Geosearch {
                        key,
                        longitude,
                        latitude,
                        radius,
                        unit,
                    })
                }
                "ACL" => match els[1].get_str().unwrap() {
                    "WHOAMI" => Some(Command::AclWhoami),
                    "GETUSER" => {
                        let username = els[2].get_str().unwrap().to_string();
                        Some(Command::AclGetuser { username })
                    }
                    "SETUSER" => {
                        let username = els[2].get_str().unwrap().to_string();
                        let mut password = els[3].get_str().unwrap().to_string();
                        assert!(password.starts_with(">"));
                        password.remove(0);
                        Some(Command::AclSetuser { username, password })
                    }
                    _ => None,
                },
                "AUTH" => {
                    let username = els[1].get_str().unwrap().to_string();
                    let password = els[2].get_str().unwrap().to_string();
                    Some(Command::Auth { username, password })
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
}
