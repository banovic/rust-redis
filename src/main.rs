#![allow(unused_imports)]
//use clap::Parser;
use core::{num, str};
use futures::channel::oneshot;
//use futures::future::select_all;
use std::collections::HashSet;
use std::env;
use std::io::Write;
use std::num::ParseIntError;
use std::ops::Bound::{Excluded, Included, Unbounded};
use std::sync::atomic::AtomicUsize;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    error::{self, Error},
    fmt::{self, Debug, format},
    hash::Hash,
    ops::{AddAssign, Mul, MulAssign, Neg},
    os::unix::process,
    result,
    str::{FromStr, from_utf8},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    usize,
};
use tokio::select;
use tokio::stream;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, RwLock},
    time::timeout,
};

mod parser;
use parser::*;

use crate::PrimitiveValue::List;

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

// macro_rules! list_get {
//     ($key: ident, $some_expr: expr, $none_expr: expr) => {

//     };
// }

type Bytes = Vec<u8>;

#[derive(Debug)]
enum Reply {
    Ok,
    //Error(&'static str),
    Null,
    NullArray,
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Reply>),
    RdbFile(Vec<u8>),
}

//type RedisList = VecDeque<Bytes>;
type RedisStream = BTreeMap<StreamKey, Vec<Bytes>>;

#[derive(Debug)]

enum PrimitiveValue {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Stream(RedisStream),
}
/**
 * Store
 */
#[derive(PartialEq, Eq, Hash, Debug, Clone)]
struct Key(Vec<u8>);

#[derive(Debug)]
struct Value {
    t: Instant,
    ttl: Option<Duration>,
    value: PrimitiveValue,
}

type WaiterId = usize;

enum TryExecuteResult {
    Done(Reply),
    BlockingXread(WaiterId, Vec<Key>, Vec<(u64, u64)>),
    BlockingBlpop(WaiterId, Vec<Key>),
}
#[derive(Debug)]
struct Store {
    is_replica: bool,
    replicas: HashMap<usize, mpsc::Sender<Command>>,
    data: HashMap<Key, Value>,
    waiter_id: WaiterId,
    watched_keys: HashMap<Key, HashSet<usize>>,
    stream_xread_waiters: HashMap<WaiterId, (oneshot::Sender<Reply>, Vec<Key>, Vec<(u64, u64)>)>,
    list_blpop_waiters: HashMap<WaiterId, (oneshot::Sender<Reply>, Vec<Key>)>,
}

impl Store {
    fn new(is_replica: bool) -> Self {
        Self {
            is_replica,
            replicas: HashMap::new(),
            data: HashMap::new(),
            waiter_id: 0,
            watched_keys: HashMap::new(),
            stream_xread_waiters: HashMap::new(),
            list_blpop_waiters: HashMap::new(),
        }
    }

    fn map_list<F, R>(&self, key: &Key, f: F) -> Option<R>
    where
        F: Fn(&VecDeque<Bytes>) -> R,
    {
        if let Some(Value {
            t: _,
            ttl: _,
            value: PrimitiveValue::List(list),
        }) = self.data.get(key)
        {
            Some(f(list))
        } else {
            None
        }
    }

    fn fetch_xread(&self, keys: &[Key], ids: &[(u64, u64)]) -> (Vec<Reply>, bool) {
        let mut rows: Vec<Reply> = Vec::new();
        let mut is_empty = true;

        for (i, key) in keys.iter().enumerate() {
            let mut stream_rows: Vec<Reply> = Vec::new();

            if let Some(Value {
                t: _,
                ttl: _,
                value: PrimitiveValue::Stream(stream),
            }) = self.data.get(&key)
            {
                stream_rows.push(Reply::BulkString(key.0.clone()));

                let mut stream_row_data: Vec<Reply> = Vec::new();

                for (&k, v) in stream.range((Excluded(ids[i]), Unbounded)) {
                    is_empty = false;
                    let mut row: Vec<Reply> = Vec::new();
                    row.push(Reply::BulkString(
                        format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                    ));
                    row.push(Reply::Array(
                        v.iter()
                            .map(|s| Reply::BulkString(s.to_vec()))
                            .collect::<Vec<_>>(),
                    ));

                    stream_row_data.push(Reply::Array(row));
                }

                stream_rows.push(Reply::Array(stream_row_data));

                rows.push(Reply::Array(stream_rows));
            }
        }

        (rows, is_empty)
    }

    fn notify_xread_waiters(&mut self, key: &Key) {
        let mut waiters: Vec<WaiterId> = Vec::new();

        for (waiter_id, (_, keys, _)) in &self.stream_xread_waiters {
            if keys.contains(&key) {
                waiters.push(*waiter_id);
            }
        }

        for waiter_id in waiters {
            if let Some((reply_channel, keys, ids)) = self.stream_xread_waiters.remove(&waiter_id) {
                let (rows, _) = self.fetch_xread(&keys, &ids);
                let _ = reply_channel.send(Reply::Array(rows));
            }
        }
    }

    fn fetch_blpop(&mut self, keys: &[Key]) -> (Reply, bool) {
        for k in keys {
            if let Some(Value {
                t: _,
                ttl: _,
                value: PrimitiveValue::List(list),
            }) = self.data.get_mut(k)
            {
                if let Some(head) = list.pop_front() {
                    return (
                        Reply::Array(vec![
                            Reply::BulkString(k.clone().0),
                            Reply::BulkString(head),
                        ]),
                        false,
                    );
                }
            }
        }
        (Reply::NullArray, true)
    }

    fn notify_blpop_waiters(&mut self, key: &Key) {
        // Notify interested waiters:
        let mut waiters: Vec<WaiterId> = Vec::new();

        for (waiter_id, (_, keys)) in &self.list_blpop_waiters {
            if keys.contains(&key) {
                waiters.push(*waiter_id);
            }
        }

        for waiter_id in waiters {
            let (reply_channel, keys) = self.list_blpop_waiters.remove(&waiter_id).unwrap();
            let (rows, is_empty) = self.fetch_blpop(&keys);
            if !is_empty {
                let _ = reply_channel.send(rows);
                self.list_blpop_waiters.remove(&waiter_id);
            } else {
                self.list_blpop_waiters
                    .insert(waiter_id, (reply_channel, keys));
            }
        }
    }

    // Pure, sync
    fn try_execute(&mut self, client_id: usize, cmd: Command) -> TryExecuteResult {
        for key in cmd.modified_keys() {
            self.watched_keys.entry(key).and_modify(|clients| {
                (*clients).insert(client_id);
            });
        }

        match cmd {
            Command::Set { key, value, ex, px } => {
                let ttl = match (ex, px) {
                    (Some(ex), _) => Some(Duration::from_secs(ex)),
                    (_, Some(px)) => Some(Duration::from_millis(px)),
                    _ => None,
                };
                let v = Value {
                    t: Instant::now(),
                    ttl,
                    value: PrimitiveValue::Str(value),
                };
                self.data.insert(key, v);
                TryExecuteResult::Done(Reply::Ok)
            }

            Command::Get { key } => match self.data.get(&key) {
                Some(Value {
                    t,
                    ttl,
                    value: PrimitiveValue::Str(value),
                }) => match ttl {
                    None => TryExecuteResult::Done(Reply::BulkString(value.to_vec())),
                    Some(duration) if *t + *duration < Instant::now() => {
                        TryExecuteResult::Done(Reply::Null)
                    }
                    Some(_) => TryExecuteResult::Done(Reply::BulkString(value.to_vec())),
                },
                Some(_) => TryExecuteResult::Done(Reply::Null), // TODO - error wrong type
                None => TryExecuteResult::Done(Reply::Null),
            },

            Command::Watch { keys } => {
                for key in keys {
                    self.watched_keys
                        .entry(key)
                        .and_modify(|s| {
                            (*s).insert(client_id);
                        })
                        .or_insert_with(|| HashSet::from([client_id]));
                }
                TryExecuteResult::Done(Reply::Ok)
            }

            Command::Unwatch => {
                // Cleanup watched keys for this client, and return OK simple string
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                TryExecuteResult::Done(Reply::SimpleString("OK".as_bytes().to_vec()))
            }

            Command::InternalExecuteTx { commands } => {
                println!("tx: store1: {:?}", self);
                // Optimistic locking check
                let mut lock_failed = false;
                for (key, clients) in &self.watched_keys {
                    println!(
                        "tx: client_id {}, checking: key {:?} - {:?}",
                        client_id, &key, clients
                    );
                    if clients.contains(&client_id) && clients.len() > 1 {
                        lock_failed = true;
                    }
                }
                if lock_failed {
                    // Cleanup watched keys for this client, and return null array
                    for (_, clients) in &mut self.watched_keys {
                        clients.remove(&client_id);
                    }
                    self.watched_keys.retain(|_, clients| !clients.is_empty());

                    return TryExecuteResult::Done(Reply::NullArray);
                }

                // Execute tx
                let mut replies: Vec<Reply> = Vec::new();
                for cmd in commands {
                    let reply = match self.try_execute(client_id, cmd) {
                        TryExecuteResult::Done(r) => r,
                        _ => Reply::NullArray,
                    };
                    replies.push(reply);
                }

                // Cleanup watched keys for this client, and return null array
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                println!("tx: store2: {:?}", self);
                TryExecuteResult::Done(Reply::Array(replies))
            }
            Command::InternalDiscardTx => {
                // Cleanup watched keys for this client, and return null array
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                TryExecuteResult::Done(Reply::Ok)
            }
            Command::Incr { key } => {
                if let Some(Value {
                    t,
                    ttl: _,
                    value: PrimitiveValue::Str(s),
                }) = self.data.get_mut(&key)
                {
                    let result = parser::Parser::parse(&integer::<i64>(), s);
                    match result {
                        Ok((n, _)) => {
                            *t = Instant::now();
                            *s = (n + 1).to_string().as_bytes().to_vec();
                            TryExecuteResult::Done(Reply::Integer(n + 1))
                        }
                        _ => TryExecuteResult::Done(Reply::SimpleError(
                            b"ERR value is not an integer or out of range".to_vec(),
                        )),
                    }
                } else {
                    self.data.insert(
                        key,
                        Value {
                            t: Instant::now(),
                            ttl: None,
                            value: PrimitiveValue::Str(1.to_string().as_bytes().to_vec()),
                        },
                    );
                    TryExecuteResult::Done(Reply::Integer(1))
                }
            }
            Command::Xadd {
                key,
                id,
                field_values,
            } => {
                // Ensure that there is stream `key`:
                self.data.entry(key.clone()).or_insert(Value {
                    t: Instant::now(),
                    ttl: None,
                    value: PrimitiveValue::Stream(BTreeMap::new()),
                });

                if let Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::Stream(stream),
                }) = self.data.get_mut(&key)
                {
                    let (tid, sid) = match next_stream_id(id, stream) {
                        Some(id) => id,
                        _ => {
                            return TryExecuteResult::Done(Reply::SimpleError(
                                b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
                            ));
                        }
                    };

                    if (tid, sid) < (0, 1) {
                        return TryExecuteResult::Done(Reply::SimpleError(
                            b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
                        ));
                    }

                    if stream.contains_key(&(tid, sid)) {
                        return TryExecuteResult::Done(Reply::SimpleError(
                            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                .to_vec(),
                        ));
                    }

                    if let Some((latest, _)) = stream.last_key_value() {
                        if &(tid, sid) < latest {
                            return TryExecuteResult::Done(Reply::SimpleError(
                                b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                    .to_vec(),
                            ));
                        }
                    }

                    stream.insert((tid, sid), field_values);

                    self.notify_xread_waiters(&key);

                    TryExecuteResult::Done(Reply::BulkString(
                        format!("{}-{}", tid, sid).as_bytes().to_vec(),
                    ))
                } else {
                    TryExecuteResult::Done(Reply::SimpleError(
                        b"ERR No stream found for given key".to_vec(),
                    ))
                }
            }

            Command::Xread {
                keys,
                milliseconds,
                ids,
            } => {
                // Calculate ids:
                let mut real_ids: Vec<(u64, u64)> = Vec::new();
                for (i, id) in ids.iter().enumerate() {
                    match id {
                        XreadStreamIdInput::DollarId => {
                            let last = self
                                .data
                                .get(&keys[i])
                                .and_then(|s| match &s.value {
                                    PrimitiveValue::Stream(st) => st.last_key_value(),
                                    _ => None,
                                })
                                .map(|(&k, _)| k)
                                .unwrap_or((0, 1));
                            real_ids.push(last);
                        }
                        XreadStreamIdInput::Explicit(tid, sid) => real_ids.push((*tid, *sid)),
                    }
                }

                let (rows, is_empty) = self.fetch_xread(&keys, &real_ids);

                if !is_empty {
                    return TryExecuteResult::Done(Reply::Array(rows));
                }

                if milliseconds.is_none() {
                    return TryExecuteResult::Done(Reply::Array(vec![]));
                }

                self.waiter_id += 1;
                TryExecuteResult::BlockingXread(self.waiter_id, keys, real_ids)
            }

            Command::Xrange { key, start, end } => {
                if let Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::Stream(stream),
                }) = self.data.get(&key)
                {
                    let mut data: Vec<Reply> = Vec::new();
                    for (&k, v) in stream.range((Included(&start), Included(&end))) {
                        let mut row: Vec<Reply> = Vec::new();
                        row.push(Reply::BulkString(
                            format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                        ));
                        row.push(Reply::Array(
                            v.iter()
                                .map(|s| Reply::BulkString(s.to_vec()))
                                .collect::<Vec<_>>(),
                        ));
                        data.push(Reply::Array(row));
                    }
                    TryExecuteResult::Done(Reply::Array(data))
                } else {
                    TryExecuteResult::Done(Reply::SimpleError(
                        format!("Stream not found, XRANGE: {:?}", key)
                            .as_bytes()
                            .to_vec(),
                    ))
                }
            }

            Command::Type { key } => match self.data.get(&key) {
                Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::List(_),
                }) => TryExecuteResult::Done(Reply::SimpleString("list".as_bytes().to_vec())),
                Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::Str(_),
                }) => TryExecuteResult::Done(Reply::SimpleString("string".as_bytes().to_vec())),
                Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::Stream(_),
                }) => TryExecuteResult::Done(Reply::SimpleString("stream".as_bytes().to_vec())),
                _ => TryExecuteResult::Done(Reply::SimpleString("none".as_bytes().to_vec())),
            },

            Command::Rpush { key, elements } => {
                let n = match self.data.get_mut(&key) {
                    Some(Value {
                        t: _,
                        ttl: _,
                        value: PrimitiveValue::List(list),
                    }) => {
                        for e in elements {
                            list.push_back(e);
                        }
                        list.len()
                    }
                    _ => {
                        let n = elements.len();
                        self.data.insert(
                            key.clone(),
                            Value {
                                t: Instant::now(),
                                ttl: None,
                                value: PrimitiveValue::List(elements.into()),
                            },
                        );
                        n
                    }
                };

                self.notify_blpop_waiters(&key);

                TryExecuteResult::Done(Reply::Integer(n as i64))
            }

            Command::Lpush { key, mut elements } => {
                let n = match self.data.get_mut(&key) {
                    Some(Value {
                        t: _,
                        ttl: _,
                        value: PrimitiveValue::List(list),
                    }) => {
                        for e in elements {
                            list.push_front(e);
                        }
                        list.len()
                    }
                    _ => {
                        let n = elements.len();
                        elements.reverse();
                        self.data.insert(
                            key.clone(),
                            Value {
                                t: Instant::now(),
                                ttl: None,
                                value: PrimitiveValue::List(elements.into()),
                            },
                        );
                        n
                    }
                };

                self.notify_blpop_waiters(&key);

                TryExecuteResult::Done(Reply::Integer(n as i64))
            }

            Command::Lpop { key, count } => {
                if let Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::List(list),
                }) = self.data.get_mut(&key)
                {
                    if list.is_empty() {
                        return TryExecuteResult::Done(Reply::Null);
                    }

                    match count {
                        None => {
                            let e = list.pop_front().unwrap();
                            TryExecuteResult::Done(Reply::BulkString(e))
                        }
                        Some(c) => {
                            let mut els = Vec::new();
                            for _ in 0..c {
                                match list.pop_front() {
                                    Some(e) => els.push(Reply::BulkString(e)),
                                    None => return TryExecuteResult::Done(Reply::Array(els)),
                                }
                            }
                            TryExecuteResult::Done(Reply::Array(els))
                        }
                    }
                } else {
                    TryExecuteResult::Done(Reply::Null)
                }
            }

            Command::Lrange { key, start, end } => {
                if let Some(Value {
                    t: _,
                    ttl: _,
                    value: PrimitiveValue::List(list),
                }) = self.data.get(&key)
                {
                    let a = if start < 0 {
                        start + list.len() as i32
                    } else {
                        start
                    };
                    let a = 0.max(a);

                    let b = if end < 0 {
                        end + list.len() as i32
                    } else {
                        end
                    };
                    let b = (list.len() as i32 - 1).min(b);

                    if a > b {
                        return TryExecuteResult::Done(Reply::Array(vec![]));
                    }

                    let mut els = Vec::new();
                    for i in (a as usize)..=(b as usize) {
                        els.push(Reply::BulkString(list[i].to_vec()));
                    }
                    TryExecuteResult::Done(Reply::Array(els))
                } else {
                    TryExecuteResult::Done(Reply::Array(vec![]))
                }
            }

            Command::Llen { key } => {
                let n = self.map_list(&key, |list| list.len()).unwrap_or(0);
                TryExecuteResult::Done(Reply::Integer(n as i64))
            }

            Command::Blpop { keys, timeout: _ } => {
                let (reply, is_empty) = self.fetch_blpop(&keys);

                if !is_empty {
                    return TryExecuteResult::Done(reply);
                }
                self.waiter_id += 1;
                TryExecuteResult::BlockingBlpop(self.waiter_id, keys)
            }

            Command::Info { section } => {
                // Info
                let replica = if self.is_replica {
                    "slave".to_string()
                } else {
                    "master".to_string()
                };
                let mut info = "# Replication".to_string();
                info.push_str(&format!("\nrole:{}", replica).to_string());
                info.push_str(
                    &format!(
                        "\nmaster_replid:{}",
                        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    )
                    .to_string(),
                );
                info.push_str(&format!("\nmaster_repl_offset:{}", 0).to_string());
                TryExecuteResult::Done(Reply::BulkString(info.as_bytes().to_vec()))
            }

            Command::ReplconfAck => TryExecuteResult::Done(Reply::Array(vec![
                Reply::BulkString("REPLCONF".as_bytes().to_vec()),
                Reply::BulkString("ACK".as_bytes().to_vec()),
                Reply::BulkString("*".as_bytes().to_vec()),
            ])),

            _ => TryExecuteResult::Done(Reply::Null),
        }
    }
}

// milliseconds-seqeunce id
#[derive(Debug, Clone, Copy)]
enum XaddStreamIdInput {
    Explicit(u64, u64),
    AutoGenSeq(u64),
    AugoGen,
}
#[derive(Debug, Clone, Copy)]
enum XreadStreamIdInput {
    Explicit(u64, u64),
    DollarId,
}

type StreamKey = (u64, u64);

fn parse_input_stream_id<'a>(id: &'a Vec<u8>) -> Option<XaddStreamIdInput> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((tid, _, sid), _)) => Some(XaddStreamIdInput::Explicit(tid, sid)),
        _ => match and!(integer::<u64>(), byte(b'-'), byte(b'*')).parse(id) {
            Ok(((tid, _, _), _)) => Some(XaddStreamIdInput::AutoGenSeq(tid)),
            _ => match parser::Parser::parse(&byte(b'*'), id) {
                Ok(_) => Some(XaddStreamIdInput::AugoGen),
                _ => None,
            },
        },
    }
}

fn parse_xread_stream_id_input<'a>(id: &'a Vec<u8>) -> Option<XreadStreamIdInput> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((tid, _, sid), _)) => Some(XreadStreamIdInput::Explicit(tid, sid)),
        _ => match parser::Parser::parse(&byte(b'$'), id) {
            Ok(_) => Some(XreadStreamIdInput::DollarId),
            _ => None,
        },
    }
}

fn next_stream_id(ski: XaddStreamIdInput, stream: &RedisStream) -> Option<(u64, u64)> {
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

fn parse_bulk_string<'a>(input: ParserInput<'a>) -> ParseResult<'a, Bytes> {
    let ((_, l), rest) = and!(byte(b'$'), integer::<usize>()).parse(input)?;
    let ((_, s, _), rest) =
        and!(tag(&[b'\r', b'\n']), take(l), tag(&[b'\r', b'\n'])).parse(rest)?;

    Ok((s.to_vec(), rest))
}

fn parse_array<'a>(input: ParserInput<'a>) -> ParseResult<'a, VecDeque<Bytes>> {
    let ((_, l, _), rest) =
        and!(byte(b'*'), integer::<usize>(), tag(&[b'\r', b'\n'])).parse(input)?;

    let mut elements: VecDeque<Bytes> = VecDeque::new();
    let mut new_rest = rest;
    for _ in 0..l {
        let (el, rest) = parse_bulk_string(new_rest)?;
        new_rest = rest;
        elements.push_back(el);
    }

    Ok((elements, new_rest))
}

fn parse_input_resp<'a>(input: ParserInput<'a>) -> ParseResult<'a, VecDeque<Bytes>> {
    match input[0] {
        //b'$' => parse_bulk_string(input).map_or(default, f),
        b'*' => parse_array(input),
        _ => Err(ParseError {
            message: format!("unknown RESP first byte: {}", input[0]),
        }),
    }
}

fn write_usize(out: &mut Vec<u8>, n: usize) {
    let mut s = n.to_string().into_bytes();
    out.append(&mut s);
}

fn write_bytes(out: &mut Vec<u8>, bs: &[u8]) {
    out.extend_from_slice(bs);
}

fn encode_reply(r: &Reply) -> Vec<u8> {
    let mut out = Vec::new();

    match r {
        Reply::Ok => {
            write_bytes(&mut out, &[b'+', b'O', b'K', b'\r', b'\n']);
        }
        // Reply::Error(s) => {
        //     write_bytes(&mut out, &[b'-']);
        //     write_bytes(&mut out, &s.as_bytes().to_vec());
        //     write_bytes(&mut out, &[b'\r', b'\n']);
        // }
        Reply::Null => {
            write_bytes(&mut out, &[b'$', b'-', b'1', b'\r', b'\n']);
        }
        Reply::NullArray => {
            write_bytes(&mut out, &[b'*', b'-', b'1', b'\r', b'\n']);
        }
        Reply::SimpleString(value) => {
            write_bytes(&mut out, &[b'+']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Reply::SimpleError(value) => {
            write_bytes(&mut out, &[b'-']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Reply::BulkString(value) => {
            write_bytes(&mut out, &[b'$']);
            write_usize(&mut out, value.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Reply::Integer(n) => {
            write_bytes(&mut out, &[b':']);
            write_bytes(&mut out, n.to_string().as_bytes());
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Reply::Array(elements) => {
            write_bytes(&mut out, &[b'*']);
            write_usize(&mut out, elements.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            for e in elements {
                out.append(&mut encode_reply(e));
            }
        }
        Reply::RdbFile(bytes) => {
            write_bytes(&mut out, &[b'$']);
            write_usize(&mut out, bytes.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            write_bytes(&mut out, &bytes[..]);
        }
    }

    out
}

async fn write_reply(stream: &mut TcpStream, reply: &Reply) -> std::io::Result<()> {
    let out = encode_reply(reply);
    let result = stream.write_all(&out[..]).await;
    result
}

#[derive(Debug, Clone)]
enum Command {
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
    InternalDiscardTx,
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
    ReplconfAck,
}

impl Command {
    fn from_bytes(mut bs: VecDeque<Bytes>) -> Option<Command> {
        let name = bs.pop_front()?;

        match &name[..] {
            b"ECHO" => match bs.pop_front() {
                Some(message) => Some(Command::Echo { message }),
                None => None,
            },
            b"PING" => Some(Command::Ping {
                message: bs.pop_front(),
            }),
            b"SET" => match bs.len() {
                2 => {
                    let key = Key(bs.pop_front().unwrap());
                    let value = bs.pop_front().unwrap();
                    Some(Command::Set {
                        key,
                        value,
                        ex: None,
                        px: None,
                    })
                }
                4 => {
                    let key = Key(bs.pop_front().unwrap());
                    let value = bs.pop_front().unwrap();
                    let expx = bs.pop_front().unwrap();
                    let tmp = bs.pop_front().unwrap();
                    let (ttl, _) = parser::Parser::parse(&integer::<u64>(), &tmp[..]).unwrap();
                    let (ex, px) = match &expx[..] {
                        b"EX" => (Some(ttl), None),
                        b"PX" => (None, Some(ttl)),
                        _ => (None, None),
                    };
                    Some(Command::Set { key, value, ex, px })
                }
                _ => None,
            },
            b"GET" => match bs.pop_front() {
                Some(key) => Some(Command::Get { key: Key(key) }),
                None => None,
            },
            // Lists
            b"RPUSH" => match bs.len() {
                0 | 1 => None,
                _ => Some(Command::Rpush {
                    key: Key(bs.pop_front().unwrap()),
                    elements: Vec::from(bs),
                }),
            },
            b"LRANGE" => {
                let key = Key(bs.pop_front().unwrap());
                let (start, _) = parser::Parser::parse(&integer::<i32>(), &bs[0][..]).unwrap();
                let (end, _) = parser::Parser::parse(&integer::<i32>(), &bs[1][..]).unwrap();
                Some(Command::Lrange { key, start, end })
            }
            b"LPUSH" => match bs.len() {
                0 | 1 => None,
                _ => Some(Command::Lpush {
                    key: Key(bs.pop_front().unwrap()),
                    elements: Vec::from(bs),
                }),
            },
            b"LLEN" => match bs.pop_front() {
                Some(key) => Some(Command::Llen { key: Key(key) }),
                None => None,
            },
            b"LPOP" => {
                let key = Key(bs.pop_front().unwrap());
                let count = if bs.len() > 0 {
                    let (c, _) = parser::Parser::parse(&integer::<u32>(), &bs[0][..]).unwrap();
                    Some(c)
                } else {
                    None
                };
                Some(Command::Lpop { key, count })
            }
            b"BLPOP" => {
                let tmp = bs.pop_back().unwrap();
                let (timeout, _) = parser::Parser::parse(&float::<f64>(), &tmp[..]).unwrap();
                let keys = bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>();
                Some(Command::Blpop { keys, timeout })
            }
            // Streams
            b"TYPE" => Some(Command::Type {
                key: Key(bs.pop_front().unwrap()),
            }),
            b"XADD" => {
                let key = Key(bs.pop_front().unwrap());
                let id = parse_input_stream_id(&bs.pop_front().unwrap()).unwrap();
                Some(Command::Xadd {
                    key,
                    id,
                    field_values: Vec::from(bs),
                })
            }
            b"XRANGE" => {
                let key = Key(bs.pop_front().unwrap());
                let s = &bs.pop_front().unwrap()[..];
                let e = &bs.pop_front().unwrap()[..];
                let start = if s.len() == 1 && s[0] == b'-' {
                    (0, 1)
                } else {
                    let ((start_tid, _, start_sid), _) =
                        and!(integer::<u64>(), byte(b'-'), integer::<u64>())
                            .parse(s)
                            .unwrap();
                    (start_tid, start_sid)
                };
                let end = if e.len() == 1 && e[0] == b'+' {
                    (u64::MAX, u64::MAX)
                } else {
                    let ((end_tid, _, end_sid), _) =
                        and!(integer::<u64>(), byte(b'-'), integer::<u64>())
                            .parse(e)
                            .unwrap();
                    (end_tid, end_sid)
                };
                Some(Command::Xrange { key, start, end })
            }
            b"XREAD" => {
                let block = bs[0].to_ascii_uppercase() == b"BLOCK";
                let milliseconds = if block {
                    bs.pop_front(); // BLOCK
                    let m = bs.pop_front().unwrap();
                    let (ms, _) = parser::Parser::parse(&integer::<u64>(), &m[..]).unwrap();
                    Some(ms)
                } else {
                    None
                };

                assert!(
                    bs[0].to_ascii_uppercase() == b"STREAMS",
                    "Must have literal STREAM arg"
                );
                bs.pop_front(); // STREAMS

                // keys
                let l = bs.len();

                assert!(l % 2 == 0, "Must have even number of keys and ids");

                let ids = bs
                    .split_off(l / 2)
                    .iter()
                    .map(|id| parse_xread_stream_id_input(id).unwrap())
                    .collect::<Vec<_>>();

                let keys = bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>();

                assert!(
                    ids.len() == keys.len(),
                    "Must have same count of keys and ids"
                );

                Some(Command::Xread {
                    keys,
                    milliseconds,
                    ids,
                })
            }
            // Transactions
            b"INCR" => Some(Command::Incr {
                key: Key(bs.pop_front().unwrap()),
            }),
            b"MULTI" => Some(Command::Multi),
            b"EXEC" => Some(Command::Exec),
            b"DISCARD" => Some(Command::Discard),
            // Optimistic locking
            b"WATCH" => Some(Command::Watch {
                keys: bs.iter().map(|k| Key(k.to_vec())).collect::<Vec<_>>(),
            }),
            b"UNWATCH" => Some(Command::Unwatch),
            b"INFO" => Some(Command::Info {
                section: bs.pop_front(),
            }),
            b"REPLCONF" => {
                let next_token = bs.pop_front().unwrap();
                match &next_token[..] {
                    b"listening-port" => {
                        let port_part = bs.pop_front().unwrap();
                        let (port, _) = integer::<u16>().parse(&port_part).unwrap();
                        Some(Command::ReplconfListeningPort { port })
                    }
                    b"capa" => Some(Command::ReplconfCapa {
                        capabilites: bs.into(),
                    }),
                    b"GETACK" => {
                        let star = bs.pop_front().unwrap();
                        if star.len() == 1 && star[0] == b'*' {
                            Some(Command::ReplconfAck)
                        } else {
                            None
                        }
                    }
                    _ => panic!("Unknown REPLCONF shape"),
                }
            }
            b"PSYNC" => {
                let replication_id = String::from_utf8(bs.pop_front().unwrap()).unwrap();
                let offset_part = bs.pop_front().unwrap();
                let (offset, _) = integer::<i64>().parse(&offset_part).unwrap();
                Some(Command::Psync {
                    replication_id,
                    offset,
                })
            }
            _ => None,
        }
    }

    fn modified_keys(&self) -> Vec<Key> {
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
    fn block_timeout(&self) -> Option<u64> {
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

    fn is_replicatable(&self) -> bool {
        match self {
            Command::Set {
                key: _,
                value: _,
                ex: _,
                px: _,
            } => true,
            _ => false,
        }
    }

    fn encode_to_bytes(&self) -> Option<Vec<u8>> {
        let mut out = Vec::new();

        match self {
            Command::Set { key, value, ex, px } => {
                write_bytes(&mut out, &[b'*', b'3', b'\r', b'\n']);
                write_bytes(&mut out, &"$3\r\nSET\r\n".as_bytes().to_vec());

                // Key
                write_bytes(
                    &mut out,
                    &format!("${}\r\n", key.0.len()).as_bytes().to_vec(),
                );
                write_bytes(&mut out, &key.0);
                write_bytes(&mut out, &"\r\n".as_bytes().to_vec());

                // Value
                write_bytes(
                    &mut out,
                    &format!("${}\r\n", value.len()).as_bytes().to_vec(),
                );
                write_bytes(&mut out, &value);
                write_bytes(&mut out, &"\r\n".as_bytes().to_vec());

                // ex
                if let Some(ex) = ex {
                    write_bytes(&mut out, &"$2\r\nEX\r\n".as_bytes().to_vec());
                    let ex_s = format!("{}", ex);
                    write_bytes(
                        &mut out,
                        &format!("${}\r\n{}\r\n", ex_s.len(), ex_s)
                            .as_bytes()
                            .to_vec(),
                    );
                }

                // px
                if let Some(px) = px {
                    write_bytes(&mut out, &"$2\r\nPX\r\n".as_bytes().to_vec());
                    let px_s = format!("{}", px);
                    write_bytes(
                        &mut out,
                        &format!("${}\r\n{}\r\n", px_s.len(), px_s)
                            .as_bytes()
                            .to_vec(),
                    );
                }

                Some(out)
            }
            _ => None,
        }
    }
}

enum Envelope {
    WithReply {
        client_id: usize,
        command: Command,
        reply_channel: oneshot::Sender<Reply>,
    },
    TimeoutXread {
        waiter_id: WaiterId,
    },
    TimeoutBlpop {
        waiter_id: WaiterId,
    },
    AddReplica {
        client_id: usize,
        tx: mpsc::Sender<Command>,
    },
    Replicate {
        command: Command,
    },
    FromMaster {
        command: Command,
        reply_channel: oneshot::Sender<Reply>,
    },
}

// This layer handles timeouts
async fn run_store(mut store: Store, mut rx: mpsc::Receiver<Envelope>, tx: mpsc::Sender<Envelope>) {
    while let Some(e) = rx.recv().await {
        match e {
            Envelope::WithReply {
                client_id,
                command,
                reply_channel,
            } => {
                let timeout = command.block_timeout();
                let replication_command = if command.is_replicatable() {
                    Some(command.clone())
                } else {
                    None
                };

                match store.try_execute(client_id, command) {
                    TryExecuteResult::Done(reply) => {
                        let _ = reply_channel.send(reply);
                        if let Some(replication_command) = replication_command {
                            for (client_id, tx) in &store.replicas {
                                println!(
                                    "[client_id = {}] Replicating command: {:?}",
                                    client_id, replication_command
                                );
                                let _ = tx.send(replication_command.clone()).await;
                            }
                        }
                    }
                    TryExecuteResult::BlockingXread(waiter_id, keys, ids) => {
                        // Register interest in updates vs timeout conundrums
                        store
                            .stream_xread_waiters
                            .insert(waiter_id, (reply_channel, keys, ids));
                        let duration = Duration::from_millis(timeout.unwrap());
                        let tx2 = tx.clone();
                        tokio::spawn(async move {
                            sleep(duration).await;
                            let _ = tx2.send(Envelope::TimeoutXread { waiter_id }).await;
                        });
                    }
                    TryExecuteResult::BlockingBlpop(waiter_id, keys) => {
                        println!("BLPOP: Timeout: {:?}", timeout);
                        store
                            .list_blpop_waiters
                            .insert(waiter_id, (reply_channel, keys));
                        let duration = Duration::from_millis(timeout.unwrap());
                        let tx2 = tx.clone();
                        tokio::spawn(async move {
                            sleep(duration).await;
                            let _ = tx2.send(Envelope::TimeoutBlpop { waiter_id }).await;
                        });
                    }
                }
            }
            Envelope::TimeoutXread { waiter_id } => {
                // Deregister interest if there's any, and remove interestent
                if let Some((reply_channel, _, _)) = store.stream_xread_waiters.remove(&waiter_id) {
                    let _ = reply_channel.send(Reply::NullArray);
                }
            }
            Envelope::TimeoutBlpop { waiter_id } => {
                // Deregister interest if there's any, and remove interestent
                if let Some((reply_channel, _)) = store.list_blpop_waiters.remove(&waiter_id) {
                    let _ = reply_channel.send(Reply::NullArray);
                }
            }
            Envelope::AddReplica { client_id, tx } => {
                store.replicas.insert(client_id, tx);
            }
            // This is command execution on replica
            Envelope::Replicate { command } => {
                let _ = store.try_execute(0, command); // TODO client_id should be Option<usize>
            }
            Envelope::FromMaster {
                command,
                reply_channel,
            } => match store.try_execute(0, command) {
                TryExecuteResult::Done(reply) => {
                    println!("FromMaster :: {:?}", reply);
                    let _ = reply_channel.send(reply);
                }
                _ => {
                    let _ = reply_channel.send(Reply::Null);
                }
            },
        }
    }
}

async fn handle_client(
    client_id: usize,
    mut stream: TcpStream,
    producer_ch: mpsc::Sender<Envelope>,
) {
    println!("Connected client {}", client_id);
    let mut queue: Option<VecDeque<Command>> = None; //VecDeque::new();
    let mut buffer = [0u8; 1024];

    // Channel to this client, so master can send commands for replication
    let (tx, mut rx) = mpsc::channel::<Command>(1024);

    loop {
        select! {
            bytes_read = stream.read(&mut buffer) => {
                match bytes_read {
                    Ok(0) => {
                        // Client closed connection
                        break;
                    }
                    Ok(_) => {
                        let (input, _) = parse_input_resp(&buffer).unwrap();
                        let command = Command::from_bytes(input).unwrap();
                        let replies = match (&command, &mut queue) {
                            (Command::ReplconfListeningPort { port: _ }, _) => {
                                vec![Reply::SimpleString("OK".as_bytes().to_vec())]
                            }
                            (Command::ReplconfCapa { capabilites: _ }, _) => {
                                vec![Reply::SimpleString("OK".as_bytes().to_vec())]
                            }
                            // Psync
                            (
                                Command::Psync {
                                    replication_id: _,
                                    offset: _,
                                },
                                _,
                            ) => {
                                let _ = producer_ch.send(Envelope::AddReplica {client_id, tx: tx.clone()}).await;
                                // This client is replica
                                vec![
                                    Reply::SimpleString(
                                        "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"
                                            .as_bytes()
                                            .to_vec(),
                                    ),
                                    Reply::RdbFile(decode_hex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2").unwrap()),
                                ]
                            },
                            // Just echo
                            (Command::Echo { message }, _) => vec![Reply::BulkString(message.to_vec())],
                            (Command::Ping { message }, _) => match message {
                                Some(m) => vec![Reply::BulkString(m.to_vec())],
                                None => vec![Reply::SimpleString("PONG".as_bytes().to_vec())],
                            },
                            // Start tx
                            (Command::Multi, None) => {
                                queue = Some(VecDeque::new());
                                vec![Reply::Ok]
                            }
                            (Command::Exec, Some(_)) => {
                                let commands = queue.take().unwrap();
                                let tx = Command::InternalExecuteTx {
                                    commands: commands.into(),
                                };
                                vec![execute_command(&producer_ch, client_id, tx).await]
                            }
                            (Command::Exec, None) => vec![Reply::SimpleError(
                                "ERR EXEC without MULTI".as_bytes().to_vec(),
                            )],
                            (Command::Watch { keys: _ }, Some(_)) => vec![Reply::SimpleError(
                                "ERR WATCH inside MULTI is not allowed".as_bytes().to_vec(),
                            )],
                            (Command::Discard, None) => vec![Reply::SimpleError(
                                "ERR DISCARD without MULTI".as_bytes().to_vec(),
                            )],
                            (Command::Discard, Some(_)) => {
                                queue = None;
                                vec![execute_command(&producer_ch, client_id, Command::InternalDiscardTx).await]
                            }

                            // Inside tx
                            (_, Some(q)) => {
                                q.push_back(command);
                                vec![Reply::SimpleString("QUEUED".as_bytes().to_vec())]
                            }
                            (_, None) => vec![execute_command(&producer_ch, client_id, command).await],
                        };

                        for reply in replies {
                            let _ = write_reply(&mut stream, &reply).await;
                        }

                        let _ = stream.flush().await;
                        buffer.fill(0u8);
                    }
                    Err(_) => {
                        // TCP read error, ignore
                    }
                }
            },
            replicate_command = rx.recv() => {
                // Command received from master, encode it and send it to client / replica
                // (this is all happening on master, this is process inside master / server)
                println!("Replica received command: {:?}", replicate_command);
                if let Some(command) = replicate_command {
                    if let Some(encoded_command) = command.encode_to_bytes() {
                        println!("Sending ecnoded command: {:?}", encoded_command);
                        let _ = stream.write_all(&encoded_command).await;
                        let _ = stream.flush().await;
                    }
                }
            }
        }
    }

    println!("Client {} disconnected", client_id);
}

async fn execute_command(
    store_ch: &mpsc::Sender<Envelope>,
    client_id: usize,
    command: Command,
) -> Reply {
    // Create command
    let (reply_ch_sender, reply_ch_receiver) = oneshot::channel::<Reply>();
    // Pass to store to handle
    let envelope = Envelope::WithReply {
        client_id,
        command,
        reply_channel: reply_ch_sender,
    };

    let _ = store_ch.send(envelope).await; // this is store process

    // store process must send reply in all cases. how to ensure / enforce this?
    let reply = match reply_ch_receiver.await {
        Ok(r) => r,
        Err(_) => panic!("Something wrong with processing command"),
    };

    reply
}

/// Simple program to greet a person
#[derive(clap::Parser, Debug)]
struct Args {
    /// Port on which to start
    #[arg(long)]
    port: Option<u16>,

    /// Replica
    #[arg(long)]
    replicaof: Option<String>,
}

// This is run when server is replica
async fn run_replica(addr: String, port: u16, mut store_process_tx: mpsc::Sender<Envelope>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut buffer = [0; 1024];

    /// Handshake
    // PING - PONG
    let message = Reply::Array(vec![Reply::BulkString("PING".as_bytes().to_vec())]);
    let _ = stream.write_all(&encode_reply(&message)).await;
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // REPLCONF
    let message = Reply::Array(vec![
        Reply::BulkString("REPLCONF".as_bytes().to_vec()),
        Reply::BulkString("listening-port".as_bytes().to_vec()),
        Reply::BulkString(format!("{}", port).as_bytes().to_vec()),
    ]);
    let _ = stream.write_all(&encode_reply(&message)).await;
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // REPLCONF
    let message = Reply::Array(vec![
        Reply::BulkString("REPLCONF".as_bytes().to_vec()),
        Reply::BulkString("capa".as_bytes().to_vec()),
        Reply::BulkString("psync2".as_bytes().to_vec()),
    ]);
    let _ = stream.write_all(&encode_reply(&message)).await;
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // PSYNC
    let message = Reply::Array(vec![
        Reply::BulkString("PSYNC".as_bytes().to_vec()),
        Reply::BulkString("?".as_bytes().to_vec()),
        Reply::BulkString("-1".as_bytes().to_vec()),
    ]);
    let _ = stream.write_all(&encode_reply(&message)).await;
    let bytes_read = stream.read(&mut buffer).await.unwrap();

    // Read Rdb?
    //let bytes_read = stream.read(&mut buffer).await.unwrap();
    println!("Last handshake message(s) : {:?}", buffer);

    loop {
        select! {
            bytes_read = stream.read(&mut buffer) => {
                println!("First message: {:?}", buffer);
                match bytes_read {
                    Ok(0) => {
                        println!("Master disconnected");
                        break;
                    }
                    Ok(_) => {
                        // Execute (replicate) command
                        let (input, _) = parse_input_resp(&buffer).unwrap();
                        let command = Command::from_bytes(input).unwrap();
                        println!("Replica received command: {:?}", command);
                        match command {
                            Command::ReplconfAck => {
                                let (rsp_tx, rsp_rx) = oneshot::channel::<Reply>();
                                let _ = store_process_tx.send(Envelope::FromMaster { command, reply_channel: rsp_tx });
                                let reply = rsp_rx.await;
                                println!("Received from master: {:?}", reply);
                                // let _ = write_reply(&mut stream, &reply).await;
                                // let _ = stream.flush().await;
                                // buffer.fill(0u8);
                            }
                            _ => {
                                let _ = store_process_tx.send(Envelope::Replicate{ command }).await;
                            }
                        }
                    }
                    Err(e) => {
                        println!("TCP error: {:?}", e);
                    }
                }
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let client_counter = AtomicUsize::new(1);

    // CLI Args
    let default_port = 6379;
    let args = <Args as clap::Parser>::parse();
    let port = args.port.unwrap_or(default_port);
    let master_addr = args.replicaof.map(|v| v.replace(" ", ":"));
    let is_replica = master_addr.is_some();

    // Store setup
    let (tx, rx) = mpsc::channel::<Envelope>(1024);
    let store = Store::new(is_replica);
    tokio::spawn(run_store(store, rx, tx.clone()));

    if let Some(addr) = master_addr {
        tokio::spawn(run_replica(addr, port, tx.clone()));
    }

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    // mpsc == Multiple Producer Single Consumer

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let client_id = client_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let client_producer_ch = tx.clone();
        tokio::spawn(handle_client(client_id, stream, client_producer_ch));
    }
}
