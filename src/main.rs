#![allow(unused_imports)]
use core::{num, str};
use futures::channel::oneshot;
use futures::future::select_all;
use std::collections::HashSet;
use std::io::Write;
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

type Bytes = Vec<u8>;

#[derive(Debug)]
enum Reply {
    Ok,
    Error(&'static str),
    Null,
    NullArray,
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Reply>),
}

type RedisList = VecDeque<Bytes>;
type RedisStream = BTreeMap<StreamKey, Vec<Bytes>>;

#[derive(Debug)]

enum PrimitiveValue {
    Str(Bytes),
    List(Vec<Bytes>),
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
    BlockingXread(WaiterId, HashMap<Key, (u64, u64)>),
}
#[derive(Debug)]
struct Store {
    data: HashMap<Key, Value>,
    waiter_id: WaiterId,
    watched_keys: HashMap<Key, HashSet<usize>>,
    stream_xread_waiters: HashMap<WaiterId, (oneshot::Sender<Reply>, HashMap<Key, (u64, u64)>)>,
}

impl Store {
    fn new() -> Self {
        Self {
            data: HashMap::new(),
            waiter_id: 0,
            watched_keys: HashMap::new(),
            stream_xread_waiters: HashMap::new(),
        }
    }

    fn fetch_xread(&self, keys_ids: &HashMap<Key, (u64, u64)>) -> (Vec<Reply>, bool) {
        let mut rows: Vec<Reply> = Vec::new();
        let mut is_empty = true;

        for (i, (key, id)) in keys_ids.iter().enumerate() {
            let mut stream_rows: Vec<Reply> = Vec::new();

            if let Some(Value {
                t,
                ttl,
                value: PrimitiveValue::Stream(stream),
            }) = self.data.get(&key)
            {
                stream_rows.push(Reply::BulkString(key.0.clone()));

                let mut stream_row_data: Vec<Reply> = Vec::new();

                for (&k, v) in stream.range((Excluded(id), Unbounded)) {
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
            Command::Internal_Execute_Tx { commands } => {
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
            Command::Internal_Discard_Tx => {
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
                    let result = integer::<i64>().parse(s);
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

                let result = if let Some(Value {
                    t,
                    ttl,
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

                    TryExecuteResult::Done(Reply::BulkString(
                        format!("{}-{}", tid, sid).as_bytes().to_vec(),
                    ))
                } else {
                    TryExecuteResult::Done(Reply::SimpleError(
                        b"ERR No stream found for given key".to_vec(),
                    ))
                };

                if let TryExecuteResult::Done(_) = result {
                    // Notify interested waiters:
                    let mut waiters: Vec<WaiterId> = Vec::new();
                    println!(
                        "INTERESTED WAITERS - WHOLE STATE: {:?}",
                        self.stream_xread_waiters
                    );
                    println!("INTERESTED WAITERS - SEARCHING KEY: {:?}", key);
                    for (waiter_id, (_, keys_ids)) in &self.stream_xread_waiters {
                        if keys_ids.contains_key(&key) {
                            waiters.push(*waiter_id);
                        }
                    }
                    println!("INTERESTED WAITERS: {:?}", waiters);
                    for waiter_id in waiters {
                        if let Some((reply_channel, keys_ids)) =
                            self.stream_xread_waiters.remove(&waiter_id)
                        {
                            let (rows, is_empty) = self.fetch_xread(&keys_ids);
                            let _ = reply_channel.send(Reply::Array(rows));
                        }
                    }
                }

                result
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

                // First, try to read if something is in there - if so fine, done - regardless of timeout.
                // If there is timeout and there is nothing to read, do the following:
                // - schedule a timeout task to send null array to client
                // - add channel to be notified (stream - key)
                // these 2 tasks need some simple sync to write to oneshot channel
                let keys_ids: HashMap<Key, (u64, u64)> = keys.into_iter().zip(real_ids).collect();
                let (rows, is_empty) = self.fetch_xread(&keys_ids);
                // let mut rows: Vec<Reply> = Vec::new();
                // let mut is_empty = true;

                // for (i, key) in keys.iter().enumerate() {
                //     let mut stream_rows: Vec<Reply> = Vec::new();

                //     if let Some(Value {
                //         t,
                //         ttl,
                //         value: PrimitiveValue::Stream(stream),
                //     }) = self.data.get(&key)
                //     {
                //         stream_rows.push(Reply::BulkString(key.0.clone()));

                //         let mut stream_row_data: Vec<Reply> = Vec::new();

                //         for (&k, v) in stream.range((Excluded(&real_ids[i]), Unbounded)) {
                //             is_empty = false;
                //             let mut row: Vec<Reply> = Vec::new();
                //             row.push(Reply::BulkString(
                //                 format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                //             ));
                //             row.push(Reply::Array(
                //                 v.iter()
                //                     .map(|s| Reply::BulkString(s.to_vec()))
                //                     .collect::<Vec<_>>(),
                //             ));

                //             stream_row_data.push(Reply::Array(row));
                //         }

                //         stream_rows.push(Reply::Array(stream_row_data));

                //         rows.push(Reply::Array(stream_rows));
                //     }
                // }

                if !is_empty {
                    return TryExecuteResult::Done(Reply::Array(rows));
                }

                if milliseconds.is_none() {
                    return TryExecuteResult::Done(Reply::Array(vec![]));
                }

                self.waiter_id += 1;
                TryExecuteResult::BlockingXread(self.waiter_id, keys_ids)
                // async fn process_xread(
                //     cmd: &Command,
                //     stream_store: &Arc<RwLock<RedisStreamStore>>,
                // ) -> Result<Resp, ParseError> {
                //     let mut argc = 0_usize;
                //     let args2 = cmd.args.iter().collect::<Vec<_>>();
                //     // if args.len() < 3 {
                //     //     return Err(ParseError {
                //     //         message: "Unsupported XREAD command shape".to_string(),
                //     //     });
                //     // }
                //     // let block = cmp_resp_bytes_no_case(&args[0], b"BLOCK");
                //     let block = args2[argc].to_ascii_uppercase() == b"BLOCK";
                //     let duration = if block {
                //         argc += 1; // BLOCK
                //         let (ms, _) = integer::<u64>().parse(&args2[argc])?;
                //         argc += 1; // <millisecs>
                //         if ms == 0 {
                //             Duration::from_millis(u64::MAX)
                //         } else {
                //             Duration::from_millis(ms)
                //         }
                //     } else {
                //         Duration::from_hours(1)
                //     };

                //     if args2[argc].to_ascii_uppercase() != b"STREAMS" {
                //         return Err(ParseError {
                //             message: "Unsupported XREAD command shape".to_string(),
                //         });
                //     }
                //     argc += 1; // STREAMS

                //     // keys
                //     let l = args2[argc..].len();
                //     if l % 2 != 0 {
                //         return Err(ParseError {
                //             message: "Unsupported XREAD command shape".to_string(),
                //         });
                //     }
                //     let keys = &args2[argc..(argc + (l / 2))];
                //     // ids
                //     let id_slice = &args2[(argc + (l / 2))..];
                //     let mut ids: Vec<(u64, u64)> = Vec::new();
                //     for (i, id) in id_slice.iter().enumerate() {
                //         if id.len() == 1 && id[0] == b'$' {
                //             let store = stream_store.read().await;
                //             let last = store
                //                 .streams
                //                 .get(keys[i])
                //                 .and_then(|s| s.last_key_value())
                //                 .map(|(&k, _)| k)
                //                 .unwrap_or((0, 1));
                //             ids.push(last);
                //             continue;
                //         }
                //         match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(&id) {
                //             Ok(((tid, _, sid), _)) => ids.push((tid, sid)),
                //             _ => {
                //                 return Err(ParseError {
                //                     message: "Unsupported XREAD command shape, bad id".to_string(),
                //                 });
                //             }
                //         }
                //     }

                //     assert!(keys.len() == ids.len());

                //     if block {
                //         loop {
                //             // 1, Get or create notifiers for all target keys, under lock
                //             let notifiers: Vec<Arc<Notify>> = {
                //                 let mut store = stream_store.write().await;
                //                 keys.iter()
                //                     .map(|k| stream_waiter_for(&mut store, k))
                //                     .collect()
                //             }; // lock for store dropped

                //             // 2. Build& arm Notified futures before checking
                //             let mut futs: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
                //             for f in &mut futs {
                //                 f.as_mut().enable();
                //             }

                //             // 3. Try to pop - under lock, bruefly
                //             {
                //                 if let (data, is_empty) = process_xread_fetch_data(stream_store, keys, &ids).await {
                //                     if !is_empty {
                //                         return Ok(data);
                //                     }
                //                 }
                //             } // lock dropped

                //             // 4. Wait for any notifier with deadline
                //             let any = futures::future::select_all(futs);
                //             match timeout(duration, any).await {
                //                 Ok(_) => continue,
                //                 Err(_) => return Ok(Resp::NullArray),
                //             }
                //         }
                //     } else {
                //         let (data, _) = process_xread_fetch_data(stream_store, keys, &ids).await;
                //         Ok(data)
                //     }
                // }

                // async fn process_xread_fetch_data(
                //     stream_store: &Arc<RwLock<RedisStreamStore>>,
                //     keys: &[&Vec<u8>],
                //     ids: &Vec<(u64, u64)>,
                // ) -> (Resp, bool) {
                //     let stream_store = stream_store.read().await;

                //     let mut data: Vec<Resp> = Vec::new();
                //     let mut is_empty = true;

                //     for (i, &key) in keys.iter().enumerate() {
                //         let mut stream_data: Vec<Resp> = Vec::new();

                //         let stream = match stream_store.streams.get(key) {
                //             Some(stream) => stream,
                //             _ => continue,
                //         };

                //         stream_data.push(Resp::BulkString(key.to_vec()));

                //         let mut stream_row_data: Vec<Resp> = Vec::new();

                //         for (&k, v) in stream.range((Excluded(&ids[i]), Unbounded)) {
                //             is_empty = false;
                //             let mut row: Vec<Resp> = Vec::new();
                //             row.push(Resp::BulkString(
                //                 format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                //             ));
                //             row.push(Resp::Array(
                //                 v.iter()
                //                     .map(|s| Resp::BulkString(s.to_vec()))
                //                     .collect::<Vec<_>>(),
                //             ));

                //             stream_row_data.push(Resp::Array(row));
                //         }

                //         stream_data.push(Resp::Array(stream_row_data));

                //         data.push(Resp::Array(stream_data));
                //     }

                //     return (Resp::Array(data), is_empty);
                // }
            }
            _ => TryExecuteResult::Done(Reply::Null),
        }
    }

    //     async fn handle(
    //         &mut self,
    //         client_id: usize,
    //         cmd: Command,
    //         r: oneshot::Sender<Reply>,
    //         tx: mpsc::Sender<Command>,
    //     ) {
    //         let timeout = cmd.block_timeout();
    //         match self.try_execute(client_id, cmd) {
    //             TryExecuteResult::Done(result) => {
    //                 let _ = r.send(result);
    //             }
    //             TryExecuteResult::BlockingXread(waiter_id, keys) => {
    //                 self.stream_xread_waiters.insert(waiter_id, (r, keys));
    //                 let duration = Duration::from_millis(timeout.unwrap());
    //                 let tx2 = tx.clone();
    //                 tokio::spawn(async move {
    //                     sleep(duration).await;
    // //                    let _ = tx2.send(Envelope {}Command::Internal_Timeout { waiter_id }).await;
    // let _ = tx2.send(Envelope {client_id, Command::Internal_Timeout { waiter_id }, r}).await;
    //                 });
    //             }
    //         };
    //     }
}

// struct ModifiedValues {
//     // key name -> set of client ids that have modified value with that key
//     keys: HashMap<Vec<u8>, HashSet<usize>>,
// }

// impl ModifiedValues {
//     fn new() -> Self {
//         ModifiedValues {
//             keys: HashMap::new(),
//         }
//     }

//     fn register(&mut self, key: &[u8], client_id: usize) {
//         self.keys
//             .entry(key.to_vec())
//             .and_modify(|s| {
//                 (*s).insert(client_id);
//             })
//             .or_insert_with(|| HashSet::from([client_id]));
//     }

//     fn unregister(&mut self, key: &[u8], client_id: usize) {
//         if let Some(s) = self.keys.get_mut(&key.to_vec()) {
//             s.remove(&client_id);
//             if s.is_empty() {
//                 self.keys.remove(key);
//             }
//         }
//     }
// }

// milliseconds-seqeunce id
#[derive(Debug)]
enum XaddStreamIdInput {
    Explicit(u64, u64),
    AutoGenSeq(u64),
    AugoGen,
}
#[derive(Debug)]
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

// /**
//  * Process command
//  */
// fn process_echo(cmd: &Command) -> Result<Resp, ParseError> {
//     if cmd.args.len() == 1 {
//         Ok(Resp::BulkString(cmd.args[0].to_vec()))
//     } else {
//         Err(ParseError {
//             message: format!("Unsupported ECHO command shape: {:?}", cmd.args),
//         })
//     }
// }

// fn process_ping(cmd: &Command) -> Result<Resp, ParseError> {
//     match cmd.args.len() {
//         1 => Ok(Resp::BulkString(cmd.args[0].to_vec())),
//         0 => Ok(Resp::SimpleString(b"PONG".to_vec())),
//         _ => Err(ParseError {
//             message: format!("Unsupported PING command shape: {:?}", cmd.args),
//         }),
//     }
// }

// async fn process_set(
//     client_id: usize,
//     cmd: &Command,
//     in_tx: bool,
//     store: &Arc<RwLock<Store>>,
//     watches: &Arc<RwLock<Watches>>,
// ) -> Result<Resp, ParseError> {
//     match cmd.args.len() {
//         2 => {
//             let key = &cmd.args[0];
//             let value = &cmd.args[1];
//             let mut store = store.write().await;
//             let value = StoreValue {
//                 t: Instant::now(),
//                 ttl: None,
//                 value: value.to_vec(),
//             };
//             (*store).insert(key.to_vec(), value);
//             Ok(Resp::SimpleString(b"OK".to_vec()))
//         }
//         4 => {
//             let key = &cmd.args[0];
//             let value = &cmd.args[1];
//             let expx = &cmd.args[2];
//             let ttl = &cmd.args[3];
//             let n = match integer::<u64>().parse(ttl) {
//                 Ok((value, _)) => value,
//                 Err(ParseError { message }) => {
//                     return Err(ParseError {
//                         message: format!("Invalid time value for SET command: {:?}", message),
//                     });
//                 }
//             };
//             let ttl = match &expx[..] {
//                 b"EX" => Duration::from_secs(n),
//                 b"PX" => Duration::from_millis(n),
//                 _ => {
//                     return Err(ParseError {
//                         message: format!(
//                             "Invalid time spec for SET (should be PX or EX): {:?}",
//                             expx
//                         ),
//                     });
//                 }
//             };
//             let mut store = store.write().await;
//             let value = StoreValue {
//                 t: Instant::now(),
//                 ttl: Some(ttl),
//                 value: value.to_vec(),
//             };
//             (*store).insert(key.to_vec(), value);
//             Ok(Resp::SimpleString(b"OK".to_vec()))
//         }
//         _ => Err(ParseError {
//             message: format!("Unsupported SET command shape: {:?}", cmd.args),
//         }),
//     }
// }

// async fn process_get(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 1 {
//         return Err(ParseError {
//             message: format!("Unsupported GET command shape: {:?}", cmd.args),
//         });
//     }
//     let key = &cmd.args[0];
//     let store = store.read().await;
//     match store.get(key) {
//         Some(StoreValue { t, ttl, value }) => match ttl {
//             None => Ok(Resp::BulkString(value.to_vec())),
//             Some(duration) if *t + *duration < Instant::now() => Ok(Resp::Null),
//             Some(_) => Ok(Resp::BulkString(value.to_vec())),
//         },
//         None => Ok(Resp::Null),
//     }
// }

// Lists
// async fn process_list_rpush(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.is_empty() {
//         return Err(ParseError {
//             message: format!(
//                 "Unsupported RPUSH command shape, missing list name: {:?}",
//                 cmd.args
//             ),
//         });
//     }
//     let name = &cmd.args[0];

//     let mut elements = VecDeque::new();
//     for element in cmd.args.iter().skip(1) {
//         elements.push_back(element.to_vec());
//     }

//     let mut store = list_store.write().await;

//     store
//         .lists
//         .entry(name.to_vec())
//         .and_modify(|e| e.append(&mut elements))
//         .or_insert(elements);

//     let notifier = waiter_for(&mut store, name);
//     notifier.notify_waiters();

//     Ok(Resp::Integer(
//         store.lists.get(name).map_or(0, |l| l.len() as i64),
//     ))
// }

// async fn process_list_lpush(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.is_empty() {
//         return Err(ParseError {
//             message: format!(
//                 "Unsupported LPUSH command shape, missing list name: {:?}",
//                 cmd.args
//             ),
//         });
//     }
//     let name = &cmd.args[0];

//     let mut store = list_store.write().await;
//     store
//         .lists
//         .entry(name.to_vec())
//         .and_modify(|e| {
//             for element in cmd.args.iter().skip(1) {
//                 e.push_front(element.to_vec());
//             }
//         })
//         .or_insert_with(|| {
//             let mut l = VecDeque::new();
//             for element in cmd.args.iter().skip(1) {
//                 l.push_front(element.to_vec());
//             }
//             l
//         });

//     let notifier = waiter_for(&mut store, name);
//     notifier.notify_waiters();

//     Ok(Resp::Integer(
//         store.lists.get(name).map_or(0, |l| l.len() as i64),
//     ))
// }

// async fn process_list_llen(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 1 {
//         return Err(ParseError {
//             message: format!(
//                 "Unsupported LLEN command shape, missing list name: {:?}",
//                 cmd.args
//             ),
//         });
//     }
//     let name = &cmd.args[0];
//     let store = list_store.read().await;
//     let l = match store.lists.get(name) {
//         Some(l) => l.len(),
//         _ => 0,
//     };
//     Ok(Resp::Integer(l as i64))
// }

// async fn process_list_lpop(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     let (name, count) = match cmd.args.len() {
//         1 => Ok((&cmd.args[0], None)),
//         2 => match integer::<u32>().parse(&cmd.args[1])? {
//             (c, _) => Ok((&cmd.args[0], Some(c))),
//         },
//         _ => Err(ParseError {
//             message: format!(
//                 "Unsupported LPOP command shape, missing list name: {:?}",
//                 cmd.args
//             ),
//         }),
//     }?;

//     let mut store = list_store.write().await;
//     let list = store.lists.get_mut(name);
//     if list.is_none() {
//         return Ok(Resp::Null);
//     }
//     let list = list.unwrap();
//     if list.is_empty() {
//         return Ok(Resp::Null);
//     }
//     match count {
//         None => {
//             let el = list.pop_front().unwrap();
//             Ok(Resp::BulkString(el))
//         }
//         Some(count) => {
//             let mut result = Vec::new();
//             for _ in 0..count {
//                 match list.pop_front() {
//                     Some(el) => result.push(Resp::BulkString(el)),
//                     None => return Ok(Resp::Array(result)),
//                 }
//             }
//             Ok(Resp::Array(result))
//         }
//     }
// }

// async fn process_list_lrange(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 3 {
//         return Err(ParseError {
//             message: format!("Unsupported LRANGE command shape: {:?}", cmd.args),
//         });
//     }
//     let name = &cmd.args[0];
//     let (start, _) = integer::<i32>().parse(&cmd.args[1])?;
//     let (stop, _) = integer::<i32>().parse(&cmd.args[2])?;
//     let (name, start, stop) = (name, start, stop);
//     println!("start: {}, stop: {}", start, stop);

//     let mut result = Vec::new();
//     let store = list_store.read().await;
//     let list_option = store.lists.get(name);
//     if list_option.is_none() {
//         return Ok(Resp::Array(result));
//     }
//     let list = list_option.unwrap();

//     let a = if start < 0 {
//         start + list.len() as i32
//     } else {
//         start
//     };
//     let a = 0.max(a);

//     let b = if stop < 0 {
//         stop + list.len() as i32
//     } else {
//         stop
//     };
//     let b = (list.len() as i32 - 1).min(b);

//     if a > b {
//         return Ok(Resp::Array(result));
//     }

//     for i in (a as usize)..=(b as usize) {
//         result.push(Resp::BulkString(list[i].to_vec()));
//     }
//     Ok(Resp::Array(result))
// }

// async fn process_list_blpop(
//     cmd: &Command,
//     list_store: &Arc<RwLock<RedisListStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() < 2 {
//         return Err(ParseError {
//             message: format!("Unsupported BLPOP command shape: {:?}", cmd.args),
//         });
//     }
//     let t_bytes = cmd.args.back().unwrap();
//     let (t, _) = float::<f64>().parse(t_bytes).map_err(|_| ParseError {
//         message: format!("Timeout for BLPOP must be double (f64), got: {:?}", t_bytes),
//     })?;
//     let duration = if t == 0.0 {
//         Duration::MAX
//     } else {
//         Duration::from_micros((t * 1_000_000.) as u64)
//     };

//     let lists = cmd
//         .args
//         .iter()
//         .take(cmd.args.len() - 1)
//         .map(|l| l.to_vec())
//         .collect::<Vec<_>>();

//     println!(
//         "BLPOP: lists: {:?}, t: {:?}, duration: {:?}",
//         lists, t, duration
//     );

//     loop {
//         // 1, Get or create notifiers for all target keys, under lock
//         let notifiers: Vec<Arc<Notify>> = {
//             let mut store = list_store.write().await;
//             lists.iter().map(|k| waiter_for(&mut store, k)).collect()
//         }; // lock for store dropped

//         // 2. Build& arm Notified futures before checking
//         let mut futs: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
//         for f in &mut futs {
//             f.as_mut().enable();
//         }

//         // 3. Try to pop - under lock, bruefly
//         {
//             let mut store = list_store.write().await;
//             for k in &lists {
//                 if let Some(list) = store.lists.get_mut(k) {
//                     if let Some(head) = list.pop_front() {
//                         return Ok(Resp::Array(vec![
//                             Resp::BulkString(k.to_vec()),
//                             Resp::BulkString(head),
//                         ]));
//                     }
//                 }
//             }
//         } // lock dropped

//         // 4. Wait for any notifier with deadline
//         let any = futures::future::select_all(futs);
//         if t == 0.0 {
//             any.await;
//         } else {
//             match timeout(duration, any).await {
//                 Ok(_) => continue,
//                 Err(_) => return Ok(Resp::NullArray),
//             }
//         }
//     }
// }

// async fn process_type(
//     cmd: &Command,
//     store: &Arc<RwLock<Store>>,
//     list_store: &Arc<RwLock<RedisListStore>>,
//     stream_store: &Arc<RwLock<RedisStreamStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 1 {
//         return Err(ParseError {
//             message: format!("Unsupported TYPE command shape: {:?}", cmd.args),
//         });
//     }
//     let key = &cmd.args[0];
//     if store.read().await.contains_key(key) {
//         return Ok(Resp::SimpleString(b"string".to_vec()));
//     }
//     if list_store.read().await.lists.contains_key(key) {
//         return Ok(Resp::SimpleString(b"list".to_vec()));
//     }
//     if stream_store.read().await.streams.contains_key(key) {
//         return Ok(Resp::SimpleString(b"stream".to_vec()));
//     }
//     Ok(Resp::SimpleString(b"none".to_vec()))
// }

// async fn process_xadd(
//     cmd: &Command,
//     stream_store: &Arc<RwLock<RedisStreamStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() < 4 {
//         return Err(ParseError {
//             message: "Unsupported XADD command shape".to_string(),
//         });
//     }
//     let key = &cmd.args[0];
//     let id = &cmd.args[1];
//     let ski = match parse_input_stream_id(id) {
//         Some(k) => Ok(k),
//         _ => Err(ParseError {
//             message: "Unsupported XADD <id> key shape".to_string(),
//         }),
//     }?;
//     if (cmd.args.len() - 2) % 2 != 0 {
//         return Err(ParseError {
//             message: "Unsupported XADD command shape".to_string(),
//         });
//     }
//     let values = cmd
//         .args
//         .iter()
//         .skip(2)
//         .map(|v| v.to_vec())
//         .collect::<Vec<_>>();

//     let mut store = stream_store.write().await;

//     // Ensure that there is stream `key`:
//     store.streams.entry(key.to_vec()).or_insert(BTreeMap::new());

//     let (tid, sid) = match next_stream_id(ski, store.streams.get(key).unwrap()) {
//         Some(id) => id,
//         _ => {
//             return Ok(Resp::SimpleError(
//                 b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
//             ));
//         }
//     };

//     if (tid, sid) < (0, 1) {
//         return Ok(Resp::SimpleError(
//             b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
//         ));
//     }

//     if store.streams.get(key).unwrap().contains_key(&(tid, sid)) {
//         return Ok(Resp::SimpleError(
//             b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
//                 .to_vec(),
//         ));
//     }
//     if let Some((latest, _)) = store.streams.get(key).unwrap().last_key_value() {
//         if &(tid, sid) < latest {
//             return Ok(Resp::SimpleError(
//                 b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
//                     .to_vec(),
//             ));
//         }
//     }
//     store.streams.entry(key.to_vec()).and_modify(|bt| {
//         (*bt).insert((tid, sid), values);
//     });

//     let notifier = stream_waiter_for(&mut store, key);
//     notifier.notify_waiters();

//     Ok(Resp::BulkString(
//         format!("{}-{}", tid, sid).as_bytes().to_vec(),
//     ))
// }

// async fn process_xrange(
//     cmd: &Command,
//     stream_store: &Arc<RwLock<RedisStreamStore>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 3 {
//         return Err(ParseError {
//             message: "Unsupported XRANGE command shape".to_string(),
//         });
//     }
//     let key = &cmd.args[0];
//     let start = &cmd.args[1];
//     let end = &cmd.args[2];
//     let (start_tid, start_sid) = if start.len() == 1 && start[0] == b'-' {
//         (0, 1)
//     } else {
//         let ((start_tid, _, start_sid), _) =
//             and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(start)?;
//         (start_tid, start_sid)
//     };
//     let (end_tid, end_sid) = if end.len() == 1 && end[0] == b'+' {
//         (u64::MAX, u64::MAX)
//     } else {
//         let ((end_tid, _, end_sid), _) =
//             and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(end)?;
//         (end_tid, end_sid)
//     };
//     let (key, start, end) = (key, (start_tid, start_sid), (end_tid, end_sid));
//     let stream_store = stream_store.read().await;
//     let stream = match stream_store.streams.get(key) {
//         Some(stream) => Ok(stream),
//         _ => Err(ParseError {
//             message: format!("Stream not found, XRANGE: {:?}", key),
//         }),
//     }?;
//     let mut data: Vec<Resp> = Vec::new();
//     for (&k, v) in stream.range((Included(&start), Included(&end))) {
//         let mut row: Vec<Resp> = Vec::new();
//         row.push(Resp::BulkString(
//             format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
//         ));
//         row.push(Resp::Array(
//             v.iter()
//                 .map(|s| Resp::BulkString(s.to_vec()))
//                 .collect::<Vec<_>>(),
//         ));
//         data.push(Resp::Array(row));
//     }
//     Ok(Resp::Array(data))
// }

// fn cmp_bytes_no_case(a: &Vec<u8>, b: &[u8]) -> bool {
//     a.to_ascii_uppercase() != b.to_ascii_uppercase()
// }

// fn cmp_resp_bytes_no_case(a: &Resp, b: &[u8]) -> bool {
//     match a {
//         Resp::BulkString(sv) => sv.to_ascii_uppercase() != b.to_ascii_uppercase(),
//         _ => false,
//     }
// }

// async fn process_xread_fetch_data(
//     stream_store: &Arc<RwLock<RedisStreamStore>>,
//     keys: &[&Vec<u8>],
//     ids: &Vec<(u64, u64)>,
// ) -> (Resp, bool) {
//     let stream_store = stream_store.read().await;

//     let mut data: Vec<Resp> = Vec::new();
//     let mut is_empty = true;

//     for (i, &key) in keys.iter().enumerate() {
//         let mut stream_data: Vec<Resp> = Vec::new();

//         let stream = match stream_store.streams.get(key) {
//             Some(stream) => stream,
//             _ => continue,
//         };

//         stream_data.push(Resp::BulkString(key.to_vec()));

//         let mut stream_row_data: Vec<Resp> = Vec::new();

//         for (&k, v) in stream.range((Excluded(&ids[i]), Unbounded)) {
//             is_empty = false;
//             let mut row: Vec<Resp> = Vec::new();
//             row.push(Resp::BulkString(
//                 format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
//             ));
//             row.push(Resp::Array(
//                 v.iter()
//                     .map(|s| Resp::BulkString(s.to_vec()))
//                     .collect::<Vec<_>>(),
//             ));

//             stream_row_data.push(Resp::Array(row));
//         }

//         stream_data.push(Resp::Array(stream_row_data));

//         data.push(Resp::Array(stream_data));
//     }

//     return (Resp::Array(data), is_empty);
// }

// async fn process_xread(
//     cmd: &Command,
//     stream_store: &Arc<RwLock<RedisStreamStore>>,
// ) -> Result<Resp, ParseError> {
//     let mut argc = 0_usize;
//     let args2 = cmd.args.iter().collect::<Vec<_>>();
//     // if args.len() < 3 {
//     //     return Err(ParseError {
//     //         message: "Unsupported XREAD command shape".to_string(),
//     //     });
//     // }
//     // let block = cmp_resp_bytes_no_case(&args[0], b"BLOCK");
//     let block = args2[argc].to_ascii_uppercase() == b"BLOCK";
//     let duration = if block {
//         argc += 1; // BLOCK
//         let (ms, _) = integer::<u64>().parse(&args2[argc])?;
//         argc += 1; // <millisecs>
//         if ms == 0 {
//             Duration::from_millis(u64::MAX)
//         } else {
//             Duration::from_millis(ms)
//         }
//     } else {
//         Duration::from_hours(1)
//     };

//     if args2[argc].to_ascii_uppercase() != b"STREAMS" {
//         return Err(ParseError {
//             message: "Unsupported XREAD command shape".to_string(),
//         });
//     }
//     argc += 1; // STREAMS

//     // keys
//     let l = args2[argc..].len();
//     if l % 2 != 0 {
//         return Err(ParseError {
//             message: "Unsupported XREAD command shape".to_string(),
//         });
//     }
//     let keys = &args2[argc..(argc + (l / 2))];
//     // ids
//     let id_slice = &args2[(argc + (l / 2))..];
//     let mut ids: Vec<(u64, u64)> = Vec::new();
//     for (i, id) in id_slice.iter().enumerate() {
//         if id.len() == 1 && id[0] == b'$' {
//             let store = stream_store.read().await;
//             let last = store
//                 .streams
//                 .get(keys[i])
//                 .and_then(|s| s.last_key_value())
//                 .map(|(&k, _)| k)
//                 .unwrap_or((0, 1));
//             ids.push(last);
//             continue;
//         }
//         match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(&id) {
//             Ok(((tid, _, sid), _)) => ids.push((tid, sid)),
//             _ => {
//                 return Err(ParseError {
//                     message: "Unsupported XREAD command shape, bad id".to_string(),
//                 });
//             }
//         }
//     }

//     assert!(keys.len() == ids.len());

//     if block {
//         loop {
//             // 1, Get or create notifiers for all target keys, under lock
//             let notifiers: Vec<Arc<Notify>> = {
//                 let mut store = stream_store.write().await;
//                 keys.iter()
//                     .map(|k| stream_waiter_for(&mut store, k))
//                     .collect()
//             }; // lock for store dropped

//             // 2. Build& arm Notified futures before checking
//             let mut futs: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
//             for f in &mut futs {
//                 f.as_mut().enable();
//             }

//             // 3. Try to pop - under lock, bruefly
//             {
//                 if let (data, is_empty) = process_xread_fetch_data(stream_store, keys, &ids).await {
//                     if !is_empty {
//                         return Ok(data);
//                     }
//                 }
//             } // lock dropped

//             // 4. Wait for any notifier with deadline
//             let any = futures::future::select_all(futs);
//             match timeout(duration, any).await {
//                 Ok(_) => continue,
//                 Err(_) => return Ok(Resp::NullArray),
//             }
//         }
//     } else {
//         let (data, _) = process_xread_fetch_data(stream_store, keys, &ids).await;
//         Ok(data)
//     }
// }

// async fn process_incr(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 1 {
//         return Err(ParseError {
//             message: "Unsupported INCR command shape".to_string(),
//         });
//     }
//     let var_name = &cmd.args[0];
//     let mut store = store.write().await;
//     let (new_value, rsp_num) = if let Some(value) = store.get(var_name) {
//         let number = match integer::<i64>().parse(&value.value) {
//             Ok((n, _)) => Ok(n),
//             _ => {
//                 return Ok(Resp::SimpleError(
//                     b"ERR value is not an integer or out of range".to_vec(),
//                 ));
//             }
//         }?;
//         (
//             StoreValue {
//                 t: Instant::now(),
//                 ttl: value.ttl,
//                 value: (number + 1).to_string().as_bytes().to_vec(),
//             },
//             number + 1,
//         )
//     } else {
//         (
//             StoreValue {
//                 t: Instant::now(),
//                 ttl: None,
//                 value: 1.to_string().as_bytes().to_vec(),
//             },
//             1,
//         )
//     };

//     store.insert(var_name.to_vec(), new_value);

//     Ok(Resp::Integer(rsp_num))
// }

// async fn process_watch(
//     client_id: usize,
//     cmd: &Command,
//     store: &Arc<RwLock<Store>>,
//     watches: &Arc<RwLock<Watches>>,
// ) -> Result<Resp, ParseError> {
//     if cmd.args.len() != 1 {
//         return Err(ParseError {
//             message: "Unsupported WATCH command shape".to_string(),
//         });
//     }
//     let var_name = &cmd.args[0];
//     let mut watches = watches.write().await;
//     watches
//         .entry(var_name.clone())
//         .and_modify(|s| {
//             (*s).insert(client_id);
//         })
//         .or_insert(HashSet::from([client_id]));
//     Ok(Resp::SimpleString(b"OK".to_vec()))
// }

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
        Reply::Error(s) => {
            write_bytes(&mut out, &[b'-']);
            write_bytes(&mut out, &s.as_bytes().to_vec());
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
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
                //encode_resp(e, out);
                out.append(&mut encode_reply(e));
            }
        }
    }

    out
}

async fn write_reply(stream: &mut TcpStream, reply: &Reply) -> std::io::Result<()> {
    println!("write_reply received reply: {:?}", reply);
    let mut out = encode_reply(reply);
    let result = stream.write_all(&out[..]).await;
    result
}

#[derive(Debug)]
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
    Internal_Execute_Tx {
        commands: Vec<Command>,
    },
    Internal_Discard_Tx,
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
                    let (ttl, _) = integer::<u64>().parse(&tmp[..]).unwrap();
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
                let (start, _) = integer::<i32>().parse(&bs[0][..]).unwrap();
                let (end, _) = integer::<i32>().parse(&bs[1][..]).unwrap();
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
                    let (c, _) = integer::<u32>().parse(&bs[0][..]).unwrap();
                    Some(c)
                } else {
                    None
                };
                Some(Command::Lpop { key, count })
            }
            b"BLPOP" => {
                let tmp = bs.pop_back().unwrap();
                let (timeout, _) = float::<f64>().parse(&tmp[..]).unwrap();
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
                    let (ms, _) = integer::<u64>().parse(&m[..]).unwrap();
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
            Command::Blpop { keys: _, timeout } => Some((timeout * 1_000.) as u64),
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
                match store.try_execute(client_id, command) {
                    TryExecuteResult::Done(reply) => {
                        let _ = reply_channel.send(reply);
                    }
                    TryExecuteResult::BlockingXread(waiter_id, keys_ids) => {
                        // Register interest in updates vs timeout conundrums
                        println!("REGISTERING WAITER: {:?}, keys: {:?}", waiter_id, keys_ids);
                        store
                            .stream_xread_waiters
                            .insert(waiter_id, (reply_channel, keys_ids));
                        println!("WHOLE WAITER STATE: {:?}", store.stream_xread_waiters);
                        let duration = Duration::from_millis(timeout.unwrap());
                        println!("SLEEP Duration: {:?}", duration);
                        let tx2 = tx.clone();
                        tokio::spawn(async move {
                            sleep(duration).await;
                            let _ = tx2.send(Envelope::TimeoutXread { waiter_id }).await;
                        });
                    }
                }
            }
            Envelope::TimeoutXread { waiter_id } => {
                // Deregister interest if there's any, and remove interestent
                println!("DEREGISTERING WAITER: {:?}", waiter_id);
                println!(
                    "DEREGISTER WHOLE WAITER STATE: {:?}",
                    store.stream_xread_waiters
                );
                if let Some((reply_channel, _)) = store.stream_xread_waiters.remove(&waiter_id) {
                    let _ = reply_channel.send(Reply::NullArray);
                }
            }
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

    while let Ok(n) = stream.read(&mut buffer).await {
        if n == 0 {
            break;
        }

        // Parse input resp into Vec<Bytes>
        let (input, _) = parse_input_resp(&buffer).unwrap();

        let command = Command::from_bytes(input).unwrap();

        println!(
            "Client {} received command {:?}, queue = {:?}",
            client_id, &command, &queue
        );
        let result = match (&command, &mut queue) {
            // Start tx
            (Command::Multi, None) => {
                queue = Some(VecDeque::new());
                Reply::Ok
            }
            (Command::Exec, Some(_)) => {
                let commands = queue.take().unwrap();
                let tx = Command::Internal_Execute_Tx {
                    commands: commands.into(),
                };
                execute_command(&producer_ch, client_id, tx).await
            }
            (Command::Exec, None) => {
                Reply::SimpleError("ERR EXEC without MULTI".as_bytes().to_vec())
            }
            (Command::Watch { keys: _ }, Some(_)) => {
                Reply::SimpleError("ERR WATCH inside MULTI is not allowed".as_bytes().to_vec())
            }
            (Command::Discard, None) => {
                Reply::SimpleError("ERR DISCARD without MULTI".as_bytes().to_vec())
            }
            (Command::Discard, Some(_)) => {
                queue = None;
                execute_command(&producer_ch, client_id, Command::Internal_Discard_Tx).await
            }

            // Inside tx
            (_, Some(q)) => {
                q.push_back(command);
                Reply::SimpleString("QUEUED".as_bytes().to_vec())
            }
            (_, None) => execute_command(&producer_ch, client_id, command).await,
        };

        //println!("Client {} received reply {:?}", client_id, &reply);

        let r2 = write_reply(&mut stream, &result).await;
        //println!("write to stream result: {:?}", &r2);

        let _ = stream.flush().await;
        buffer.fill(0u8);
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

    let r1 = store_ch.send(envelope).await; // this is store process
    //println!("send to store result: {:?}", &r1);

    // store process must send reply in all cases. how to ensure / enforce this?
    let reply = match reply_ch_receiver.await {
        Ok(r) => r,
        Err(_) => panic!("Something wrong with processing command"),
    };
    reply
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let client_counter = AtomicUsize::new(1);
    // mpsc == Multiple Producer Single Consumer
    let (tx, rx) = mpsc::channel::<Envelope>(1024);
    let store = Store::new();
    tokio::spawn(run_store(store, rx, tx.clone()));

    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let client_id = client_counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let client_producer_ch = tx.clone();
        tokio::spawn(handle_client(client_id, stream, client_producer_ch));
    }
}
