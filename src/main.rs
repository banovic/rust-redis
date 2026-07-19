#![allow(unused_imports)]
//use clap::Parser;
use core::{num, str};
use futures::channel::oneshot;
use sha2::{Digest, Sha256};
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

mod types;
use types::*;
mod parser;
use parser::*;
mod resp;
use resp::Resp;
mod command;
use command::Command;
mod rdb;
use rdb::Rdb;
mod aof;
use aof::Aof;
mod pubsub;
use pubsub::PubSub;
mod client;
use client::ClientRunMode;
mod sorted_sets;
use sorted_sets::{SafeFloat, SortedSets};
mod geocoding;
use geocoding::*;

use crate::PrimitiveValue::List;
use crate::client::ClientDispatch;
use crate::command::{XreadStreamIdInput, next_stream_id};
use crate::rdb::{RdbString, RdbValueExpiration};
use crate::resp::parse_resp;

fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

fn get_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

type RedisStream = BTreeMap<StreamKey, Vec<Bytes>>;

#[derive(Debug)]

enum PrimitiveValue {
    Str(Bytes),
    List(VecDeque<Bytes>),
    Stream(RedisStream),
}

#[derive(Debug)]
struct Value {
    value: PrimitiveValue,
    // Absolute expiration timestamp in milliseconds
    expire_ms: Option<u64>,
}

type ClientId = usize;
type WaiterId = usize;

enum TryExecuteResult {
    Done(Resp),
    BlockingXread(WaiterId, Vec<Key>, Vec<(u64, u64)>),
    BlockingBlpop(WaiterId, Vec<Key>),
    WaitCommand(WaiterId, u64, u64),
    None, // do nothing, noop
}
#[derive(Debug)]
struct Store {
    is_replica: bool,
    // Clients - by client id, value is pair: whether client is replica or not (default not),
    // and channel on which client will receive store push messages
    clients: HashMap<ClientId, (bool, mpsc::Sender<StorePush>)>,

    data: HashMap<Key, Value>,
    waiter_id: WaiterId,
    watched_keys: HashMap<Key, HashSet<usize>>,
    stream_xread_waiters: HashMap<WaiterId, (oneshot::Sender<Resp>, Vec<Key>, Vec<(u64, u64)>)>,
    list_blpop_waiters: HashMap<WaiterId, (oneshot::Sender<Resp>, Vec<Key>)>,
    // Clients waiting for WAIT command to complete, key is waiter id and value is tuple:
    // channel to respond to client (Resp), num of replicas client wanted and the target
    // replication offset they must have acked to count as caught up.
    wait_waiters: HashMap<WaiterId, (oneshot::Sender<Resp>, u64, u64)>,
    // Bytes of write commands propagated to replicas so far.
    master_ack: u64,
    // Last replication offset acked by each replica, keyed by client id.
    replica_acks: HashMap<ClientId, u64>,
    pending_write_commands_for_wait: bool,
    config: Config,
    // dir: String,
    // dbfilename: Option<String>,
    pubsub: PubSub,
    sorted_sets: SortedSets,
    users: HashMap<String, HashSet<String>>,
}

impl Store {
    async fn new(is_replica: bool, config: Config) -> Self {
        let rdb = {
            if config.dbfilename.is_some() {
                let d = config.dir.clone();
                let f = config.dbfilename.clone().unwrap();
                let filename = format!("{}/{}", d, f);
                Rdb::read_from_file(&filename).await.ok()
            } else {
                None
            }
        };

        match rdb {
            Some(db) => Self::from_rdb(is_replica, config, &db),
            None => Self {
                is_replica,
                clients: HashMap::new(),
                //replicas: HashMap::new(),
                data: HashMap::new(),
                waiter_id: 0,
                watched_keys: HashMap::new(),
                stream_xread_waiters: HashMap::new(),
                list_blpop_waiters: HashMap::new(),
                wait_waiters: HashMap::new(),
                master_ack: 0,
                replica_acks: HashMap::new(),
                pending_write_commands_for_wait: false,
                config,
                pubsub: PubSub::new(),
                sorted_sets: SortedSets::new(),
                users: HashMap::from([("default".to_string(), HashSet::new())]),
            },
        }
    }

    fn from_rdb(is_replica: bool, config: Config, rdb: &Rdb) -> Self {
        let mut data = HashMap::new();

        for (k, v) in &rdb.data {
            let expire_ms = match v {
                RdbString {
                    encoding: _,
                    value: _,
                    expire: RdbValueExpiration::None,
                } => None,

                RdbString {
                    encoding: _,
                    value: _,
                    expire: RdbValueExpiration::Milliseconds(ms),
                } => Some(*ms),

                RdbString {
                    encoding: _,
                    value: _,
                    expire: RdbValueExpiration::Seconds(secs),
                } => Some(*secs as u64 * 1000),
            };
            data.insert(
                k.clone(),
                Value {
                    value: PrimitiveValue::Str(v.value.clone()),
                    expire_ms,
                },
            );
        }

        Self {
            is_replica,
            clients: HashMap::new(),
            //            replicas: HashMap::new(),
            data,
            waiter_id: 0,
            watched_keys: HashMap::new(),
            stream_xread_waiters: HashMap::new(),
            list_blpop_waiters: HashMap::new(),
            wait_waiters: HashMap::new(),
            master_ack: 0,
            replica_acks: HashMap::new(),
            pending_write_commands_for_wait: false,
            config,
            pubsub: PubSub::new(),
            sorted_sets: SortedSets::new(),
            users: HashMap::from([("default".to_string(), HashSet::new())]),
        }
    }

    fn to_rdb(&self) -> Rdb {
        let mut rdb = Rdb::new();
        for (k, v) in &self.data {
            if let Value {
                value: PrimitiveValue::Str(s),
                expire_ms,
            } = v
            {
                let r_key = k.clone();
                match expire_ms {
                    Some(ms) => rdb.set(
                        r_key,
                        RdbString {
                            encoding: 0,
                            value: s.clone(),
                            expire: RdbValueExpiration::Milliseconds(*ms),
                        },
                    ),
                    None => rdb.set(
                        r_key,
                        RdbString {
                            encoding: 0,
                            value: s.clone(),
                            expire: RdbValueExpiration::None,
                        },
                    ),
                };
            }
        }
        rdb
    }

    // How many replicas have acked an offset >= target. With no writes pending
    // (target == 0) every connected replica is caught up by definition.
    fn count_acked(&self, master_ack: u64) -> u64 {
        let replicas_count = self
            .clients
            .values()
            .filter(|(is_replica, _)| *is_replica)
            .count();

        if master_ack == 0 {
            return replicas_count as u64;
        }
        self.replica_acks
            .values()
            .filter(|&&replica_ack| replica_ack >= master_ack)
            .count() as u64
    }

    fn map_list<F, R>(&self, key: &Key, f: F) -> Option<R>
    where
        F: Fn(&VecDeque<Bytes>) -> R,
    {
        if let Some(Value {
            value: PrimitiveValue::List(list),
            expire_ms: _,
        }) = self.data.get(key)
        {
            Some(f(list))
        } else {
            None
        }
    }

    fn fetch_xread(&self, keys: &[Key], ids: &[(u64, u64)]) -> (Vec<Resp>, bool) {
        let mut rows: Vec<Resp> = Vec::new();
        let mut is_empty = true;

        for (i, key) in keys.iter().enumerate() {
            let mut stream_rows: Vec<Resp> = Vec::new();

            if let Some(Value {
                value: PrimitiveValue::Stream(stream),
                expire_ms: _,
            }) = self.data.get(&key)
            {
                stream_rows.push(Resp::BulkString(key.0.clone()));

                let mut stream_row_data: Vec<Resp> = Vec::new();

                for (&k, v) in stream.range((Excluded(ids[i]), Unbounded)) {
                    is_empty = false;
                    let mut row: Vec<Resp> = Vec::new();
                    row.push(Resp::BulkString(
                        format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                    ));
                    row.push(Resp::Array(
                        v.iter()
                            .map(|s| Resp::BulkString(s.to_vec()))
                            .collect::<Vec<_>>(),
                    ));

                    stream_row_data.push(Resp::Array(row));
                }

                stream_rows.push(Resp::Array(stream_row_data));

                rows.push(Resp::Array(stream_rows));
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
            if let Some((Resp_channel, keys, ids)) = self.stream_xread_waiters.remove(&waiter_id) {
                let (rows, _) = self.fetch_xread(&keys, &ids);
                let _ = Resp_channel.send(Resp::Array(rows));
            }
        }
    }

    fn fetch_blpop(&mut self, keys: &[Key]) -> (Resp, bool) {
        for k in keys {
            if let Some(Value {
                value: PrimitiveValue::List(list),
                expire_ms: _,
            }) = self.data.get_mut(k)
            {
                if let Some(head) = list.pop_front() {
                    return (
                        Resp::Array(vec![Resp::BulkString(k.clone().0), Resp::BulkString(head)]),
                        false,
                    );
                }
            }
        }
        (Resp::NullArray, true)
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
            let (Resp_channel, keys) = self.list_blpop_waiters.remove(&waiter_id).unwrap();
            let (rows, is_empty) = self.fetch_blpop(&keys);
            if !is_empty {
                let _ = Resp_channel.send(rows);
                self.list_blpop_waiters.remove(&waiter_id);
            } else {
                self.list_blpop_waiters
                    .insert(waiter_id, (Resp_channel, keys));
            }
        }
    }

    fn command_set(&mut self, client_id: ClientId, cmd: &Command) -> TryExecuteResult {
        if let Command::Set { key, value, ex, px } = cmd {
            let expire_ms = match (ex, px) {
                (Some(secs), _) => Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                        + secs * 1000,
                ),
                (_, Some(ms)) => Some(
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64
                        + ms,
                ),
                _ => None,
            };
            let v = Value {
                value: PrimitiveValue::Str(value.clone()),
                expire_ms,
            };
            self.data.insert(key.clone(), v);
            TryExecuteResult::Done(Resp::simple_string("OK"))
        } else {
            panic!("Not SET command");
        }
    }

    fn command_config_get(&self, parameter: &str) -> TryExecuteResult {
        let value = match &parameter[..] {
            "dir" => &Some(self.config.dir.clone()),
            "dbfilename" => &self.config.dbfilename,
            "appendonly" => &Some(self.config.appendonly.clone()),
            "appenddirname" => &Some(self.config.appenddirname.clone()),
            "appendfilename" => &Some(self.config.appendfilename.clone()),
            "appendfsync" => &Some(self.config.appendfsync.clone()),
            _ => &None,
        };
        let mut params: Vec<Resp> = Vec::new();
        if let Some(v) = value {
            params.push(Resp::bulk_string(parameter));
            params.push(Resp::BulkString(v.as_bytes().to_vec()));
        }
        TryExecuteResult::Done(Resp::Array(params))
    }

    fn command_keys(&self, pattern: &String) -> TryExecuteResult {
        let mut keys: Vec<Resp> = Vec::new();
        for key in self.data.keys() {
            keys.push(Resp::BulkString(key.0.clone()));
        }
        TryExecuteResult::Done(Resp::Array(keys))
    }

    fn command_get(&self, key: &Key) -> TryExecuteResult {
        match self.data.get(&key) {
            Some(Value {
                value: PrimitiveValue::Str(value),
                expire_ms,
            }) => match expire_ms {
                None => TryExecuteResult::Done(Resp::BulkString(value.to_vec())),
                Some(ms) if *ms < get_ms() => TryExecuteResult::Done(Resp::NullBulkString),
                Some(_) => TryExecuteResult::Done(Resp::BulkString(value.to_vec())),
            },
            Some(_) => TryExecuteResult::Done(Resp::NullBulkString), // TODO - error wrong type
            None => TryExecuteResult::Done(Resp::NullBulkString),
        }
    }

    fn command_subscribe(&mut self, client_id: ClientId, channels: &[String]) -> TryExecuteResult {
        for channel in channels {
            self.pubsub.add_subscription(client_id, channel);
        }
        let rsp = Resp::array(vec![
            Resp::bulk_string("subscribe"),
            Resp::bulk_string(&channels[0].clone()),
            Resp::integer(self.pubsub.get_client_subscriptions(client_id) as i64),
        ]);
        TryExecuteResult::Done(rsp)
    }

    fn command_replconf_ack(&mut self, client_id: ClientId, ack_bytes: u64) -> TryExecuteResult {
        self.replica_acks.insert(client_id, ack_bytes);

        // Complete any WAIT whose threshold is now met.
        let done: Vec<WaiterId> = self
            .wait_waiters
            .iter()
            .filter(|(_, (_, numreplicas, target))| self.count_acked(*target) >= *numreplicas)
            .map(|(waiter_id, _)| *waiter_id)
            .collect();

        for waiter_id in done {
            let (reply_channel, _numreplicas, target) =
                self.wait_waiters.remove(&waiter_id).unwrap();
            let _ = reply_channel.send(Resp::Integer(self.count_acked(target) as i64));
        }

        TryExecuteResult::None
    }

    fn command_publish(
        &mut self,
        client_id: ClientId,
        channel: &str,
        message: &str,
    ) -> TryExecuteResult {
        let resp = Resp::array(vec![
            Resp::bulk_string("message"),
            Resp::bulk_string(channel),
            Resp::bulk_string(message),
        ]);
        let push = StorePush::Message(resp);
        if let Some(clients) = self.pubsub.subscriptions.get(channel) {
            for client_id in clients {
                if let Some((_, tx)) = self.clients.get(client_id) {
                    let _ = tx.try_send(push.clone());
                }
            }
        }
        let c = self.pubsub.subscribers_count(channel);
        TryExecuteResult::Done(Resp::Integer(c as i64))
    }

    fn command_unsubscribe(
        &mut self,
        client_id: ClientId,
        channels: &Vec<String>,
    ) -> TryExecuteResult {
        for channel in channels {
            self.pubsub.unsubscribe(client_id, channel);
        }
        let rsp = Resp::array(vec![
            Resp::bulk_string("unsubscribe"),
            Resp::bulk_string(&channels[0].clone()),
            Resp::integer(self.pubsub.get_client_subscriptions(client_id) as i64),
        ]);
        TryExecuteResult::Done(rsp)
    }

    fn command_zadd(
        &mut self,
        client_id: ClientId,
        key: &String,
        score: f64,
        member: &String,
    ) -> TryExecuteResult {
        let r = self.sorted_sets.insert(key, score, member);
        TryExecuteResult::Done(Resp::Integer(r as i64))
    }

    fn command_zrank(&mut self, key: &String, member: &String) -> TryExecuteResult {
        let r = self.sorted_sets.rank(key, member);
        match r {
            Some(rank) => TryExecuteResult::Done(Resp::Integer(rank as i64)),
            None => TryExecuteResult::Done(Resp::NullBulkString),
        }
    }

    fn command_zrange(&mut self, key: &String, start: i32, stop: i32) -> TryExecuteResult {
        let r = self.sorted_sets.range(key, start, stop);
        let els = r
            .iter()
            .map(|r| Resp::bulk_string(&r.clone()))
            .collect::<Vec<_>>();

        TryExecuteResult::Done(Resp::array(els))
    }

    fn command_zcard(&self, key: &String) -> TryExecuteResult {
        TryExecuteResult::Done(Resp::integer(self.sorted_sets.card(key) as i64))
    }

    fn command_zscore(&mut self, key: &String, member: &String) -> TryExecuteResult {
        match self.sorted_sets.score(key, member) {
            Some(score) => TryExecuteResult::Done(Resp::bulk_string(&score.to_string())),
            None => TryExecuteResult::Done(Resp::NullBulkString),
        }
    }

    fn command_zrem(&mut self, key: &String, member: &String) -> TryExecuteResult {
        let r = self.sorted_sets.rem(key, member);
        TryExecuteResult::Done(Resp::integer(r as i64))
    }

    fn command_geoadd(
        &mut self,
        key: &String,
        longitude: f64,
        latitude: f64,
        member: &String,
    ) -> TryExecuteResult {
        if longitude < -180.0
            || longitude > 180.0
            || latitude < -85.05112878
            || latitude > 85.05112878
        {
            TryExecuteResult::Done(Resp::simple_error(&format!(
                "ERR invalid longitude,latitude pair {}, {}",
                longitude, latitude
            )))
        } else {
            let score = encode(latitude, longitude) as f64;
            let r = self.sorted_sets.insert(key, score, member);
            TryExecuteResult::Done(Resp::Integer(r as i64))
        }
    }

    fn command_geopos(&mut self, key: &String, members: &[String]) -> TryExecuteResult {
        let mut scores = Vec::new();
        for member in members {
            let score = match self.sorted_sets.score(key, member) {
                Some(score) => {
                    let coord = decode(score as u64);
                    Resp::array(vec![
                        Resp::bulk_string(&coord.longitude.to_string()),
                        Resp::bulk_string(&coord.latitude.to_string()),
                    ])
                }
                None => Resp::NullArray,
            };
            scores.push(score);
        }
        TryExecuteResult::Done(Resp::array(scores))
    }

    fn command_geodist(
        &mut self,
        key: &String,
        member1: &String,
        member2: &String,
    ) -> TryExecuteResult {
        if let (Some(score1), Some(score2)) = (
            self.sorted_sets.score(key, member1),
            self.sorted_sets.score(key, member2),
        ) {
            let coord1 = decode(score1 as u64);
            let coord2 = decode(score2 as u64);
            let dist = haversine(&coord1, &coord2);
            TryExecuteResult::Done(Resp::bulk_string(&dist.to_string()))
        } else {
            TryExecuteResult::Done(Resp::NullBulkString)
        }
    }

    fn command_geosearch(
        &mut self,
        key: &String,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: &String,
    ) -> TryExecuteResult {
        let mut finds = Vec::new();
        let c1 = Coordinates {
            longitude,
            latitude,
        };
        assert!(unit.to_uppercase() == "M"); // since haversine works in meters as well
        if let Some(set) = self.sorted_sets.data.get(key) {
            for (score, member) in set {
                let c2 = decode(score.0 as u64);
                if haversine(&c1, &c2) <= radius {
                    finds.push(Resp::bulk_string(member));
                }
            }
        }
        TryExecuteResult::Done(Resp::array(finds))
    }

    fn command_acl_whoami(&self) -> TryExecuteResult {
        TryExecuteResult::Done(Resp::bulk_string("default"))
    }

    fn command_acl_getuser(&self, username: &String) -> TryExecuteResult {
        let passwords_resp = if let Some(passwords) = self.users.get(username) {
            passwords.iter().map(|p| Resp::bulk_string(p)).collect()
        } else {
            vec![]
        };

        let flags_resp = if passwords_resp.is_empty() {
            vec![Resp::bulk_string("nopass")]
        } else {
            vec![]
        };
        let res = Resp::array(vec![
            Resp::bulk_string("flags"),
            Resp::array(flags_resp),
            Resp::bulk_string("passwords"),
            Resp::array(passwords_resp),
        ]);
        TryExecuteResult::Done(res)
    }

    fn command_acl_setuser(&mut self, username: &String, password: &String) -> TryExecuteResult {
        let hash = Sha256::digest(password.as_bytes());
        let hash_string: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
        self.users
            .entry(username.clone())
            .and_modify(|set| {
                (*set).insert(hash_string.clone());
            })
            .or_insert(HashSet::from([hash_string]));
        TryExecuteResult::Done(Resp::simple_string("OK"))
    }

    fn command_auth(&mut self, username: &String, password: &String) -> TryExecuteResult {
        if let Some(passwords) = self.users.get(username) {
            let hash = Sha256::digest(password.as_bytes());
            let hash_string: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
            if passwords.contains(&hash_string) {
                return TryExecuteResult::Done(Resp::simple_string("OK"));
            }
        }
        TryExecuteResult::Done(Resp::simple_error(
            "WRONGPASS invalid username-password pair or user is disabled.",
        ))
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
                let expire_ms = match (ex, px) {
                    (Some(secs), _) => Some(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                            + secs * 1000,
                    ),
                    (_, Some(ms)) => Some(
                        SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_millis() as u64
                            + ms,
                    ),
                    _ => None,
                };
                let v = Value {
                    value: PrimitiveValue::Str(value),
                    expire_ms,
                };
                self.data.insert(key, v);
                TryExecuteResult::Done(Resp::simple_string("OK"))
            }

            Command::Get { key } => self.command_get(&key),

            Command::Watch { keys } => {
                for key in keys {
                    self.watched_keys
                        .entry(key)
                        .and_modify(|s| {
                            (*s).insert(client_id);
                        })
                        .or_insert_with(|| HashSet::from([client_id]));
                }
                TryExecuteResult::Done(Resp::simple_string("OK"))
            }

            Command::Unwatch => {
                // Cleanup watched keys for this client, and return OK simple string
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                TryExecuteResult::Done(Resp::SimpleString("OK".as_bytes().to_vec()))
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

                    return TryExecuteResult::Done(Resp::NullArray);
                }

                // Execute tx
                let mut replies: Vec<Resp> = Vec::new();
                for cmd in commands {
                    let resp = match self.try_execute(client_id, cmd) {
                        TryExecuteResult::Done(r) => r,
                        _ => Resp::NullArray,
                    };
                    replies.push(resp);
                }

                // Cleanup watched keys for this client, and return null array
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                println!("tx: store2: {:?}", self);
                TryExecuteResult::Done(Resp::Array(replies))
            }
            Command::Discard => {
                // Cleanup watched keys for this client, and return OK
                for (_, clients) in &mut self.watched_keys {
                    clients.remove(&client_id);
                }
                self.watched_keys.retain(|_, clients| !clients.is_empty());
                TryExecuteResult::Done(Resp::simple_string("OK"))
            }
            Command::Incr { key } => {
                if let Some(Value {
                    value: PrimitiveValue::Str(s),
                    expire_ms,
                }) = self.data.get_mut(&key)
                {
                    let result = parser::Parser::parse(&integer::<i64>(), s);
                    match result {
                        Ok((n, _)) => {
                            *s = (n + 1).to_string().as_bytes().to_vec();
                            TryExecuteResult::Done(Resp::Integer(n + 1))
                        }
                        _ => TryExecuteResult::Done(Resp::simple_error(
                            "ERR value is not an integer or out of range",
                        )),
                    }
                } else {
                    self.data.insert(
                        key,
                        Value {
                            value: PrimitiveValue::Str(1.to_string().as_bytes().to_vec()),
                            expire_ms: None,
                        },
                    );
                    TryExecuteResult::Done(Resp::Integer(1))
                }
            }
            Command::Xadd {
                key,
                id,
                field_values,
            } => {
                // Ensure that there is stream `key`:
                self.data.entry(key.clone()).or_insert(Value {
                    value: PrimitiveValue::Stream(BTreeMap::new()),
                    expire_ms: None,
                });

                if let Some(Value {
                    value: PrimitiveValue::Stream(stream),
                    expire_ms: _,
                }) = self.data.get_mut(&key)
                {
                    let (tid, sid) = match next_stream_id(id, stream) {
                        Some(id) => id,
                        _ => {
                            return TryExecuteResult::Done(Resp::SimpleError(
                                b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
                            ));
                        }
                    };

                    if (tid, sid) < (0, 1) {
                        return TryExecuteResult::Done(Resp::SimpleError(
                            b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
                        ));
                    }

                    if stream.contains_key(&(tid, sid)) {
                        return TryExecuteResult::Done(Resp::SimpleError(
                            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                .to_vec(),
                        ));
                    }

                    if let Some((latest, _)) = stream.last_key_value() {
                        if &(tid, sid) < latest {
                            return TryExecuteResult::Done(Resp::SimpleError(
                                b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                    .to_vec(),
                            ));
                        }
                    }

                    stream.insert((tid, sid), field_values);

                    self.notify_xread_waiters(&key);

                    TryExecuteResult::Done(Resp::BulkString(
                        format!("{}-{}", tid, sid).as_bytes().to_vec(),
                    ))
                } else {
                    TryExecuteResult::Done(Resp::SimpleError(
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
                    return TryExecuteResult::Done(Resp::Array(rows));
                }

                if milliseconds.is_none() {
                    return TryExecuteResult::Done(Resp::Array(vec![]));
                }

                self.waiter_id += 1;
                TryExecuteResult::BlockingXread(self.waiter_id, keys, real_ids)
            }

            Command::Xrange { key, start, end } => {
                if let Some(Value {
                    value: PrimitiveValue::Stream(stream),
                    expire_ms: _,
                }) = self.data.get(&key)
                {
                    let mut data: Vec<Resp> = Vec::new();
                    for (&k, v) in stream.range((Included(&start), Included(&end))) {
                        let mut row: Vec<Resp> = Vec::new();
                        row.push(Resp::BulkString(
                            format!("{}-{}", k.0, k.1).as_bytes().to_vec(),
                        ));
                        row.push(Resp::Array(
                            v.iter()
                                .map(|s| Resp::BulkString(s.to_vec()))
                                .collect::<Vec<_>>(),
                        ));
                        data.push(Resp::Array(row));
                    }
                    TryExecuteResult::Done(Resp::Array(data))
                } else {
                    TryExecuteResult::Done(Resp::SimpleError(
                        format!("Stream not found, XRANGE: {:?}", key)
                            .as_bytes()
                            .to_vec(),
                    ))
                }
            }

            Command::Type { key } => match self.data.get(&key) {
                Some(Value {
                    value: PrimitiveValue::List(_),
                    expire_ms: _,
                }) => TryExecuteResult::Done(Resp::SimpleString("list".as_bytes().to_vec())),
                Some(Value {
                    value: PrimitiveValue::Str(_),
                    expire_ms: _,
                }) => TryExecuteResult::Done(Resp::SimpleString("string".as_bytes().to_vec())),
                Some(Value {
                    value: PrimitiveValue::Stream(_),
                    expire_ms: _,
                }) => TryExecuteResult::Done(Resp::SimpleString("stream".as_bytes().to_vec())),
                _ => TryExecuteResult::Done(Resp::SimpleString("none".as_bytes().to_vec())),
            },

            Command::Rpush { key, elements } => {
                let n = match self.data.get_mut(&key) {
                    Some(Value {
                        value: PrimitiveValue::List(list),
                        expire_ms: _,
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
                                value: PrimitiveValue::List(elements.into()),
                                expire_ms: None,
                            },
                        );
                        n
                    }
                };

                self.notify_blpop_waiters(&key);

                TryExecuteResult::Done(Resp::Integer(n as i64))
            }

            Command::Lpush { key, mut elements } => {
                let n = match self.data.get_mut(&key) {
                    Some(Value {
                        value: PrimitiveValue::List(list),
                        expire_ms: _,
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
                                value: PrimitiveValue::List(elements.into()),
                                expire_ms: None,
                            },
                        );
                        n
                    }
                };

                self.notify_blpop_waiters(&key);

                TryExecuteResult::Done(Resp::Integer(n as i64))
            }

            Command::Lpop { key, count } => {
                if let Some(Value {
                    value: PrimitiveValue::List(list),
                    expire_ms: _,
                }) = self.data.get_mut(&key)
                {
                    if list.is_empty() {
                        return TryExecuteResult::Done(Resp::NullBulkString);
                    }

                    match count {
                        None => {
                            let e = list.pop_front().unwrap();
                            TryExecuteResult::Done(Resp::BulkString(e))
                        }
                        Some(c) => {
                            let mut els = Vec::new();
                            for _ in 0..c {
                                match list.pop_front() {
                                    Some(e) => els.push(Resp::BulkString(e)),
                                    None => return TryExecuteResult::Done(Resp::Array(els)),
                                }
                            }
                            TryExecuteResult::Done(Resp::Array(els))
                        }
                    }
                } else {
                    TryExecuteResult::Done(Resp::NullBulkString)
                }
            }

            Command::Lrange { key, start, end } => {
                if let Some(Value {
                    value: PrimitiveValue::List(list),
                    expire_ms: _,
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
                        return TryExecuteResult::Done(Resp::Array(vec![]));
                    }

                    let mut els = Vec::new();
                    for i in (a as usize)..=(b as usize) {
                        els.push(Resp::BulkString(list[i].to_vec()));
                    }
                    TryExecuteResult::Done(Resp::Array(els))
                } else {
                    TryExecuteResult::Done(Resp::Array(vec![]))
                }
            }

            Command::Llen { key } => {
                let n = self.map_list(&key, |list| list.len()).unwrap_or(0);
                TryExecuteResult::Done(Resp::Integer(n as i64))
            }

            Command::Blpop { keys, timeout: _ } => {
                let (Resp, is_empty) = self.fetch_blpop(&keys);

                if !is_empty {
                    return TryExecuteResult::Done(Resp);
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
                info.push_str(&format!("\nmaster_repl_offset:{}", self.master_ack).to_string());
                TryExecuteResult::Done(Resp::BulkString(info.as_bytes().to_vec()))
            }

            Command::Ping { message } => {
                let result = match message {
                    Some(m) => Resp::BulkString(m),
                    None => Resp::SimpleString("PONG".as_bytes().to_vec()),
                };
                TryExecuteResult::Done(result)
            }

            Command::Wait {
                numreplicas,
                timeout,
            } => {
                let current_replicas = self.count_acked(self.master_ack);
                if current_replicas >= numreplicas {
                    return TryExecuteResult::Done(Resp::Integer(current_replicas as i64));
                }

                self.waiter_id += 1;
                TryExecuteResult::WaitCommand(self.waiter_id, numreplicas, timeout)
            }

            Command::ConfigGet { parameter } => self.command_config_get(&parameter),

            Command::Keys { pattern } => self.command_keys(&pattern),

            Command::Subscribe { channels } => self.command_subscribe(client_id, &channels),

            Command::ReplconfAck { ack_bytes } => self.command_replconf_ack(client_id, ack_bytes),

            Command::Publish { channel, message } => {
                self.command_publish(client_id, &channel, &message)
            }

            Command::Unsubscribe { channels } => self.command_unsubscribe(client_id, &channels),

            Command::Zadd { key, score, member } => {
                self.command_zadd(client_id, &key, score, &member)
            }

            Command::Zrank { key, member } => self.command_zrank(&key, &member),

            Command::Zrange { key, start, stop } => self.command_zrange(&key, start, stop),

            Command::Zcard { key } => self.command_zcard(&key),

            Command::Zscore { key, member } => self.command_zscore(&key, &member),

            Command::Zrem { key, member } => self.command_zrem(&key, &member),

            Command::Geoadd {
                key,
                longitude,
                latitude,
                member,
            } => self.command_geoadd(&key, longitude, latitude, &member),

            Command::Geopos { key, members } => self.command_geopos(&key, &members),

            Command::Geodist {
                key,
                member1,
                member2,
            } => self.command_geodist(&key, &member1, &member2),

            Command::Geosearch {
                key,
                longitude,
                latitude,
                radius,
                unit,
            } => self.command_geosearch(&key, longitude, latitude, radius, &unit),

            Command::AclWhoami => self.command_acl_whoami(),

            Command::AclGetuser { username } => self.command_acl_getuser(&username),

            Command::AclSetuser { username, password } => {
                self.command_acl_setuser(&username, &password)
            }

            Command::Auth { username, password } => self.command_auth(&username, &password),

            _ => TryExecuteResult::Done(Resp::NullBulkString),
        }
    }
}

// Messages that Server (store) sends to connected Clients (either replicas, or just subscribed)
#[derive(Debug, Clone)]
enum StorePush {
    Replicate(Command),
    Message(Resp),
}

#[derive(Debug)]
enum Envelope {
    WithReply {
        client_id: usize,
        command: Command,
        reply_channel: oneshot::Sender<Resp>,
    },
    TimeoutXread {
        waiter_id: WaiterId,
    },
    TimeoutBlpop {
        waiter_id: WaiterId,
    },
    AddReplica {
        client_id: usize,
        // Need to reply back: replication_id (fixed for now) and RDB, this is just for rDB for now
        reply_channel: oneshot::Sender<Resp>,
    },
    Replicate {
        command: Command,
    },
    WaitCommandTimeout {
        waiter_id: WaiterId,
    },
    RegisterClient {
        client_id: ClientId,
        tx: mpsc::Sender<StorePush>,
        reply_channel: oneshot::Sender<bool>,
    },
    UnregisterClient {
        client_id: ClientId,
    },
}

// This layer handles timeouts
async fn run_store(
    mut aof: Aof,
    mut store: Store,
    mut rx: mpsc::Receiver<Envelope>,
    tx: mpsc::Sender<Envelope>,
) {
    // On startup - no replication yet
    for command in aof.get_initial_commands().await {
        store.try_execute(0, command);
    }

    while let Some(e) = rx.recv().await {
        match e {
            Envelope::WithReply {
                client_id,
                command,
                reply_channel,
            } => {
                let timeout = command.block_timeout();
                let replication_command = if command.is_replicatable() {
                    store.pending_write_commands_for_wait = true;
                    store.master_ack += command.to_resp().unwrap().len() as u64;
                    Some(command.clone())
                } else {
                    None
                };

                if let Some(resp) = command.to_resp() {
                    aof.append(resp).await;
                }

                match store.try_execute(client_id, command) {
                    TryExecuteResult::Done(reply) => {
                        // Answer to client connected to master
                        let _ = reply_channel.send(reply);
                        // Update all connected replicas
                        if let Some(replication_command) = replication_command {
                            for (client_id, (is_replica, tx)) in &store.clients {
                                if *is_replica {
                                    println!(
                                        "[client_id = {}] Replicating command: {:?}",
                                        client_id, replication_command
                                    );
                                    let _ = tx
                                        .send(StorePush::Replicate(replication_command.clone()))
                                        .await;
                                }
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
                    TryExecuteResult::WaitCommand(waiter_id, numreplicas, timeout) => {
                        if store.pending_write_commands_for_wait {
                            for (_client_id, (is_replica, replica_tx)) in &store.clients {
                                if *is_replica {
                                    let _ = replica_tx
                                        .send(StorePush::Replicate(Command::ReplconfGetAck))
                                        .await;
                                }
                            }
                            store.pending_write_commands_for_wait = false;
                        }
                        store
                            .wait_waiters
                            .insert(waiter_id, (reply_channel, numreplicas, store.master_ack));
                        let duration = Duration::from_millis(timeout);
                        let tx2 = tx.clone();
                        tokio::spawn(async move {
                            sleep(duration).await;
                            let _ = tx2.send(Envelope::WaitCommandTimeout { waiter_id }).await;
                        });
                    }
                    TryExecuteResult::None => {}
                }
            }
            Envelope::TimeoutXread { waiter_id } => {
                // Deregister interest if there's any, and remove interestent
                if let Some((reply_channel, _, _)) = store.stream_xread_waiters.remove(&waiter_id) {
                    let _ = reply_channel.send(Resp::NullArray);
                }
            }
            Envelope::TimeoutBlpop { waiter_id } => {
                // Deregister interest if there's any, and remove interestent
                if let Some((reply_channel, _)) = store.list_blpop_waiters.remove(&waiter_id) {
                    let _ = reply_channel.send(Resp::NullArray);
                }
            }
            Envelope::WaitCommandTimeout { waiter_id } => {
                // On timeout, reply with however many replicas are caught up by now.
                if let Some((reply_channel, _numreplicas, target)) =
                    store.wait_waiters.remove(&waiter_id)
                {
                    let _ = reply_channel.send(Resp::Integer(store.count_acked(target) as i64));
                }
            }
            Envelope::AddReplica {
                client_id,
                reply_channel,
            } => {
                // A newly connected replica is not an acknowledgement of pending writes,
                // so it does not resolve any in-flight WAIT.
                store
                    .clients
                    .entry(client_id)
                    .and_modify(|(is_replica, _)| *is_replica = true);
                let rdb = Resp::file(store.to_rdb().serialize());
                reply_channel.send(rdb);
            }
            // This is command execution on replica
            Envelope::Replicate { command } => {
                let _ = store.try_execute(0, command); // TODO client_id should be Option<usize>
            }
            Envelope::RegisterClient {
                client_id,
                tx,
                reply_channel,
            } => {
                store.clients.insert(client_id, (false, tx));
                let need_auth = if let Some(passwords) = store.users.get("default") {
                    !passwords.is_empty()
                } else {
                    true
                };
                let _ = reply_channel.send(need_auth);
            }
            Envelope::UnregisterClient { client_id } => {
                store.clients.remove(&client_id);
                store.replica_acks.remove(&client_id);
            }
        }
    }
}

async fn handle_client(client_id: usize, mut stream: TcpStream, store_tx: mpsc::Sender<Envelope>) {
    println!("Connected client {}", client_id);
    let mut queue: Option<VecDeque<Command>> = None; //VecDeque::new();
    let mut buffer = [0u8; 1024];

    // id the client in subscribe mode?
    let mut is_subscribe_mode = false;

    // Channel to this client, so master can send commands for replication
    let (tx, mut rx) = mpsc::channel::<StorePush>(1024);

    // Register this client for receiveing messages from server/store
    let (need_auth_tx, need_auth_rx) = oneshot::channel::<bool>();
    let _ = store_tx
        .send(Envelope::RegisterClient {
            client_id,
            tx,
            reply_channel: need_auth_tx,
        })
        .await;
    let mut need_auth = need_auth_rx.await.unwrap();

    // Initial state / mode for client:
    let mut mode = ClientRunMode::Normal;

    loop {
        select! {
            bytes_read = stream.read(&mut buffer) => {
                match bytes_read {
                    Ok(0) => {
                        // Client closed connection
                        break;
                    }
                    Ok(n) => {
                        let (inputs, _) = parse_resp(&buffer[..n]).unwrap();

                        for input in inputs {
                            let command = Command::from_resp(&input).unwrap();
                            let (next_mode, dispatch ) = mode.run(need_auth, command);
                            mode = next_mode;

                            match dispatch {
                                ClientDispatch::Execute(command) => {
                                    let (reply_tx, reply_rx) = oneshot::channel::<Resp>();
                                    let envelope = Envelope::WithReply { client_id, command, reply_channel: reply_tx };
                                    let _ = store_tx.send(envelope).await;
                                    let resp = match reply_rx.await {
                                        Ok(resp) => resp,
                                        Err(e) => panic!("Something wrong with processing command: {:?}", e),
                                    };
                                    let _ = write_resp(&mut stream, &resp).await;
                                },
                                ClientDispatch::ExecuteNoReply(command) => {
                                    let (reply_tx, reply_rx) = oneshot::channel::<Resp>();
                                    let envelope = Envelope::WithReply { client_id, command, reply_channel: reply_tx };
                                    let _ = store_tx.send(envelope).await;
                                },
                                ClientDispatch::Reply(resp) => {
                                    let _ = write_resp(&mut stream, &resp).await;
                                },
                                ClientDispatch::StartReplica(command) => {
                                    let (reply_tx, reply_rx) = oneshot::channel::<Resp>();
                                    let envelope = Envelope::AddReplica { client_id, reply_channel: reply_tx};
                                    let _ = store_tx.send(envelope).await;
                                    let rdb = reply_rx.await.unwrap();
                                    let _ = write_resp(&mut stream, &Resp::SimpleString("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".as_bytes().to_vec())).await;
                                    let _ = write_resp(&mut stream, &rdb).await;
                                },
                                ClientDispatch::MustAuth(username, password) => {
                                    let (reply_tx, reply_rx) = oneshot::channel::<Resp>();
                                    let envelope = Envelope::WithReply { client_id, command: Command::Auth { username, password}, reply_channel: reply_tx };
                                    let _ = store_tx.send(envelope).await;
                                    let resp = match reply_rx.await {
                                        Ok(resp) => resp,
                                        Err(e) => panic!("Something wrong with processing command: {:?}", e),
                                    };
                                    if let Resp::SimpleString(s) = &resp {
                                        need_auth = false;
                                    }
                                    let _ = write_resp(&mut stream, &resp).await;
                                }
                            }
                        }

                        buffer.fill(0u8);
                    }
                    Err(_) => {
                        // TCP read error, ignore
                    }
                }
            },
            store_push = rx.recv() => {
                // Command received from master, encode it and send it to client / replica
                // (this is all happening on master, this is process inside master / server)
                println!("(master process, client connection handler) received store push message: {:?}", store_push);
                match store_push.unwrap() {
                    StorePush::Replicate(command) => {
                        write_command_to_stream(&mut stream, &command).await;
                    },
                    StorePush::Message(resp) => {
                        write_resp(&mut stream, &resp).await;
                    }
                }
            }
        }
    }

    // De-register this client from server/store
    let _ = store_tx
        .send(Envelope::UnregisterClient { client_id })
        .await;

    println!("Client {} disconnected", client_id);
}

async fn execute_command(
    store_ch: &mpsc::Sender<Envelope>,
    client_id: usize,
    command: Command,
) -> Resp {
    // Create command
    let (reply_ch_sender, reply_ch_receiver) = oneshot::channel::<Resp>();
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
        Err(e) => panic!("Something wrong with processing command: {:?}", e),
    };

    reply
}

// function that processes messages replica receovis from master
async fn process_replica_message(
    store_tx: &mpsc::Sender<Envelope>,
    input: Resp,
    ack_bytes: usize,
) -> Option<Resp> {
    println!("Replica processing input: {:?}", input);
    if let Some(command) = Command::from_resp(&input) {
        // println!(
        //     "[process_replica_message] input: {:?}, command: {:?}",
        //     input, command
        // );
        let reply = match command {
            Command::ReplconfGetAck => Some(Resp::Array(vec![
                Resp::BulkString("REPLCONF".as_bytes().to_vec()),
                Resp::BulkString("ACK".as_bytes().to_vec()),
                Resp::BulkString(format!("{}", ack_bytes).as_bytes().to_vec()),
            ])),
            _ => {
                let _ = store_tx
                    .send(Envelope::Replicate {
                        command: command.clone(),
                    })
                    .await;
                None
            }
        };
        reply
    } else {
        None
    }
}

async fn read_resp_from_stream(stream: &mut TcpStream) -> Option<Vec<Resp>> {
    let mut buffer = [0; 1024];
    let n = stream.read(&mut buffer).await.unwrap();
    let read_inputs = if n == 0 {
        // Client disconected
        println!("[read] None");
        None
    } else {
        let (inputs, _) = parse_resp(&buffer[..n]).unwrap();
        // for resp in &inputs {
        //     println!("[read][{}] {:?}", resp.len(), resp);
        // }
        Some(inputs)
    };

    read_inputs
}

async fn write_resp_to_stream(stream: &mut TcpStream, resp: &Resp) -> std::io::Result<()> {
    let result = stream.write_all(&resp.to_bytes()[..]).await;
    let _ = stream.flush();
    result
}

async fn write_resp(stream: &mut TcpStream, resp: &Resp) -> std::io::Result<()> {
    let result = stream.write_all(&resp.to_bytes()[..]).await;
    let _ = stream.flush();
    result
}

async fn write_command_to_stream(stream: &mut TcpStream, command: &Command) -> std::io::Result<()> {
    let resp = command.to_resp().unwrap();
    let result = stream.write_all(&resp.to_bytes()[..]).await;
    let _ = stream.flush();
    result
}

async fn replica_server_handshake(stream: &mut TcpStream, port: u16) -> (bool, VecDeque<Resp>) {
    // Handshake: 1) PING - PONG
    let _ = write_command_to_stream(stream, &Command::Ping { message: None }).await;
    let _ = read_resp_from_stream(stream).await;

    // Handshake: 2) REPLCONF
    let _ = write_command_to_stream(stream, &Command::ReplconfListeningPort { port }).await;
    let _ = read_resp_from_stream(stream).await;

    // Handshake: 3) REPLCONF
    let replconf2 = Command::ReplconfCapa {
        capabilites: vec![b"psync2".to_vec()],
    };
    let _ = write_command_to_stream(stream, &replconf2).await;
    let _ = read_resp_from_stream(stream).await;

    // Handshake: 4) PSYNC
    let psync = Command::Psync {
        replication_id: "?".to_string(),
        offset: -1,
    };
    let _ = write_command_to_stream(stream, &psync).await;

    // FULLRESYNC response tO PSYNC and RDB file, 3rd message can be also in these inputs
    let mut queue = VecDeque::new();
    loop {
        let new_resp = read_resp_from_stream(stream).await.unwrap();
        queue.extend(new_resp);
        if queue.len() >= 2 {
            // This means that FULLRESYNC and RDB file were received as 2 first messages
            break;
        }
        // TODO timeout case
    }

    // Remove first 2 resp elements (FULLRESYNC command as response to PSYNC) and RDB file

    // FULLRESYNC
    queue.pop_front();

    // Rdb file
    queue.pop_front();

    (true, queue)
}

// This is run when server is replica
async fn run_replica_server(addr: String, port: u16, mut store_tx: mpsc::Sender<Envelope>) {
    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Handshake
    let (is_handshake_success, mut inputs_queue) =
        replica_server_handshake(&mut stream, port).await;

    println!("Handshake phase 2 complete, starting listening and metering on this connection");
    println!(
        "Handshake phase 2 finish, success: {:?}",
        is_handshake_success
    );
    println!("Handshake phase 2 finish, input queue: {:?}", inputs_queue);

    // Start counting ACK bytes here:
    let mut ack_bytes = 0;

    // Optional other inputs:
    while let Some(resp) = inputs_queue.pop_front() {
        println!("Post-handshake, first input: {:?}", resp);
        let l = resp.len();
        // Count this command's bytes before replying, so a GETACK reports the
        // offset that includes the GETACK command itself.
        match process_replica_message(&mut store_tx, resp, ack_bytes).await {
            Some(reply) => {
                let _ = write_resp(&mut stream, &reply).await;
            }
            _ => {}
        };
        // Count ACK after command is run
        ack_bytes += l;
    }

    loop {
        let read_inputs = read_resp_from_stream(&mut stream).await;
        match read_inputs {
            None => {
                println!("Master disconnected");
                break;
            }
            Some(inputs) => {
                for input in inputs {
                    let l = input.len();
                    match process_replica_message(&mut store_tx, input, ack_bytes).await {
                        Some(reply) => {
                            println!(
                                "Replica (its process), has response for master: {:?}",
                                reply
                            );
                            let _ = write_resp(&mut stream, &reply).await;
                        }
                        _ => {}
                    };
                    // Count ACK after command is run
                    ack_bytes += l;
                }
            }
        }
    }
}

/// Parse CLI args
#[derive(clap::Parser, Debug)]
struct Args {
    /// Port on which to start
    #[arg(long, default_value_t = 6379)]
    port: u16,

    /// Replica
    #[arg(long)]
    replicaof: Option<String>,

    /// dir
    #[arg(long, default_value_t = env::current_dir().unwrap().to_str().unwrap().to_string())]
    dir: String,

    /// dbfilename, replication
    #[arg(long)]
    dbfilename: Option<String>,

    /// appendonly, append only file (aof)
    #[arg(long, default_value = "no")]
    appendonly: String,

    /// appenddirname, append only file (aof)
    #[arg(long, default_value = "appendonlydir")]
    appenddirname: String,

    /// appendfilename, append only file (aof)
    #[arg(long, default_value = "appendonly.aof")]
    appendfilename: String,

    /// appendfsync, append only file (aof)
    #[arg(long, default_value = "everysec")]
    appendfsync: String,
}

#[derive(Debug)]
struct Config {
    port: u16,
    replicaof: Option<String>,
    dir: String,
    dbfilename: Option<String>,
    appendonly: String,
    appenddirname: String,
    appendfilename: String,
    appendfsync: String,
}

impl Config {
    fn from_args(args: Args) -> Self {
        Config {
            port: args.port,
            replicaof: args.replicaof,
            dir: args.dir,
            dbfilename: args.dbfilename,
            appendonly: args.appendonly,
            appenddirname: args.appenddirname,
            appendfilename: args.appendfilename,
            appendfsync: args.appendfsync,
        }
    }
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let client_counter = AtomicUsize::new(1);

    // CLI Args
    let args = <Args as clap::Parser>::parse();
    let config = Config::from_args(args);
    let port = config.port;
    let master_addr = config.replicaof.as_ref().map(|v| v.replace(" ", ":"));
    let is_replica = master_addr.is_some();

    // Aof setup
    let aof = Aof::from_config(&config).await;

    // Store setup
    let (tx, rx) = mpsc::channel::<Envelope>(1024);
    let store = Store::new(is_replica, config).await;
    tokio::spawn(run_store(aof, store, rx, tx.clone()));

    if let Some(addr) = master_addr {
        tokio::spawn(run_replica_server(addr, port, tx.clone()));
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
