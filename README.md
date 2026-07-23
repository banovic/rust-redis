# rust-redis

A Redis server written in Rust while following [Codecrafters Redis challenge](https://app.codecrafters.io/courses/redis/overview). Implemented base stages and all extensions.

The goal isn't to replace Redis - and having completed the challenge, I can confirm: not even close - the goal was to complete a non-trivial project in Rust and gain insight into the inner workings of Redis (RESP protocol, handling concurrent clients, replication, persistence, and blocking commands).


## Features
- Core: RESP protocol, concurrent clients, PING, ECHO, SET/GET with expiry, TYPE, KEYS
- Replication: master/replica setup with REPLCONF/PSYNC handshake, command propagation, and WAIT
- RDB persistence: reads RDB files on startup, restores keys and expiry, sends contents to replicas
- Streams: queries with + and -, XADD, XRANGE, XREAD, including blocking reads
- Transactions: including optimistic locking, INCR, MULTI, EXEC, DISCARD, command queueing
- Pub/Sub: PING, SUBSCRIBE, UNSUBSCRIBE, PUBLISH
- Lists: RPUSH, LPUSH, LRANGE, LLEN, LPOP, blocking BLPOP
- Sorted sets: ZADD, ZRANGE, ZRANK, ZSCORE, ZCARD, ZREM
- Geospatial: implemented part that builds mostly on sorted sets, GEOADD, GEOPOS, GEOSEARCH
- Authentication: ACL WHOAMI/GETUSER/SETUSER, AUTH command

## Running it

```sh
cargo run
```

Then you can connect and communicate using real redis-cli:

```sh
$ redis-cli
127.0.0.1:6379> PING
PONG
127.0.0.1:6379> SET foo bar
OK
127.0.0.1:6379> GET foo
"bar"
```

## What I learned
- basics of async Rust using tokio
- creating parser combinators and using them to parse RESP
- serializing and deserializing binary files (RDB file format)
- using tokio channels and message passing to create event loops
- how blocking commands interact with event loop
- replication handshake and how replication works in client/server

## Notes
Learning project, not production ready, and shouldn't be used for non-educational purposes.
