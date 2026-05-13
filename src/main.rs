#![allow(unused_imports)]
use core::{num, str};
use futures::future::select_all;
use std::net::TcpStream;
use std::ops::Bound::{Excluded, Included, Unbounded};
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
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{Notify, RwLock},
    time::timeout,
};

type ByteString = Vec<u8>;

/// RESP - REdis Serialization Protocol
#[derive(Debug)]
enum Resp {
    Null,
    NullArray,
    SimpleString(Vec<u8>),
    SimpleError(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Resp>),
}

/**
 * Store
 */
struct StoreValue {
    t: Instant,
    ttl: Option<Duration>,
    value: Vec<u8>,
}

type Store = HashMap<Vec<u8>, StoreValue>;
type RedisList = VecDeque<ByteString>;
struct RedisListStore {
    lists: HashMap<ByteString, RedisList>,
    waiters: HashMap<ByteString, Arc<Notify>>,
}

fn waiter_for(store: &mut RedisListStore, key: &[u8]) -> Arc<Notify> {
    store
        .waiters
        .entry(key.to_vec())
        .or_insert_with(|| Arc::new(Notify::new()))
        .clone()
}

#[derive(Debug, Clone)]
struct ParseError {
    message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

///
/// Parser input type is slice of bytes:
type ParserInput<'a> = &'a [u8];
///
type ParseResult<'a, T> = Result<(T, ParserInput<'a>), ParseError>;
///
trait Parser<'a, T> {
    fn parse(&self, input: ParserInput<'a>) -> ParseResult<'a, T>;
}

impl<'a, T, F> Parser<'a, T> for F
where
    F: Fn(ParserInput<'a>) -> ParseResult<'a, T>,
{
    fn parse(&self, input: ParserInput<'a>) -> ParseResult<'a, T> {
        self(input)
    }
}
///
/// Read `b` byte by value.
fn byte<'a>(b: u8) -> impl Parser<'a, u8> {
    move |input: ParserInput<'a>| {
        if input.len() > 0 && input[0] == b {
            Ok((b, &input[1..]))
        } else {
            Err(ParseError {
                message: format!("[byte] no byte: {:?} found", b),
            })
        }
    }
}

///
/// Read `tag` bytes by value.
fn tag<'a>(expected: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        if input.starts_with(expected) {
            Ok(input.split_at(expected.len()))
        } else {
            Err(ParseError {
                message: format!("[tag] no tag: {:?} found", expected),
            })
        }
    }
}

fn tag_no_case<'a>(expected: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let n = expected.len();
        if input.len() >= n && input[..n].eq_ignore_ascii_case(expected) {
            Ok(input.split_at(n))
        } else {
            Err(ParseError {
                message: format!("[tag_no_case] no tag: {:?} found", expected),
            })
        }
    }
}

/// Read `n` bytes.
fn take<'a>(n: usize) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        if input.len() < n {
            return Err(ParseError {
                message: format!("expected {} bytes, got {}", n, input.len()),
            });
        }
        let (head, rest) = input.split_at(n);
        Ok((head, rest))
    }
}

/// Read all bytes while predicate `pred` returns true.
fn take_while<'a>(pred: impl Fn(u8) -> bool) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let n = input.iter().take_while(|&&b| pred(b)).count();
        if n == 0 {
            return Err(ParseError {
                message: format!(
                    "expected to match at least one byte, but matched 0; input = {:?}",
                    input
                ),
            });
        }
        let (head, rest) = input.split_at(n);
        Ok((head, rest))
    }
}

/// Read all bytes until `delimiter` bytes are next. It does not read any of `delimiter` bytes.
fn take_until<'a>(delimiter: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let mut i = 0;
        while i < input.len() - delimiter.len() {
            if input[i..].starts_with(delimiter) {
                return Ok(input.split_at(i));
            }
            i += 1;
        }

        Err(ParseError {
            message: format!("expected to match limit: {:?}, but haven't", delimiter),
        })
    }
}

/// `or` combinator, it succeeds if `p1` or `p2` succeeds.
fn or<'a, T: Debug>(p1: impl Parser<'a, T>, p2: impl Parser<'a, T>) -> impl Parser<'a, T> {
    move |input: ParserInput<'a>| match p1.parse(input) {
        Ok(result) => Ok(result),
        _ => p2.parse(input),
    }
}

/// `and` combinator, it succeeds when `p1` matches and then `p2` matches.
fn and<'a, A, B>(p1: impl Parser<'a, A>, p2: impl Parser<'a, B>) -> impl Parser<'a, (A, B)> {
    move |input: ParserInput<'a>| {
        let (a, rest) = p1.parse(input)?;
        let (b, rest) = p2.parse(rest)?;
        Ok(((a, b), rest))
    }
}

macro_rules! and {
    ($p1: expr, $p2: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            Ok(((a, b), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            let (c, rest) = p3.parse(rest)?;
            Ok(((a, b, c), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        let p4 = $p4;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            let (c, rest) = p3.parse(rest)?;
            let (d, rest) = p4.parse(rest)?;
            Ok(((a, b, c, d), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        let p4 = $p4;
        let p5 = $p5;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            let (c, rest) = p3.parse(rest)?;
            let (d, rest) = p4.parse(rest)?;
            let (e, rest) = p5.parse(rest)?;
            Ok(((a, b, c, d, e), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr, $p6: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        let p4 = $p4;
        let p5 = $p5;
        let p6 = $p6;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            let (c, rest) = p3.parse(rest)?;
            let (d, rest) = p4.parse(rest)?;
            let (e, rest) = p5.parse(rest)?;
            let (f, rest) = p6.parse(rest)?;
            Ok(((a, b, c, d, e, f), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr, $p6: expr, $p7: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        let p4 = $p4;
        let p5 = $p5;
        let p6 = $p6;
        let p7 = $p7;
        move |input| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            let (c, rest) = p3.parse(rest)?;
            let (d, rest) = p4.parse(rest)?;
            let (e, rest) = p5.parse(rest)?;
            let (f, rest) = p6.parse(rest)?;
            let (g, rest) = p7.parse(rest)?;
            Ok(((a, b, c, d, e, f, g), rest))
        }
    }};
}

macro_rules! or {
    ($p1: expr, $p2: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        move |input| match p1.parse(input) {
            Ok(result) => Ok(result),
            _ => p2.parse(input),
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        move |input| match p1.parse(input) {
            Ok(result) => Ok(result),
            _ => match p2.parse(input) {
                Ok(result) => Ok(result),
                _ => p3.parse(input),
            },
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        let p4 = $p4;
        move |input| match p1.parse(input) {
            Ok(result) => Ok(result),
            _ => match p2.parse(input) {
                Ok(result) => Ok(result),
                _ => match p3.parse(input) {
                    Ok(result) => Ok(result),
                    _ => p4.parse(input),
                },
            },
        }
    }};
}

/// `opt` combinator, it always succeeds. If it matches input is advanced.
fn opt<'a, T: Debug>(p: impl Parser<'a, T>) -> impl Parser<'a, Option<T>> {
    move |input: ParserInput<'a>| match p.parse(input) {
        Ok((result, rest)) => Ok((Some(result), rest)),
        _ => Ok((None, input)),
    }
}

fn recognize<'a, T>(p: impl Parser<'a, T>) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let (_, rest) = p.parse(input)?;
        let len = input.len() - rest.len();
        Ok((&input[..len], rest))
    }
}

fn integer<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr,
{
    move |input: ParserInput<'a>| {
        let digits = || recognize(take_while(|b| b.is_ascii_digit()));
        let sign = || recognize(or(byte(b'-'), byte(b'+')));
        let number = recognize(and!(opt(sign()), digits()));
        let (bytes, rest) = number.parse(input)?;
        let string = from_utf8(bytes).unwrap();
        let n = match string.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(ParseError {
                message: format!("[float] cannot parse from string: {}", string),
            }),
        }?;
        Ok((n, rest))
    }
}

fn float<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr,
{
    move |input: ParserInput<'a>| {
        let digits = || recognize(take_while(|b| b.is_ascii_digit()));
        let sign = || recognize(or(byte(b'-'), byte(b'+')));
        let inifinity = || recognize(tag(b"inifinity"));
        let inf = || recognize(tag(b"inf"));
        let nan = || recognize(tag(b"nan"));
        let e = || recognize(or!(byte(b'e'), byte(b'E')));
        let dot = || recognize(byte(b'.'));
        let exp = recognize(and!(e(), opt(sign()), digits()));
        let number_digits = or!(
            recognize(and!(digits(), dot(), opt(digits()))),
            recognize(and!(opt(digits()), dot(), digits())),
            recognize(digits()),
        );
        let number = recognize(and!(number_digits, opt(exp)));
        let f = recognize(and!(opt(sign()), or!(inifinity(), inf(), nan(), number)));
        let (bytes, rest) = f.parse(input)?;
        let string = from_utf8(bytes).unwrap();
        let n = match string.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(ParseError {
                message: format!("[float] cannot parse from string: {}", string),
            }),
        }?;
        Ok((n, rest))
    }
}

// milliseconds-seqeunce id
enum StreamIdInputSpec {
    Explicit(u64, u64),
    AutoGenSeq(u64),
    AugoGen,
}
type StreamKey = (u64, u64);

fn parse_input_stream_id<'a>(id: &'a Vec<u8>) -> Option<StreamIdInputSpec> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((tid, _, sid), _)) => Some(StreamIdInputSpec::Explicit(tid, sid)),
        _ => match and!(integer::<u64>(), byte(b'-'), byte(b'*')).parse(id) {
            Ok(((tid, _, _), _)) => Some(StreamIdInputSpec::AutoGenSeq(tid)),
            _ => match byte(b'*').parse(id) {
                Ok(_) => Some(StreamIdInputSpec::AugoGen),
                _ => None,
            },
        },
    }
}

fn next_stream_id(ski: StreamIdInputSpec, stream: &RedisStream) -> Option<(u64, u64)> {
    let latest = stream.last_key_value();
    match ski {
        StreamIdInputSpec::Explicit(tid, sid) => Some((tid, sid)),
        StreamIdInputSpec::AutoGenSeq(tid) => {
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
        StreamIdInputSpec::AugoGen => {
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

type RedisStream = BTreeMap<StreamKey, Vec<Vec<u8>>>;
struct RedisStreamStore {
    streams: HashMap<Vec<u8>, BTreeMap<StreamKey, Vec<Vec<u8>>>>,
    waiters: HashMap<ByteString, Arc<Notify>>,
}

fn stream_waiter_for(store: &mut RedisStreamStore, key: &[u8]) -> Arc<Notify> {
    store
        .waiters
        .entry(key.to_vec())
        .or_insert_with(|| Arc::new(Notify::new()))
        .clone()
}

/// APP PARSERS
/// Parse client request:
fn parse_simple_string<'a>(input: ParserInput<'a>) -> ParseResult<'a, Resp> {
    let ((_, s, _), rest) = and!(
        byte(b'+'),
        take_until(&[b'\r', b'\n']),
        tag(&[b'\r', b'\n'])
    )
    .parse(input)?;

    Ok((Resp::SimpleString(s.to_vec()), rest))
}

fn parse_bulk_string<'a>(input: ParserInput<'a>) -> ParseResult<'a, Resp> {
    let ((_, l), rest) = and!(byte(b'$'), integer::<usize>()).parse(input)?;
    let ((_, s, _), rest) =
        and!(tag(&[b'\r', b'\n']), take(l), tag(&[b'\r', b'\n'])).parse(rest)?;

    Ok((Resp::BulkString(s.to_vec()), rest))
}

fn parse_array<'a>(input: ParserInput<'a>) -> ParseResult<'a, Resp> {
    let ((_, l, _), rest) =
        and!(byte(b'*'), integer::<usize>(), tag(&[b'\r', b'\n'])).parse(input)?;

    let mut elements: Vec<Resp> = Vec::new();
    let mut new_rest = rest;
    for _ in 0..l {
        let (el, rest) = parse_input_resp(new_rest)?;
        new_rest = rest;
        elements.push(el);
    }

    Ok((Resp::Array(elements), new_rest))
}

fn parse_input_resp<'a>(input: ParserInput<'a>) -> ParseResult<'a, Resp> {
    match input[0] {
        b'$' => parse_bulk_string(input),
        b'*' => parse_array(input),
        _ => Err(ParseError {
            message: format!("unknown RESP first byte: {}", input[0]),
        }),
    }
}

/**
 * Process command
 */
fn process_echo(cmd: &Command) -> Result<Resp, ParseError> {
    if cmd.args.len() == 1 {
        Ok(Resp::BulkString(cmd.args[0].to_vec()))
    } else {
        Err(ParseError {
            message: format!("Unsupported ECHO command shape: {:?}", cmd.args),
        })
    }
}

fn process_ping(cmd: &Command) -> Result<Resp, ParseError> {
    match cmd.args.len() {
        1 => Ok(Resp::BulkString(cmd.args[0].to_vec())),
        0 => Ok(Resp::SimpleString(b"PONG".to_vec())),
        _ => Err(ParseError {
            message: format!("Unsupported PING command shape: {:?}", cmd.args),
        }),
    }
}

async fn process_set(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    match cmd.args.len() {
        2 => {
            let key = &cmd.args[0];
            let value = &cmd.args[1];
            let mut store = store.write().await;
            let value = StoreValue {
                t: Instant::now(),
                ttl: None,
                value: value.to_vec(),
            };
            (*store).insert(key.to_vec(), value);
            Ok(Resp::SimpleString(b"OK".to_vec()))
        }
        4 => {
            let key = &cmd.args[0];
            let value = &cmd.args[1];
            let expx = &cmd.args[2];
            let ttl = &cmd.args[3];
            let n = match integer::<u64>().parse(ttl) {
                Ok((value, _)) => value,
                Err(ParseError { message }) => {
                    return Err(ParseError {
                        message: format!("Invalid time value for SET command: {:?}", message),
                    });
                }
            };
            let ttl = match &expx[..] {
                b"EX" => Duration::from_secs(n),
                b"PX" => Duration::from_millis(n),
                _ => {
                    return Err(ParseError {
                        message: format!(
                            "Invalid time spec for SET (should be PX or EX): {:?}",
                            expx
                        ),
                    });
                }
            };
            let mut store = store.write().await;
            let value = StoreValue {
                t: Instant::now(),
                ttl: Some(ttl),
                value: value.to_vec(),
            };
            (*store).insert(key.to_vec(), value);
            Ok(Resp::SimpleString(b"OK".to_vec()))
        }
        _ => Err(ParseError {
            message: format!("Unsupported SET command shape: {:?}", cmd.args),
        }),
    }
}

async fn process_get(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    if cmd.args.len() != 1 {
        return Err(ParseError {
            message: format!("Unsupported GET command shape: {:?}", cmd.args),
        });
    }
    let key = &cmd.args[0];
    let store = store.read().await;
    match store.get(key) {
        Some(StoreValue { t, ttl, value }) => match ttl {
            None => Ok(Resp::BulkString(value.to_vec())),
            Some(duration) if *t + *duration < Instant::now() => Ok(Resp::Null),
            Some(_) => Ok(Resp::BulkString(value.to_vec())),
        },
        None => Ok(Resp::Null),
    }
}

// Lists
async fn process_list_rpush(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.is_empty() {
        return Err(ParseError {
            message: format!(
                "Unsupported RPUSH command shape, missing list name: {:?}",
                cmd.args
            ),
        });
    }
    let name = &cmd.args[0];

    let mut elements = VecDeque::new();
    for element in cmd.args.iter().skip(1) {
        elements.push_back(element.to_vec());
    }

    let mut store = list_store.write().await;

    store
        .lists
        .entry(name.to_vec())
        .and_modify(|e| e.append(&mut elements))
        .or_insert(elements);

    let notifier = waiter_for(&mut store, name);
    notifier.notify_waiters();

    Ok(Resp::Integer(
        store.lists.get(name).map_or(0, |l| l.len() as i64),
    ))
}

async fn process_list_lpush(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.is_empty() {
        return Err(ParseError {
            message: format!(
                "Unsupported LPUSH command shape, missing list name: {:?}",
                cmd.args
            ),
        });
    }
    let name = &cmd.args[0];

    let mut store = list_store.write().await;
    store
        .lists
        .entry(name.to_vec())
        .and_modify(|e| {
            for element in cmd.args.iter().skip(1) {
                e.push_front(element.to_vec());
            }
        })
        .or_insert_with(|| {
            let mut l = VecDeque::new();
            for element in cmd.args.iter().skip(1) {
                l.push_front(element.to_vec());
            }
            l
        });

    let notifier = waiter_for(&mut store, name);
    notifier.notify_waiters();

    Ok(Resp::Integer(
        store.lists.get(name).map_or(0, |l| l.len() as i64),
    ))
}

async fn process_list_llen(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() != 1 {
        return Err(ParseError {
            message: format!(
                "Unsupported LLEN command shape, missing list name: {:?}",
                cmd.args
            ),
        });
    }
    let name = &cmd.args[0];
    let store = list_store.read().await;
    let l = match store.lists.get(name) {
        Some(l) => l.len(),
        _ => 0,
    };
    Ok(Resp::Integer(l as i64))
}

async fn process_list_lpop(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let (name, count) = match cmd.args.len() {
        1 => Ok((&cmd.args[0], None)),
        2 => match integer::<u32>().parse(&cmd.args[1])? {
            (c, _) => Ok((&cmd.args[0], Some(c))),
        },
        _ => Err(ParseError {
            message: format!(
                "Unsupported LPOP command shape, missing list name: {:?}",
                cmd.args
            ),
        }),
    }?;

    let mut store = list_store.write().await;
    let list = store.lists.get_mut(name);
    if list.is_none() {
        return Ok(Resp::Null);
    }
    let list = list.unwrap();
    if list.is_empty() {
        return Ok(Resp::Null);
    }
    match count {
        None => {
            let el = list.pop_front().unwrap();
            Ok(Resp::BulkString(el))
        }
        Some(count) => {
            let mut result = Vec::new();
            for _ in 0..count {
                match list.pop_front() {
                    Some(el) => result.push(Resp::BulkString(el)),
                    None => return Ok(Resp::Array(result)),
                }
            }
            Ok(Resp::Array(result))
        }
    }
}

async fn process_list_lrange(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() != 3 {
        return Err(ParseError {
            message: format!("Unsupported LRANGE command shape: {:?}", cmd.args),
        });
    }
    let name = &cmd.args[0];
    let (start, _) = integer::<i32>().parse(&cmd.args[1])?;
    let (stop, _) = integer::<i32>().parse(&cmd.args[2])?;
    let (name, start, stop) = (name, start, stop);
    println!("start: {}, stop: {}", start, stop);

    let mut result = Vec::new();
    let store = list_store.read().await;
    let list_option = store.lists.get(name);
    if list_option.is_none() {
        return Ok(Resp::Array(result));
    }
    let list = list_option.unwrap();

    let a = if start < 0 {
        start + list.len() as i32
    } else {
        start
    };
    let a = 0.max(a);

    let b = if stop < 0 {
        stop + list.len() as i32
    } else {
        stop
    };
    let b = (list.len() as i32 - 1).min(b);

    if a > b {
        return Ok(Resp::Array(result));
    }

    for i in (a as usize)..=(b as usize) {
        result.push(Resp::BulkString(list[i].to_vec()));
    }
    Ok(Resp::Array(result))
}

async fn process_list_blpop(
    cmd: &Command,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() < 2 {
        return Err(ParseError {
            message: format!("Unsupported BLPOP command shape: {:?}", cmd.args),
        });
    }
    let t_bytes = cmd.args.back().unwrap();
    let (t, _) = float::<f64>().parse(t_bytes).map_err(|_| ParseError {
        message: format!("Timeout for BLPOP must be double (f64), got: {:?}", t_bytes),
    })?;
    let duration = if t == 0.0 {
        Duration::MAX
    } else {
        Duration::from_micros((t * 1_000_000.) as u64)
    };

    let lists = cmd
        .args
        .iter()
        .take(cmd.args.len() - 1)
        .map(|l| l.to_vec())
        .collect::<Vec<_>>();

    println!(
        "BLPOP: lists: {:?}, t: {:?}, duration: {:?}",
        lists, t, duration
    );

    loop {
        // 1, Get or create notifiers for all target keys, under lock
        let notifiers: Vec<Arc<Notify>> = {
            let mut store = list_store.write().await;
            lists.iter().map(|k| waiter_for(&mut store, k)).collect()
        }; // lock for store dropped

        // 2. Build& arm Notified futures before checking
        let mut futs: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
        for f in &mut futs {
            f.as_mut().enable();
        }

        // 3. Try to pop - under lock, bruefly
        {
            let mut store = list_store.write().await;
            for k in &lists {
                if let Some(list) = store.lists.get_mut(k) {
                    if let Some(head) = list.pop_front() {
                        return Ok(Resp::Array(vec![
                            Resp::BulkString(k.to_vec()),
                            Resp::BulkString(head),
                        ]));
                    }
                }
            }
        } // lock dropped

        // 4. Wait for any notifier with deadline
        let any = futures::future::select_all(futs);
        if t == 0.0 {
            any.await;
        } else {
            match timeout(duration, any).await {
                Ok(_) => continue,
                Err(_) => return Ok(Resp::NullArray),
            }
        }
    }
}

async fn process_type(
    cmd: &Command,
    store: &Arc<RwLock<Store>>,
    list_store: &Arc<RwLock<RedisListStore>>,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() != 1 {
        return Err(ParseError {
            message: format!("Unsupported TYPE command shape: {:?}", cmd.args),
        });
    }
    let key = &cmd.args[0];
    if store.read().await.contains_key(key) {
        return Ok(Resp::SimpleString(b"string".to_vec()));
    }
    if list_store.read().await.lists.contains_key(key) {
        return Ok(Resp::SimpleString(b"list".to_vec()));
    }
    if stream_store.read().await.streams.contains_key(key) {
        return Ok(Resp::SimpleString(b"stream".to_vec()));
    }
    Ok(Resp::SimpleString(b"none".to_vec()))
}

async fn process_xadd(
    cmd: &Command,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() < 4 {
        return Err(ParseError {
            message: "Unsupported XADD command shape".to_string(),
        });
    }
    let key = &cmd.args[0];
    let id = &cmd.args[1];
    let ski = match parse_input_stream_id(id) {
        Some(k) => Ok(k),
        _ => Err(ParseError {
            message: "Unsupported XADD <id> key shape".to_string(),
        }),
    }?;
    if (cmd.args.len() - 2) % 2 != 0 {
        return Err(ParseError {
            message: "Unsupported XADD command shape".to_string(),
        });
    }
    let values = cmd
        .args
        .iter()
        .skip(2)
        .map(|v| v.to_vec())
        .collect::<Vec<_>>();

    let mut store = stream_store.write().await;

    // Ensure that there is stream `key`:
    store.streams.entry(key.to_vec()).or_insert(BTreeMap::new());

    let (tid, sid) = match next_stream_id(ski, store.streams.get(key).unwrap()) {
        Some(id) => id,
        _ => {
            return Ok(Resp::SimpleError(
                b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
            ));
        }
    };

    if (tid, sid) < (0, 1) {
        return Ok(Resp::SimpleError(
            b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
        ));
    }

    if store.streams.get(key).unwrap().contains_key(&(tid, sid)) {
        return Ok(Resp::SimpleError(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_vec(),
        ));
    }
    if let Some((latest, _)) = store.streams.get(key).unwrap().last_key_value() {
        if &(tid, sid) < latest {
            return Ok(Resp::SimpleError(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_vec(),
            ));
        }
    }
    store.streams.entry(key.to_vec()).and_modify(|bt| {
        (*bt).insert((tid, sid), values);
    });

    let notifier = stream_waiter_for(&mut store, key);
    notifier.notify_waiters();

    Ok(Resp::BulkString(
        format!("{}-{}", tid, sid).as_bytes().to_vec(),
    ))
}

async fn process_xrange(
    cmd: &Command,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    if cmd.args.len() != 3 {
        return Err(ParseError {
            message: "Unsupported XRANGE command shape".to_string(),
        });
    }
    let key = &cmd.args[0];
    let start = &cmd.args[1];
    let end = &cmd.args[2];
    let (start_tid, start_sid) = if start.len() == 1 && start[0] == b'-' {
        (0, 1)
    } else {
        let ((start_tid, _, start_sid), _) =
            and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(start)?;
        (start_tid, start_sid)
    };
    let (end_tid, end_sid) = if end.len() == 1 && end[0] == b'+' {
        (u64::MAX, u64::MAX)
    } else {
        let ((end_tid, _, end_sid), _) =
            and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(end)?;
        (end_tid, end_sid)
    };
    let (key, start, end) = (key, (start_tid, start_sid), (end_tid, end_sid));
    let stream_store = stream_store.read().await;
    let stream = match stream_store.streams.get(key) {
        Some(stream) => Ok(stream),
        _ => Err(ParseError {
            message: format!("Stream not found, XRANGE: {:?}", key),
        }),
    }?;
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
    Ok(Resp::Array(data))
}

fn cmp_bytes_no_case(a: &Vec<u8>, b: &[u8]) -> bool {
    a.to_ascii_uppercase() != b.to_ascii_uppercase()
}

fn cmp_resp_bytes_no_case(a: &Resp, b: &[u8]) -> bool {
    match a {
        Resp::BulkString(sv) => sv.to_ascii_uppercase() != b.to_ascii_uppercase(),
        _ => false,
    }
}

async fn process_xread_fetch_data(
    stream_store: &Arc<RwLock<RedisStreamStore>>,
    keys: &[&Vec<u8>],
    ids: &Vec<(u64, u64)>,
) -> (Resp, bool) {
    let stream_store = stream_store.read().await;

    let mut data: Vec<Resp> = Vec::new();
    let mut is_empty = true;

    for (i, &key) in keys.iter().enumerate() {
        let mut stream_data: Vec<Resp> = Vec::new();

        let stream = match stream_store.streams.get(key) {
            Some(stream) => stream,
            _ => continue,
        };

        stream_data.push(Resp::BulkString(key.to_vec()));

        let mut stream_row_data: Vec<Resp> = Vec::new();

        for (&k, v) in stream.range((Excluded(&ids[i]), Unbounded)) {
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

        stream_data.push(Resp::Array(stream_row_data));

        data.push(Resp::Array(stream_data));
    }

    return (Resp::Array(data), is_empty);
}

async fn process_xread(
    cmd: &Command,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    let mut argc = 0_usize;
    let args2 = cmd.args.iter().collect::<Vec<_>>();
    // if args.len() < 3 {
    //     return Err(ParseError {
    //         message: "Unsupported XREAD command shape".to_string(),
    //     });
    // }
    // let block = cmp_resp_bytes_no_case(&args[0], b"BLOCK");
    let block = args2[argc].to_ascii_uppercase() == b"BLOCK";
    let duration = if block {
        argc += 1; // BLOCK
        let (ms, _) = integer::<u64>().parse(&args2[argc])?;
        argc += 1; // <millisecs>
        if ms == 0 {
            Duration::from_millis(u64::MAX)
        } else {
            Duration::from_millis(ms)
        }
    } else {
        Duration::from_hours(1)
    };

    if args2[argc].to_ascii_uppercase() != b"STREAMS" {
        return Err(ParseError {
            message: "Unsupported XREAD command shape".to_string(),
        });
    }
    argc += 1; // STREAMS

    // keys
    let l = args2[argc..].len();
    if l % 2 != 0 {
        return Err(ParseError {
            message: "Unsupported XREAD command shape".to_string(),
        });
    }
    let keys = &args2[argc..(argc + (l / 2))];
    // ids
    let id_slice = &args2[(argc + (l / 2))..];
    let mut ids: Vec<(u64, u64)> = Vec::new();
    for (i, id) in id_slice.iter().enumerate() {
        if id.len() == 1 && id[0] == b'$' {
            let store = stream_store.read().await;
            let last = store
                .streams
                .get(keys[i])
                .and_then(|s| s.last_key_value())
                .map(|(&k, _)| k)
                .unwrap_or((0, 1));
            ids.push(last);
            continue;
        }
        match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(&id) {
            Ok(((tid, _, sid), _)) => ids.push((tid, sid)),
            _ => {
                return Err(ParseError {
                    message: "Unsupported XREAD command shape, bad id".to_string(),
                });
            }
        }
    }

    assert!(keys.len() == ids.len());

    if block {
        loop {
            // 1, Get or create notifiers for all target keys, under lock
            let notifiers: Vec<Arc<Notify>> = {
                let mut store = stream_store.write().await;
                keys.iter()
                    .map(|k| stream_waiter_for(&mut store, k))
                    .collect()
            }; // lock for store dropped

            // 2. Build& arm Notified futures before checking
            let mut futs: Vec<_> = notifiers.iter().map(|n| Box::pin(n.notified())).collect();
            for f in &mut futs {
                f.as_mut().enable();
            }

            // 3. Try to pop - under lock, bruefly
            {
                if let (data, is_empty) = process_xread_fetch_data(stream_store, keys, &ids).await {
                    if !is_empty {
                        return Ok(data);
                    }
                }
            } // lock dropped

            // 4. Wait for any notifier with deadline
            let any = futures::future::select_all(futs);
            match timeout(duration, any).await {
                Ok(_) => continue,
                Err(_) => return Ok(Resp::NullArray),
            }
        }
    } else {
        let (data, _) = process_xread_fetch_data(stream_store, keys, &ids).await;
        Ok(data)
    }
}

async fn process_incr(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    if cmd.args.len() != 1 {
        return Err(ParseError {
            message: "Unsupported INCR command shape".to_string(),
        });
    }
    let var_name = &cmd.args[0];
    let mut store = store.write().await;
    let (new_value, rsp_num) = if let Some(value) = store.get(var_name) {
        let number = match integer::<i64>().parse(&value.value) {
            Ok((n, _)) => Ok(n),
            _ => {
                return Ok(Resp::SimpleError(
                    b"ERR value is not an integer or out of range".to_vec(),
                ));
            }
        }?;
        (
            StoreValue {
                t: Instant::now(),
                ttl: value.ttl,
                value: (number + 1).to_string().as_bytes().to_vec(),
            },
            number + 1,
        )
    } else {
        (
            StoreValue {
                t: Instant::now(),
                ttl: None,
                value: 1.to_string().as_bytes().to_vec(),
            },
            1,
        )
    };

    store.insert(var_name.to_vec(), new_value);

    Ok(Resp::Integer(rsp_num))
}

async fn process_multi(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    Ok(Resp::SimpleString(b"OK".to_vec()))
}

async fn process_exec(cmd: &Command, store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    Ok(Resp::SimpleError(b"ERR EXEC without MULTI".to_vec()))
}

async fn process_command(
    command: Command,
    store: &Arc<RwLock<Store>>,
    list_store: &Arc<RwLock<RedisListStore>>,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    match command.name {
        CommandName::ECHO => process_echo(&command),
        CommandName::PING => process_ping(&command),
        CommandName::SET => process_set(&command, store).await,
        CommandName::GET => process_get(&command, store).await,
        // Lists
        CommandName::RPUSH => process_list_rpush(&command, list_store).await,
        CommandName::LRANGE => process_list_lrange(&command, list_store).await,
        CommandName::LPUSH => process_list_lpush(&command, list_store).await,
        CommandName::LLEN => process_list_llen(&command, list_store).await,
        CommandName::LPOP => process_list_lpop(&command, list_store).await,
        CommandName::BLPOP => process_list_blpop(&command, list_store).await,
        // Streams
        CommandName::TYPE => process_type(&command, store, list_store, stream_store).await,
        CommandName::XADD => process_xadd(&command, stream_store).await,
        CommandName::XRANGE => process_xrange(&command, stream_store).await,
        CommandName::XREAD => process_xread(&command, stream_store).await,
        // Transactions are handled per connection
        CommandName::INCR => process_incr(&command, store).await,
        _ => Err(ParseError {
            message: format!("Unsupported command: {:?}", command),
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

fn encode_resp(r: &Resp) -> Vec<u8> {
    let mut out = Vec::new();

    match r {
        Resp::Null => {
            write_bytes(&mut out, &[b'$', b'-', b'1', b'\r', b'\n']);
        }
        Resp::NullArray => {
            write_bytes(&mut out, &[b'*', b'-', b'1', b'\r', b'\n']);
        }
        Resp::SimpleString(value) => {
            write_bytes(&mut out, &[b'+']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Resp::SimpleError(value) => {
            write_bytes(&mut out, &[b'-']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Resp::BulkString(value) => {
            write_bytes(&mut out, &[b'$']);
            write_usize(&mut out, value.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Resp::Integer(n) => {
            write_bytes(&mut out, &[b':']);
            write_bytes(&mut out, n.to_string().as_bytes());
            write_bytes(&mut out, &[b'\r', b'\n']);
        }
        Resp::Array(elements) => {
            write_bytes(&mut out, &[b'*']);
            write_usize(&mut out, elements.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            for e in elements {
                //encode_resp(e, out);
                out.append(&mut encode_resp(e));
            }
        }
    }

    out
}

#[derive(Debug, PartialEq)]
enum CommandName {
    ECHO,
    PING,
    SET,
    GET,
    // Lists
    RPUSH,
    LRANGE,
    LPUSH,
    LLEN,
    LPOP,
    BLPOP,
    // Streams
    TYPE,
    XADD,
    XRANGE,
    XREAD,
    // Transactions
    INCR,
    MULTI,
    EXEC,
    DISCARD,
}

fn get_command_name(c: &[u8]) -> Option<CommandName> {
    match c {
        b"ECHO" => Some(CommandName::ECHO),
        b"PING" => Some(CommandName::PING),
        b"SET" => Some(CommandName::SET),
        b"GET" => Some(CommandName::GET),
        // Lists
        b"RPUSH" => Some(CommandName::RPUSH),
        b"LRANGE" => Some(CommandName::LRANGE),
        b"LPUSH" => Some(CommandName::LPUSH),
        b"LLEN" => Some(CommandName::LLEN),
        b"LPOP" => Some(CommandName::LPOP),
        b"BLPOP" => Some(CommandName::BLPOP),
        // Streams
        b"TYPE" => Some(CommandName::TYPE),
        b"XADD" => Some(CommandName::XADD),
        b"XRANGE" => Some(CommandName::XRANGE),
        b"XREAD" => Some(CommandName::XREAD),
        // Transactions
        b"INCR" => Some(CommandName::INCR),
        b"MULTI" => Some(CommandName::MULTI),
        b"EXEC" => Some(CommandName::EXEC),
        b"DISCARD" => Some(CommandName::DISCARD),
        _ => None,
    }
}

#[derive(Debug)]
struct Command {
    name: CommandName,
    args: VecDeque<Vec<u8>>,
}

fn create_command(mut elements: VecDeque<Vec<u8>>) -> Option<Command> {
    if elements.is_empty() {
        return None;
    }
    let name = elements.pop_front().unwrap();
    match get_command_name(&name) {
        Some(name) => Some(Command {
            name,
            args: elements,
        }),
        _ => None,
    }
}

// async fn send(stream: TcpStream, out: Vec<u8>) {
//     println!("Response: {:?}", resp);
//     let mut out = Vec::new();
//     encode_resp(&resp, &mut out);
//     let _ = stream.write_all(&out[..]).await;
// }

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let store = Arc::new(RwLock::new(HashMap::new()));
    let list_store = Arc::new(RwLock::new(RedisListStore {
        lists: HashMap::new(),
        waiters: HashMap::new(),
    }));
    let stream_store = Arc::new(RwLock::new(RedisStreamStore {
        streams: HashMap::new(),
        waiters: HashMap::new(),
    }));
    // Tx lock
    let tx_lock = Arc::new(RwLock::new(false));

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let store = Arc::clone(&store); //store.clone();
        let list_store = Arc::clone(&list_store);
        let stream_store = Arc::clone(&stream_store);
        let tx_lock = Arc::clone(&tx_lock);

        tokio::spawn(async move {
            println!("accepted new connection");
            let mut tx_queue: Option<Vec<Command>> = None;
            let mut buffer = [0u8; 1024];

            while let Ok(n) = stream.read(&mut buffer).await {
                if n == 0 {
                    break;
                }

                let elements = match parse_input_resp(&buffer) {
                    Ok((Resp::Array(els), _)) => els,
                    Ok(_) => {
                        println!("Unexpected input, expecting array");
                        continue;
                    }
                    Err(e) => {
                        println!("Cannot parse input: {:?}", e);
                        continue;
                    }
                }
                .iter()
                .flat_map(|r| {
                    if let Resp::BulkString(s) = r {
                        Some(s.to_vec())
                    } else {
                        None
                    }
                })
                .collect::<VecDeque<_>>();

                let command = match create_command(elements) {
                    Some(c) => c,
                    _ => {
                        println!("Unsupported command");
                        continue;
                    }
                };

                if command.name == CommandName::MULTI {
                    tx_queue = Some(Vec::new());
                    let out = encode_resp(&Resp::SimpleString(b"OK".to_vec()));
                    let _ = stream.write_all(&out[..]).await;
                } else if command.name == CommandName::EXEC && tx_queue.is_some() {
                    let lock = tx_lock.write().await;
                    let mut results = Vec::new();
                    for cmd in tx_queue.take().unwrap() {
                        let resp = process_command(cmd, &store, &list_store, &stream_store)
                            .await
                            .unwrap_or_else(|e| Resp::SimpleError(e.message.into_bytes()));
                        results.push(resp);
                    }
                    drop(lock);
                    //tx_queue = None;
                    let out = encode_resp(&Resp::Array(results));
                    let _ = stream.write_all(&out[..]).await;
                } else if command.name == CommandName::EXEC && tx_queue.is_none() {
                    let out = encode_resp(&Resp::SimpleError(b"ERR EXEC without MULTI".to_vec()));
                    let _ = stream.write_all(&out[..]).await;
                } else if command.name == CommandName::DISCARD && tx_queue.is_none() {
                    let out =
                        encode_resp(&Resp::SimpleError(b"ERR DISCARD without MULTI".to_vec()));
                    let _ = stream.write_all(&out[..]).await;
                } else if command.name == CommandName::DISCARD && tx_queue.is_some() {
                    tx_queue.take();
                    let out = encode_resp(&Resp::SimpleString(b"OK".to_vec()));
                    let _ = stream.write_all(&out[..]).await;
                } else if let Some(ref mut q) = tx_queue {
                    q.push(command);
                    let out = encode_resp(&Resp::SimpleString(b"QUEUED".to_vec()));
                    let _ = stream.write_all(&out[..]).await;
                } else {
                    match process_command(command, &store, &list_store, &stream_store).await {
                        Ok(resp) => {
                            let out = encode_resp(&resp);
                            let _ = stream.write_all(&out[..]).await;
                        }
                        Err(error) => println!("Processing error: {:?}", error),
                    }
                }

                // match parse_input_resp(&buffer) {
                //     Ok((command, _)) => {
                //         let command2 = get_command_name(&command);

                //         match command2 {
                //             Some(CommandName::MULTI) => {}
                //             Some(CommandName::EXEC) => {}
                //             Some(CommandName::DISCARD) => {}
                //             _ => {
                //                 match process_command(command, &store, &list_store, &stream_store)
                //                     .await
                //                 {
                //                     Ok(resp) => {
                //                         println!("Response: {:?}", resp);
                //                         let mut out = Vec::new();
                //                         encode_resp(&resp, &mut out);
                //                         let _ = stream.write_all(&out[..]).await;
                //                     }
                //                     Err(error) => println!("Processing error: {:?}", error),
                //                 }
                //             }
                //         }
                //     }
                //     Err(error) => println!("Parse error: {:?}", error),
                // }

                let _ = stream.flush().await;
                buffer.fill(0u8);
            }
        });
    }
}
