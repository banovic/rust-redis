#![allow(unused_imports)]
use core::str;
use futures::future::select_all;
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    error,
    fmt::{self, Debug, format},
    hash::Hash,
    ops::{AddAssign, Mul, MulAssign, Neg},
    os::unix::process,
    result,
    str::{FromStr, from_utf8},
    sync::Arc,
    thread,
    time::{Duration, Instant, SystemTime},
    usize,
};
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

// milliseconds-seqeunce id
type StreamKey = (u64, u64);
type RedisStream = BTreeMap<Vec<u8>, Vec<u8>>;
struct RedisStreamStore {
    streams: HashMap<Vec<u8>, BTreeMap<StreamKey, Vec<Vec<u8>>>>,
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
        move |input: ParserInput<'a>| {
            let (a, rest) = p1.parse(input)?;
            let (b, rest) = p2.parse(rest)?;
            Ok(((a, b), rest))
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        move |input: ParserInput<'a>| {
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
        move |input: ParserInput<'a>| {
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
        move |input: ParserInput<'a>| {
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
        move |input: ParserInput<'a>| {
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
        move |input: ParserInput<'a>| {
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
        move |input: ParserInput<'a>| match p1.parse(input) {
            Ok(result) => Ok(result),
            _ => p2.parse(input),
        }
    }};

    ($p1: expr, $p2: expr, $p3: expr $(,)?) => {{
        let p1 = $p1;
        let p2 = $p2;
        let p3 = $p3;
        move |input: ParserInput<'a>| match p1.parse(input) {
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
        move |input: ParserInput<'a>| match p1.parse(input) {
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
        let (el, rest) = parse_resp(new_rest)?;
        new_rest = rest;
        elements.push(el);
    }

    Ok((Resp::Array(elements), new_rest))
}

fn parse_resp<'a>(input: ParserInput<'a>) -> ParseResult<'a, Resp> {
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
fn process_echo(args: &[Resp]) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(value)] => Ok(Resp::BulkString(value.to_vec())),
        _ => Err(ParseError {
            message: format!("Unsupported ECHO command shape: {:?}", args),
        }),
    }
}

fn process_ping(args: &[Resp]) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(value)] => Ok(Resp::BulkString(value.to_vec())),
        [] => Ok(Resp::SimpleString(b"PONG".to_vec())),
        _ => Err(ParseError {
            message: format!("Unsupported PING command shape: {:?}", args),
        }),
    }
}

async fn process_set(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(key), Resp::BulkString(value)] => {
            let mut store = store.write().await;
            let value = StoreValue {
                t: Instant::now(),
                ttl: None,
                value: value.to_vec(),
            };
            (*store).insert(key.to_vec(), value);
            Ok(Resp::SimpleString(b"OK".to_vec()))
        }
        [
            Resp::BulkString(key),
            Resp::BulkString(value),
            Resp::BulkString(expx),
            Resp::BulkString(ttl),
        ] => {
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
            message: format!("Unsupported SET command shape: {:?}", args),
        }),
    }
}

async fn process_get(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(key)] => {
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
        _ => Err(ParseError {
            message: format!("Unsupported GET command shape: {:?}", args),
        }),
    }
}

// Lists
async fn process_list_rpush(
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let name = match &args[0] {
        Resp::BulkString(name) => Ok(name),
        _ => Err(ParseError {
            message: format!(
                "Unsupported RPUSH command shape, missing list name: {:?}",
                args
            ),
        }),
    }?;

    let mut elements = VecDeque::new();

    for el in &args[1..] {
        if let Resp::BulkString(element) = el {
            elements.push_back(element.to_vec());
        }
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
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let name = match &args[0] {
        Resp::BulkString(name) => Ok(name),
        _ => Err(ParseError {
            message: format!(
                "Unsupported LPUSH command shape, missing list name: {:?}",
                args
            ),
        }),
    }?;

    let mut store = list_store.write().await;
    store
        .lists
        .entry(name.to_vec())
        .and_modify(|e| {
            for el in &args[1..] {
                if let Resp::BulkString(element) = el {
                    (*e).push_front(element.to_vec());
                }
            }
        })
        .or_insert_with(|| {
            let mut l = VecDeque::new();
            for el in &args[1..] {
                if let Resp::BulkString(element) = el {
                    l.push_front(element.to_vec());
                }
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
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let name = match &args[0] {
        Resp::BulkString(name) => Ok(name),
        _ => Err(ParseError {
            message: format!(
                "Unsupported LLEN command shape, missing list name: {:?}",
                args
            ),
        }),
    }?;

    let store = list_store.read().await;
    let l = match store.lists.get(name) {
        Some(l) => l.len(),
        _ => 0,
    };
    Ok(Resp::Integer(l as i64))
}

async fn process_list_lpop(
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let (name, count) = match args {
        [Resp::BulkString(name)] => Ok((name, None)),
        [Resp::BulkString(name), Resp::BulkString(count)] => {
            //let (count, _) = unsigned_integer::<u32>().parse(RespParseContext::from_vec(count))?;
            match integer::<u32>().parse(count)? {
                (c, _) => Ok((name, Some(c))),
                _ => Err(ParseError {
                    message: format!("Invalid count param spec: {:?}", args),
                }),
            }
        }
        _ => Err(ParseError {
            message: format!(
                "Unsupported LLEN command shape, missing list name: {:?}",
                args
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
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let (name, start, stop) = match args {
        [
            Resp::BulkString(name),
            Resp::BulkString(start),
            Resp::BulkString(stop),
        ] => {
            let (start, _) = integer::<i32>().parse(start)?;
            let (stop, _) = integer::<i32>().parse(stop)?;
            Ok((name, start, stop))
        }
        _ => Err(ParseError {
            message: format!("Unsupported LRANGE command shape: {:?}", args),
        }),
    }?;
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
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    if args.len() < 2 {
        return Err(ParseError {
            message: format!("Unsupported BLPOP command shape: {:?}", args),
        });
    }
    let (t, lists) = args.split_last().unwrap();
    let (t, _) = match t {
        Resp::BulkString(n) => float::<f64>().parse(n),
        _ => Err(ParseError {
            message: format!(
                "Timeout for BLPOP must be double (f64), got: {:?}",
                args.last().unwrap()
            ),
        }),
    }?;
    let duration = if t == 0.0 {
        Duration::MAX
    } else {
        Duration::from_micros((t * 1_000_000.) as u64)
    };

    let lists = lists
        .iter()
        .flat_map(|r| match r {
            Resp::BulkString(l) => Some(l.to_vec()),
            _ => None,
        })
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
    args: &[Resp],
    store: &Arc<RwLock<Store>>,
    list_store: &Arc<RwLock<RedisListStore>>,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    if args.len() != 1 {
        return Err(ParseError {
            message: format!("Unsupported TYPE command shape: {:?}", args),
        });
    }
    let key = match &args[0] {
        Resp::BulkString(k) => Ok(k),
        _ => Err(ParseError {
            message: format!("Unsupported TYPE command shape: {:?}", args),
        }),
    }?;
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

fn parse_stream_key<'a>(id: &'a Vec<u8>) -> Option<StreamKey> {
    match and!(integer::<u64>(), byte(b'-'), integer::<u64>()).parse(id) {
        Ok(((mid, _, sid), _)) => Some((mid, sid)),
        _ => None,
    }
}

async fn process_xadd(
    args: &[Resp],
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    if args.len() < 4 {
        return Err(ParseError {
            message: "Unsupported XADD command shape".to_string(),
        });
    }
    let (name, id) = match (&args[0], &args[1]) {
        (Resp::BulkString(n), Resp::BulkString(i)) => Ok((n, i)),
        _ => Err(ParseError {
            message: "Unsupported XADD command shape".to_string(),
        }),
    }?;
    let key = match parse_stream_key(id) {
        Some(k) => Ok(k),
        _ => Err(ParseError {
            message: "Unsupported XADD <id> key shape".to_string(),
        }),
    }?;
    if key < (0, 1) {
        return Ok(Resp::SimpleError(
            b"ERR The ID specified in XADD must be greater than 0-0".to_vec(),
        ));
    }
    if &args[2..].len() % 2 != 0 {
        return Err(ParseError {
            message: "Unsupported XADD command shape".to_string(),
        });
    }
    let values = args[2..]
        .iter()
        .flat_map(|r| match r {
            Resp::BulkString(v) => Some(v.to_vec()),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut store = stream_store.write().await;
    store
        .streams
        .entry(name.to_vec())
        .or_insert(BTreeMap::new());
    // let mut new_btree_map: BTreeMap<(u64, u64), Vec<Vec<u8>>> = BTreeMap::new();
    // let stream = store.streams.get_mut(name).unwrap_or(&mut new_btree_map);
    if store.streams.get(name).unwrap().contains_key(&key) {
        // return Err(ParseError {
        //     message: "XADD: key already exists".to_string(),
        // });
        return Ok(Resp::SimpleError(
            b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                .to_vec(),
        ));
        //return Ok(Resp::Null);
    }
    if let Some((latest, _)) = store.streams.get(name).unwrap().last_key_value() {
        if &key < latest {
            return Ok(Resp::SimpleError(
                b"ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_vec(),
            ));
        }
    }
    store.streams.entry(name.to_vec()).and_modify(|bt| {
        (*bt).insert(key, values);
    });
    // //store.streams.get_mut(name).unwrap().insert(key, values);
    // stream.insert(key, values);
    // store.streams.insert(name.to_vec(), stream);
    Ok(Resp::BulkString(id.to_vec()))
}

async fn process_command(
    input: Resp,
    store: &Arc<RwLock<Store>>,
    list_store: &Arc<RwLock<RedisListStore>>,
    stream_store: &Arc<RwLock<RedisStreamStore>>,
) -> Result<Resp, ParseError> {
    match input {
        Resp::Array(elements) => {
            match &elements[0] {
                Resp::BulkString(command) => {
                    let args = &elements[1..];
                    match &command[..] {
                        b"ECHO" => process_echo(args),
                        b"PING" => process_ping(args),
                        b"SET" => process_set(args, store).await,
                        b"GET" => process_get(args, store).await,
                        // Lists
                        b"RPUSH" => process_list_rpush(args, list_store).await,
                        b"LRANGE" => process_list_lrange(args, list_store).await,
                        b"LPUSH" => process_list_lpush(args, list_store).await,
                        b"LLEN" => process_list_llen(args, list_store).await,
                        b"LPOP" => process_list_lpop(args, list_store).await,
                        b"BLPOP" => process_list_blpop(args, list_store).await,
                        // Streams
                        b"TYPE" => process_type(args, store, list_store, stream_store).await,
                        b"XADD" => process_xadd(args, stream_store).await,
                        _ => Err(ParseError {
                            message: format!(
                                "Unsupported command: {:?} with shape: {:?}",
                                command, args
                            ),
                        }),
                    }
                }
                _ => Err(ParseError {
                    message: format!("invalid command spec: {:?}", elements),
                }),
            }
        }
        _ => Err(ParseError {
            message: "command must be an array (for now)".to_string(),
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

fn encode_resp(r: &Resp, mut out: &mut Vec<u8>) {
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
                encode_resp(e, out);
            }
        }
    }
}

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
    }));

    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let store = Arc::clone(&store); //store.clone();
        let list_store = Arc::clone(&list_store);
        let stream_store = Arc::clone(&stream_store);
        tokio::spawn(async move {
            println!("accepted new connection");
            let mut buffer = [0u8; 1024];

            while let Ok(n) = stream.read(&mut buffer).await {
                if n == 0 {
                    break;
                }
                match parse_resp(&buffer) {
                    Ok((command, _)) => {
                        match process_command(command, &store, &list_store, &stream_store).await {
                            Ok(resp) => {
                                println!("Response: {:?}", resp);
                                let mut out = Vec::new();
                                encode_resp(&resp, &mut out);
                                let _ = stream.write_all(&out[..]).await;
                            }
                            Err(error) => println!("Processing error: {:?}", error),
                        }
                    }
                    Err(error) => println!("Parse error: {:?}", error),
                }
                //let _ = _stream.write(b"+PONG\r\n");
                let _ = stream.flush().await;
                buffer.fill(0u8);
            }
        });
    }
}
