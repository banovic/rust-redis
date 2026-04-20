#![allow(unused_imports)]
use std::{collections::HashMap, error, fmt::format, hash::Hash, io::{BufRead, BufReader, Read, Write}, net::TcpListener, ops::{AddAssign, Mul, MulAssign, Neg}, result, str::{FromStr, from_utf8}, sync::{Arc, RwLock}, thread, time::{Duration, Instant, SystemTime}, usize};

type ByteString = Vec<u8>;

#[derive(Debug)]
enum Resp {
    Null,
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Resp>)
}

#[derive(Debug,Clone)]
struct RespParseError {
    message: String
}

//
#[derive(Debug,Clone,Copy)]
struct RespParseContext<'a> {
    content: &'a [u8],
    pos: usize
}
impl<'a> RespParseContext<'a> {
    fn from_vec(content: &'a Vec<u8>) -> Self {
        RespParseContext { content, pos: 0 }
    }

    fn input(&self) -> &'a [u8] {
        &self.content[self.pos..]
    }

    fn forward(&self, n: usize) -> Self {
        RespParseContext { content: self.content, pos: self.pos + n }
    }
}
///
type RespParseResult<'a, T> = Result<(T, RespParseContext<'a>), RespParseError>;
///
trait Parser<'a, T> {
    fn parse(&self, pc: RespParseContext<'a>) -> RespParseResult<'a, T>;
}

impl<'a, T, F> Parser<'a, T> for F
where
    F: Fn(RespParseContext<'a>) -> RespParseResult<'a, T>
{
    fn parse(&self, pc: RespParseContext<'a>) -> RespParseResult<'a, T> {
        self(pc)
    }
}
///
/// Read `b` byte by value.
fn byte<'a>(b: u8) -> impl Parser<'a, u8> {
    move |pc: RespParseContext<'a>| match pc.input().len() > 0 && pc.input()[0] == b {
        true => Ok((b, pc.forward(1))),
        _ => Err(RespParseError { message: format!("no byte: {:?} found", b) })
    }
}

///
/// Read `tag` bytes by value.
fn tag2<'a>(tag: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |pc: RespParseContext<'a>| {
        match pc.input().starts_with(tag) {
            true => Ok((tag, pc.forward(tag.len()))),
            _ => Err(RespParseError { message: format!("no tag: {:?} found", tag) })
        }
    }
}

/// Read `n` bytes.
fn take2<'a>(n: usize) -> impl Parser<'a, &'a [u8]> {
    move |pc: RespParseContext<'a>| {
        if pc.input().len() < n {
            return Err(RespParseError { message: format!("expected {} bytes, got {}", n, pc.input().len()) });
        }
        let (head, _) = pc.input().split_at(n);
        Ok((head, pc.forward(head.len())))
    }
}

/// Read all bytes while predicate `pred` returns true.
fn take_while2<'a>(pred: impl Fn(u8) -> bool) -> impl Parser<'a, &'a [u8]> {
    move |pc: RespParseContext<'a>| {
        let digits_len = pc.input().iter().take_while(|&&b| pred(b)).count();
        if digits_len == 0 {
            return Err(RespParseError { message: format!("expected to match at least one byte, but matched 0; pc = {:?}", pc) })
        }
        let (digits, _) = pc.content.split_at(pc.pos + digits_len);
        Ok((digits, pc.forward(digits_len)))
    }
}

/// `or` combinator, it succeeds if `p1` or `p2` succeeds.
fn or<'a, T>(p1: impl Parser<'a, T>, p2: impl Parser<'a, T>) -> impl Parser<'a, T> {
    move |pc: RespParseContext<'a>| match p1.parse(pc) {
        Ok(result) => Ok(result),
        _ => p2.parse(pc)
    }
}

/// `and` combinator, it succeeds when `p1` matches and then `p2` matches.
fn and<'a, A, B>(p1: impl Parser<'a, A>, p2: impl Parser<'a, B>) -> impl Parser<'a, (A, B)> {
    move |pc: RespParseContext<'a>| {
        let (a, rest) = p1.parse(pc)?;
        let (b, rest) = p2.parse(rest)?;
        Ok(((a, b), rest))
    }
}

/// `opt` combinator, it always succeeds. If it matches input is advanced.
fn opt<'a, T>(p: impl Parser<'a, T>) -> impl Parser<'a, Option<T>> {
    move |pc: RespParseContext<'a>| match p.parse(pc) {
        Ok((result, rest)) => Ok((Some(result), rest)),
        _ => Ok((None, pc))
    }
}

fn unsigned_integer<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr
{
    move |pc: RespParseContext<'a>| {
        let digits_parser = take_while2(|b| b.is_ascii_digit());
        let ((_, digits), rest) = and(opt(byte(b'+')), digits_parser).parse(pc)?;
        // Ok, since digits are ASCII
        let s = from_utf8(digits).unwrap();
        let n = match s.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(RespParseError { message: format!("cannot parse digits from string: {}", s) })
        }?;
        Ok((n, rest))
    }
}

fn signed_integer<'a, T>() -> impl Parser<'a, T>
where
    T: Neg<Output = T> + FromStr
{
    move |pc: RespParseContext<'a>| {
        let digits_parser = take_while2(|b| b.is_ascii_digit());
        let ((sign, digits), rest) = and(opt(or(byte(b'-'), byte(b'+'))), digits_parser).parse(pc)?;
        // Ok, since digits are ASCII
        let s = from_utf8(digits).unwrap();
        let n = match s.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(RespParseError { message: format!("cannot parse digits from string: {}", s) })
        }?;
        let n = match sign {
            Some(b'-') => n.neg(),
            _ => n
        };
        Ok((n, rest))
    }
}
///
fn tag<'a>(pc: RespParseContext<'a>, tag: &'static [u8]) -> RespParseResult<'a, &'a [u8]> {
    let input = &pc.content[pc.pos..];
    if input.starts_with(tag) {
        return Ok((tag, RespParseContext { pos: pc.pos + tag.len(), ..pc}));
    }
    Err(RespParseError { message: format!("no tag: {:?} found", tag) })
}

fn is_digit(b: u8) -> bool {
    b >= b'0' && b <= b'9'
}

fn unsigned_number<'a, T>(pc: RespParseContext<'a>) -> RespParseResult<'a, T> where T: From<u8> + AddAssign<T> + MulAssign<T> + Mul<T, Output = T> {
    let input = &pc.content[pc.pos..];
    let digits = input.iter().take_while(|&b| is_digit(*b)).collect::<Vec<_>>();
    if digits.is_empty() {
        return Err(RespParseError { message: "no digits for unsigned_number value".to_string() });
    }
    let mut value: T = T::from(0);
    for (i, &&v) in digits.iter().enumerate() {
        let d = T::from(v - b'0');
        let n = digits.len() - i - 1;
        let mut m = T::from(1);
        for _ in 0..n {
            m *= T::from(10);
        }
        value += d * m;
    }
    Ok((value, RespParseContext { pos: pc.pos + digits.len(), ..pc}))
}

// fn usize<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, usize> {
//     let input = &pc.content[pc.pos..];
//     let digits = input.iter().take_while(|&b| is_digit(*b)).collect::<Vec<_>>();
//     if digits.is_empty() {
//         return Err(RespParseError { message: "no digits for usize value".to_string() });
//     }
//     let mut value: usize = 0;
//     for (i, &&v) in digits.iter().enumerate() {
//         value += ((v - b'0') as usize) * (10 as usize).pow((digits.len() - i - 1).try_into().unwrap());
//     }
//     Ok((value, RespParseContext { pos: pc.pos + digits.len(), ..pc}))
// }

fn take<'a>(pc: RespParseContext<'a>, n: usize) -> RespParseResult<'a, &'a [u8]> {
    let input = &pc.content[pc.pos..];
    if input.len() < n {
        return Err(RespParseError { message: format!("expected {n} bytes, got {}", input.len()) });
    }
    let (head, _) = input.split_at(n);
    Ok((head, RespParseContext { pos: pc.pos + head.len(), ..pc}))
}

fn take_while<'a>(pc: RespParseContext<'a>, needle: &'static [u8]) -> RespParseResult<'a, &'a [u8]> {
    let mut i = pc.pos;
    while i < pc.content.len() - pc.pos {
        if pc.content[i..].starts_with(needle) {
            return Ok((&pc.content[pc.pos..i], RespParseContext {pos: i, ..pc}));
        }
        i += 1;
    }

    Err(RespParseError { message: format!("expected to match needle: {:?}, but haven't", needle) })
}

/// Parse client request:
fn parse_simple_string<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    let (_, rest) = tag(pc, &[b'+'])?;
    let (s, rest) = take_while(rest, &[b'\r', b'\n'])?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    Ok((Resp::SimpleString(s.to_vec()), rest))
}

fn parse_bulk_string<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    let (_, rest) = tag(pc, &[b'$'])?;
    let (l, rest) = unsigned_number::<usize>(rest)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;
    let (s, rest) = take(rest, l)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    Ok((Resp::BulkString(s.to_vec()), rest))
}

fn parse_array<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    let (_, rest) = tag(pc, &[b'*'])?;
    let (l, rest) = unsigned_number::<usize>(rest)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    let mut elements: Vec<Resp> = Vec::new();
    let mut new_rest = rest;
    for _ in 0..l {
        let (el, rest) = parse_resp(new_rest)?;
        new_rest = rest;
        elements.push(el);
    }

    Ok((Resp::Array(elements), new_rest))
}

fn parse_resp<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    match pc.content[pc.pos] {
        b'$' => {
            parse_bulk_string(pc)
        },
        b'*' => {
            parse_array(pc)
        },
        _ => Err(RespParseError { message: format!("unknown RESP first byte: {}", pc.content[0]) })
    }
}
/**
 * Store
 */
struct StoreValue {
    t: Instant,
    ttl: Option<Duration>,
    value: Vec<u8>
}

type Store = HashMap<Vec<u8>, StoreValue>;

type RedisList = Vec<ByteString>;
type RedisListStore = HashMap<ByteString, RedisList>;

/**
 * Process command
 */
fn process_echo(args: &[Resp]) -> Result<Resp, RespParseError> {
    match args {
        [Resp::BulkString(value)] => Ok(Resp::BulkString(value.to_vec())),
        _ => Err(RespParseError { message: format!("Unsupported ECHO command shape: {:?}", args) })
    }
}

fn process_ping(args: &[Resp]) -> Result<Resp, RespParseError> {
    match args {
        [Resp::BulkString(value)] => Ok(Resp::BulkString(value.to_vec())),
        [] => Ok(Resp::SimpleString(b"PONG".to_vec())),
        _ => Err(RespParseError { message: format!("Unsupported PING command shape: {:?}", args) })
    }
}

fn process_set(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, RespParseError> {
    match args {
        [Resp::BulkString(key), Resp::BulkString(value)] => {
            let mut store = store.write().unwrap();
            let value = StoreValue {
                t: Instant::now(),
                ttl: None,
                value: value.to_vec()
            };
            (*store).insert(key.to_vec(), value);
            Ok(Resp::SimpleString(b"OK".to_vec()))
        },
        [Resp::BulkString(key), Resp::BulkString(value), Resp::BulkString(expx), Resp::BulkString(ttl)] => {
            let n = match unsigned_number::<u64>(RespParseContext::from_vec(ttl)) {
                Ok((value, _)) => value,
                Err(RespParseError { message }) => return Err(RespParseError { message: format!("Invalid time value for SET command: {:?}", message)})
            };
            let ttl = match &expx[..] {
                b"EX" => Duration::from_secs(n),
                b"PX" => Duration::from_millis(n),
                _ => return Err(RespParseError { message: format!("Invalid time spec for SET (should be PX or EX): {:?}", expx) })
            };
            let mut store = store.write().unwrap();
            let value = StoreValue {
                t: Instant::now(),
                ttl: Some(ttl),
                value: value.to_vec()
            };
            (*store).insert(key.to_vec(), value);
            Ok(Resp::SimpleString(b"OK".to_vec()))
        },
        _ => Err(RespParseError { message: format!("Unsupported SET command shape: {:?}", args) })
    }
}

fn process_get(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, RespParseError> {
    match args {
        [Resp::BulkString(key)] => {
            let store = store.read().unwrap();
            match store.get(key) {
                Some(StoreValue{t, ttl, value}) => {
                    match ttl {
                        None => Ok(Resp::BulkString(value.to_vec())),
                        Some(duration) if *t + *duration < Instant::now() => Ok(Resp::Null),
                        Some(_) => Ok(Resp::BulkString(value.to_vec())),
                    }
                } 
                None => Ok(Resp::Null)
            }
        },
        _ => Err(RespParseError { message: format!("Unsupported GET command shape: {:?}", args) })
    }
}

// Lists
fn process_list_rpush(args: &[Resp], list_store: &Arc<RwLock<RedisListStore>>) -> Result<Resp, RespParseError> {
    let name = match &args[0] {
        Resp::BulkString(name) => Ok(name),
        _ => Err(RespParseError { message: format!("Unsupported RPUSH command shape, missing list name: {:?}",  args)})
    }?;

    let mut elements = Vec::new();

    for el in &args[1..] {
        if let Resp::BulkString(element) = el {
            elements.push(element.to_vec());
        }
    }

    let mut store = list_store.write().unwrap();

    store.entry(name.to_vec()).and_modify(|e| e.append(&mut elements)).or_insert(elements);

    Ok(Resp::Integer(store.get(name).map_or(0, |l| l.len() as i64)))
}

fn process_list_lrange(args: &[Resp], list_store: &Arc<RwLock<RedisListStore>>) -> Result<Resp, RespParseError> {
    let (name, start, stop) = match args {
        [Resp::BulkString(name), Resp::BulkString(start), Resp::BulkString(stop)] => {
            let (start, _) = signed_integer::<i32>().parse(RespParseContext::from_vec(start))?;
            let (stop, _) = signed_integer::<i32>().parse(RespParseContext::from_vec(stop))?;
            Ok((name, start, stop))
        },
        _ => Err(RespParseError { message: format!("Unsupported LRANGE command shape: {:?}", args) })
    }?;

    let mut result = Vec::new();
    let store = list_store.read().unwrap();
    let list_option = store.get(name);
    if list_option.is_none() {
        return Ok(Resp::Array(result));
    }
    let list = list_option.unwrap();
    if start > (list.len() as i32) || start > stop {
        return Ok(Resp::Array(result));
    }
    
    let a = if start < 0 {
        0
    } else {
        start as usize
    };

    let b = if stop > (list.len() as i32 - 1) {
        list.len() - 1
    } else if stop < 0 {
        list.len() - 1 + stop.abs() as usize
    } else {
        stop as usize
    };
    for i in a..=b {
        result.push(Resp::BulkString(list[i].to_vec()));
    }
    Ok(Resp::Array(result))
    //panic!()
    // let (name, start, stop) = match args {
    //     [Resp::BulkString(name), Resp::BulkString(start), Resp::BulkString(stop)] => {
    //         //Ok(name, )
    //         panic!()
    //     },
    //     _ => Err(RespParseError { message: format!("Unsupported LRANGE command shape: {:?}", args) })
    // }?;
    // let name = match &args[0] {
    //     Resp::BulkString(name) => Ok(name),
    //     _ => Err(RespParseError { message: format!("Unsupported RPUSH command shape, missing list name: {:?}",  args)})
    // }?;

    // let mut elements = Vec::new();

    // for el in &args[1..] {
    //     if let Resp::BulkString(element) = el {
    //         elements.push(element.to_vec());
    //     }
    // }

    // let mut store = list_store.write().unwrap();

    // store.entry(name.to_vec()).and_modify(|e| e.append(&mut elements)).or_insert(elements);

    // Ok(Resp::Integer(store.get(name).map_or(0, |l| l.len() as i64)))
}



fn process_command(input: Resp, store: &Arc<RwLock<Store>>, list_store: &Arc<RwLock<RedisListStore>>) -> Result<Resp, RespParseError> {
    match input {
        Resp::Array(elements) => {
            match &elements[0] {
                Resp::BulkString(command) => {
                    let args = &elements[1..];
                    match &command[..] {
                        b"ECHO" => process_echo(args),
                        b"PING" => process_ping(args),
                        b"SET" => process_set(args, store),
                        b"GET" => process_get(args, store),
                        // Lists
                        b"RPUSH" => process_list_rpush(args, list_store),
                        b"LRANGE" => process_list_lrange(args, list_store),
                        _ => Err(RespParseError { message: format!("Unsupported command: {:?} with shape: {:?}", command, args)})
                    }
                }
                _ => Err(RespParseError { message: format!("invalid command spec: {:?}", elements) })
            }
        },
        _ => Err(RespParseError { message: "command must be an array (for now)".to_string() })
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
        },
        Resp::SimpleString(value) => {
            write_bytes(&mut out, &[b'+']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        },
        Resp::BulkString(value) => {
            write_bytes(&mut out, &[b'$']);
            write_usize(&mut out, value.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        },
        Resp::Integer(n) => {
            write_bytes(&mut out, &[b':']);
            write_bytes(&mut out, n.to_string().as_bytes());
            write_bytes(&mut out, &[b'\r', b'\n']);
        },
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

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let store = Arc::new(RwLock::new(HashMap::new()));
    let list_store = Arc::new(RwLock::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let store = Arc::clone(&store);//store.clone();
                let list_store = Arc::clone(&list_store);
                thread::spawn(move || {
                    println!("accepted new connection");
                    let mut buffer= [0u8; 1024];

                    while let Ok(n) = _stream.read(&mut buffer) {
                        if n == 0 {
                            break;
                        }
                        match parse_resp(RespParseContext { content: &buffer, pos: 0 }) {
                            Ok((command, _)) => match process_command(command, &store, &list_store) {
                                Ok(resp) => {
                                    //println!("Response: {:?}", resp);
                                    let mut out = Vec::new();
                                    encode_resp(&resp, &mut out);
                                    let _ = _stream.write(&out[..]);
                                },
                                Err(error) => println!("Processing error: {:?}", error)
                            }
                            Err(error) => println!("Parse error: {:?}", error)
                        }
                        //let _ = _stream.write(b"+PONG\r\n");
                        let _ = _stream.flush();
                        buffer.fill(0u8);
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
