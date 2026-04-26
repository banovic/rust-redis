#![allow(unused_imports)]
use std::{
    collections::{HashMap, VecDeque},
    error,
    fmt::{self, Debug, format},
    hash::Hash,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    ops::{AddAssign, Mul, MulAssign, Neg},
    result,
    str::{FromStr, from_utf8},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant, SystemTime},
    usize,
};

type ByteString = Vec<u8>;

/// RESP - REdis Serialization Protocol
#[derive(Debug)]
enum Resp {
    Null,
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Integer(i64),
    Array(Vec<Resp>),
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

//
#[derive(Debug, Clone, Copy)]
struct ParseContext<'a> {
    content: &'a [u8],
    pos: usize,
}
impl<'a> ParseContext<'a> {
    fn from_vec(content: &'a Vec<u8>) -> Self {
        ParseContext { content, pos: 0 }
    }

    fn input(&self) -> &'a [u8] {
        &self.content[self.pos..]
    }

    fn forward(&self, n: usize) -> Self {
        ParseContext {
            content: self.content,
            pos: self.pos + n,
        }
    }
}
///
type ParseResult<'a, T> = Result<(T, ParseContext<'a>), ParseError>;
///
trait Parser<'a, T> {
    fn parse(&self, pc: ParseContext<'a>) -> ParseResult<'a, T>;
}

impl<'a, T, F> Parser<'a, T> for F
where
    F: Fn(ParseContext<'a>) -> ParseResult<'a, T>,
{
    fn parse(&self, pc: ParseContext<'a>) -> ParseResult<'a, T> {
        self(pc)
    }
}
///
/// Read `b` byte by value.
fn byte<'a>(b: u8) -> impl Parser<'a, u8> {
    move |pc: ParseContext<'a>| {
        println!("[l:{}][byte][in] b: {}", pc.pos, b);
        let x = match pc.input().len() > 0 && pc.input()[0] == b {
            true => Ok((b, pc.forward(1))),
            _ => Err(ParseError {
                message: format!("no byte: {:?} found", b),
            }),
        };
        println!("[l:{}][byte][OUT] b: {}", x.as_ref().unwrap().1.pos, b);
        x
    }
}

///
/// Read `tag` bytes by value.
fn tag2<'a>(tag: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |pc: ParseContext<'a>| match pc.input().starts_with(tag) {
        true => Ok((tag, pc.forward(tag.len()))),
        _ => Err(ParseError {
            message: format!("no tag: {:?} found", tag),
        }),
    }
}

/// Read `n` bytes.
fn take2<'a>(n: usize) -> impl Parser<'a, &'a [u8]> {
    move |pc: ParseContext<'a>| {
        if pc.input().len() < n {
            return Err(ParseError {
                message: format!("expected {} bytes, got {}", n, pc.input().len()),
            });
        }
        let (head, _) = pc.input().split_at(n);
        Ok((head, pc.forward(head.len())))
    }
}

/// Read all bytes while predicate `pred` returns true.
fn take_while2<'a>(pred: impl Fn(u8) -> bool) -> impl Parser<'a, &'a [u8]> {
    move |pc: ParseContext<'a>| {
        println!("digits, pc: {:?}", pc);

        let digits_len = pc.input().iter().take_while(|&&b| pred(b)).count();
        if digits_len == 0 {
            return Err(ParseError {
                message: format!(
                    "expected to match at least one byte, but matched 0; pc = {:?}",
                    pc
                ),
            });
        }
        let (digits, _) = pc.content.split_at(pc.pos + digits_len);
        Ok((digits, pc.forward(digits_len)))
    }
}

/// Read all bytes until `limit` bytes are next. It does not read any of `limit` bytes.
fn take_until<'a>(limit: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |pc: ParseContext<'a>| {
        let mut i = pc.pos;
        while i < pc.content.len() - pc.pos {
            if pc.content[i..].starts_with(limit) {
                return Ok((&pc.content[pc.pos..i], ParseContext { pos: i, ..pc }));
            }
            i += 1;
        }

        Err(ParseError {
            message: format!("expected to match limit: {:?}, but haven't", limit),
        })
    }
}

/// `or` combinator, it succeeds if `p1` or `p2` succeeds.
fn or<'a, T: Debug>(p1: impl Parser<'a, T>, p2: impl Parser<'a, T>) -> impl Parser<'a, T> {
    move |pc: ParseContext<'a>| {
        //println!("or, pc: {:?}", pc);
        let x = match p1.parse(pc) {
            Ok(result) => Ok(result),
            _ => p2.parse(pc),
        };
        //println!("or, out: {:?}", x);
        x
    }
}

/// `and` combinator, it succeeds when `p1` matches and then `p2` matches.
fn and<'a, A, B>(p1: impl Parser<'a, A>, p2: impl Parser<'a, B>) -> impl Parser<'a, (A, B)> {
    move |pc: ParseContext<'a>| {
        //println!("and, pc: {:?}", pc);
        let (a, rest) = p1.parse(pc)?;
        let (b, rest) = p2.parse(rest)?;
        Ok(((a, b), rest))
    }
}

macro_rules! and {
    ($p1: expr, $p2: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            Ok(((a, b), rest))
        }
    };

    ($p1: expr, $p2: expr, $p3: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            let (c, rest) = $p3.parse(rest)?;
            Ok(((a, b, c), rest))
        }
    };

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            let (c, rest) = $p3.parse(rest)?;
            let (d, rest) = $p4.parse(rest)?;
            Ok(((a, b, c, d), rest))
        }
    };

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            let (c, rest) = $p3.parse(rest)?;
            let (d, rest) = $p4.parse(rest)?;
            let (e, rest) = $p5.parse(rest)?;
            Ok(((a, b, c, d, e), rest))
        }
    };

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr, $p6: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            let (c, rest) = $p3.parse(rest)?;
            let (d, rest) = $p4.parse(rest)?;
            let (e, rest) = $p5.parse(rest)?;
            let (f, rest) = $p6.parse(rest)?;
            Ok(((a, b, c, d, e, f), rest))
        }
    };

    ($p1: expr, $p2: expr, $p3: expr, $p4: expr, $p5: expr, $p6: expr, $p7: expr $(,)?) => {
        move |pc: ParseContext<'a>| {
            //println!("and, pc: {:?}", pc);
            let (a, rest) = $p1.parse(pc)?;
            let (b, rest) = $p2.parse(rest)?;
            let (c, rest) = $p3.parse(rest)?;
            let (d, rest) = $p4.parse(rest)?;
            let (e, rest) = $p5.parse(rest)?;
            let (f, rest) = $p6.parse(rest)?;
            let (g, rest) = $p7.parse(rest)?;
            Ok(((a, b, c, d, e, f, g), rest))
        }
    };
}
/// `opt` combinator, it always succeeds. If it matches input is advanced.
fn opt<'a, T: Debug>(p: impl Parser<'a, T>) -> impl Parser<'a, Option<T>> {
    move |pc: ParseContext<'a>| {
        //println!("opt, pc: {:?}", pc);
        let x = match p.parse(pc) {
            Ok((result, rest)) => Ok((Some(result), rest)),
            _ => Ok((None, pc)),
        };
        //println!("opt, out: {:?}", &x);
        x
    }
}

fn unsigned_integer<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr,
{
    move |pc: ParseContext<'a>| {
        let digits_parser = take_while2(|b| b.is_ascii_digit());
        let ((_, digits), rest) = and(opt(byte(b'+')), digits_parser).parse(pc)?;
        // Ok, since digits are ASCII
        let s = from_utf8(digits).unwrap();
        let n = match s.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(ParseError {
                message: format!("cannot parse digits from string: {}", s),
            }),
        }?;
        Ok((n, rest))
    }
}

fn signed_integer<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr + Mul<Output = T> + From<i8>,
{
    move |pc: ParseContext<'a>| {
        println!("signed_integer, pc: {:?}", pc);
        let digits_parser = take_while2(|b| b.is_ascii_digit());
        let ((sign, digits), rest) =
            and(opt(or(byte(b'-'), byte(b'+'))), digits_parser).parse(pc)?;
        println!(
            "signed_integer, sign: {:?}, digits = {:?}, rest: {:?}",
            sign, digits, rest
        );
        // Ok, since digits are ASCII
        let s = from_utf8(digits).unwrap();
        let n = match s.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(ParseError {
                message: format!("cannot parse digits from string: {}", s),
            }),
        }?;
        // let n = match sign {
        //     Some(b'-') => n * T::from(-1),
        //     _ => n
        // };
        Ok((n, rest))
    }
}
///
// fn tag<'a>(pc: ParseContext<'a>, tag: &'static [u8]) -> ParseResult<'a, &'a [u8]> {
//     let input = &pc.content[pc.pos..];
//     if input.starts_with(tag) {
//         return Ok((
//             tag,
//             ParseContext {
//                 pos: pc.pos + tag.len(),
//                 ..pc
//             },
//         ));
//     }
//     Err(ParseError {
//         message: format!("no tag: {:?} found", tag),
//     })
// }

// fn is_digit(b: u8) -> bool {
//     b >= b'0' && b <= b'9'
// }

// fn unsigned_number<'a, T>(pc: ParseContext<'a>) -> ParseResult<'a, T>
// where
//     T: From<u8> + AddAssign<T> + MulAssign<T> + Mul<T, Output = T>,
// {
//     let input = &pc.content[pc.pos..];
//     let digits = input
//         .iter()
//         .take_while(|&b| is_digit(*b))
//         .collect::<Vec<_>>();
//     if digits.is_empty() {
//         return Err(ParseError {
//             message: "no digits for unsigned_number value".to_string(),
//         });
//     }
//     let mut value: T = T::from(0);
//     for (i, &&v) in digits.iter().enumerate() {
//         let d = T::from(v - b'0');
//         let n = digits.len() - i - 1;
//         let mut m = T::from(1);
//         for _ in 0..n {
//             m *= T::from(10);
//         }
//         value += d * m;
//     }
//     Ok((
//         value,
//         ParseContext {
//             pos: pc.pos + digits.len(),
//             ..pc
//         },
//     ))
// }

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

// fn take<'a>(pc: ParseContext<'a>, n: usize) -> ParseResult<'a, &'a [u8]> {
//     let input = &pc.content[pc.pos..];
//     if input.len() < n {
//         return Err(ParseError {
//             message: format!("expected {n} bytes, got {}", input.len()),
//         });
//     }
//     let (head, _) = input.split_at(n);
//     Ok((
//         head,
//         ParseContext {
//             pos: pc.pos + head.len(),
//             ..pc
//         },
//     ))
// }

// fn take_while<'a>(pc: ParseContext<'a>, needle: &'static [u8]) -> ParseResult<'a, &'a [u8]> {
//     let mut i = pc.pos;
//     while i < pc.content.len() - pc.pos {
//         if pc.content[i..].starts_with(needle) {
//             return Ok((&pc.content[pc.pos..i], ParseContext { pos: i, ..pc }));
//         }
//         i += 1;
//     }

//     Err(ParseError {
//         message: format!("expected to match needle: {:?}, but haven't", needle),
//     })
// }

/// APP PARSERS
/// Parse client request:
fn parse_simple_string<'a>(pc: ParseContext<'a>) -> ParseResult<'a, Resp> {
    let ((_, s, _), rest) = and!(
        byte(b'+'),
        take_until(&[b'\r', b'\n']),
        tag2(&[b'\r', b'\n'])
    )
    .parse(pc)?;

    Ok((Resp::SimpleString(s.to_vec()), rest))
}

fn parse_bulk_string<'a>(pc: ParseContext<'a>) -> ParseResult<'a, Resp> {
    let ((_, l), rest) = and!(byte(b'$'), unsigned_integer::<usize>()).parse(pc)?;
    let ((_, s, _), rest) =
        and!(tag2(&[b'\r', b'\n']), take2(l), tag2(&[b'\r', b'\n'])).parse(rest)?;
    // let (_, rest) = tag(pc, &[b'$'])?;
    // let (l, rest) = unsigned_number::<usize>(rest)?;
    // let (_, rest) = tag(rest, &[b'\r', b'\n'])?;
    // let (s, rest) = take(rest, l)?;
    // let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    Ok((Resp::BulkString(s.to_vec()), rest))
}

fn parse_array<'a>(pc: ParseContext<'a>) -> ParseResult<'a, Resp> {
    let ((_, l, _), rest) = and!(
        byte(b'*'),
        unsigned_integer::<usize>(),
        tag2(&[b'\r', b'\n'])
    )
    .parse(pc)?;
    // let (_, rest) = tag(pc, &[b'*'])?;
    // let (l, rest) = unsigned_number::<usize>(rest)?;
    // let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    let mut elements: Vec<Resp> = Vec::new();
    let mut new_rest = rest;
    for _ in 0..l {
        let (el, rest) = parse_resp(new_rest)?;
        new_rest = rest;
        elements.push(el);
    }

    Ok((Resp::Array(elements), new_rest))
}

fn parse_resp<'a>(pc: ParseContext<'a>) -> ParseResult<'a, Resp> {
    match pc.content[pc.pos] {
        b'$' => parse_bulk_string(pc),
        b'*' => parse_array(pc),
        _ => Err(ParseError {
            message: format!("unknown RESP first byte: {}", pc.content[0]),
        }),
    }
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
type RedisListStore = HashMap<ByteString, RedisList>;

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

fn process_set(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(key), Resp::BulkString(value)] => {
            let mut store = store.write().unwrap();
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
            let n = match unsigned_integer::<u64>().parse(ParseContext::from_vec(ttl)) {
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
            let mut store = store.write().unwrap();
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

fn process_get(args: &[Resp], store: &Arc<RwLock<Store>>) -> Result<Resp, ParseError> {
    match args {
        [Resp::BulkString(key)] => {
            let store = store.read().unwrap();
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
fn process_list_rpush(
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

    let mut store = list_store.write().unwrap();

    store
        .entry(name.to_vec())
        .and_modify(|e| e.append(&mut elements))
        .or_insert(elements);

    Ok(Resp::Integer(store.get(name).map_or(0, |l| l.len() as i64)))
}

fn process_list_lpush(
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

    let mut store = list_store.write().unwrap();
    let mut list = store.get_mut(name).unwrap_or(&mut VecDeque::new());
    store
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
    // for el in &args[1..] {
    //     if let Resp::BulkString(element) = el {
    //         list.push_front(element.to_vec());
    //     }
    // }

    // store.entry(name.to_vec()).insert_entry(list);

    Ok(Resp::Integer(store.get(name).map_or(0, |l| l.len() as i64)))
}

fn process_list_llen(
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

    let store = list_store.read().unwrap();
    let l = match store.get(name) {
        Some(l) => l.len(),
        _ => 0,
    };
    Ok(Resp::Integer(l as i64))
}

fn process_list_lpop(
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let (name, count) = match args {
        [Resp::BulkString(name)] => Ok((name, None)),
        [Resp::BulkString(name), Resp::BulkString(count)] => {
            //let (count, _) = unsigned_integer::<u32>().parse(RespParseContext::from_vec(count))?;
            match unsigned_integer::<u32>().parse(ParseContext::from_vec(count))? {
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

    let mut store = list_store.write().unwrap();
    let list = store.get_mut(name);
    if list.is_none() {
        return Ok(Resp::Null);
    }
    let mut list = list.unwrap();
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

fn process_list_lrange(
    args: &[Resp],
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
    let (name, start, stop) = match args {
        [
            Resp::BulkString(name),
            Resp::BulkString(start),
            Resp::BulkString(stop),
        ] => {
            let (start, _) = signed_integer::<i32>().parse(ParseContext::from_vec(start))?;
            let (stop, _) = signed_integer::<i32>().parse(ParseContext::from_vec(stop))?;
            Ok((name, start, stop))
        }
        _ => Err(ParseError {
            message: format!("Unsupported LRANGE command shape: {:?}", args),
        }),
    }?;
    println!("start: {}, stop: {}", start, stop);

    let mut result = Vec::new();
    let store = list_store.read().unwrap();
    let list_option = store.get(name);
    if list_option.is_none() {
        return Ok(Resp::Array(result));
    }
    let list = list_option.unwrap();

    // if start > (list.len() as i32) || start > stop {
    //     return Ok(Resp::Array(result));
    // }
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

fn process_command(
    input: Resp,
    store: &Arc<RwLock<Store>>,
    list_store: &Arc<RwLock<RedisListStore>>,
) -> Result<Resp, ParseError> {
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
                        b"LPUSH" => process_list_lpush(args, list_store),
                        b"LLEN" => process_list_llen(args, list_store),
                        b"LPOP" => process_list_lpop(args, list_store),
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
        Resp::SimpleString(value) => {
            write_bytes(&mut out, &[b'+']);
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
                let store = Arc::clone(&store); //store.clone();
                let list_store = Arc::clone(&list_store);
                thread::spawn(move || {
                    println!("accepted new connection");
                    let mut buffer = [0u8; 1024];

                    while let Ok(n) = _stream.read(&mut buffer) {
                        if n == 0 {
                            break;
                        }
                        match parse_resp(ParseContext {
                            content: &buffer,
                            pos: 0,
                        }) {
                            Ok((command, _)) => match process_command(command, &store, &list_store)
                            {
                                Ok(resp) => {
                                    //println!("Response: {:?}", resp);
                                    let mut out = Vec::new();
                                    encode_resp(&resp, &mut out);
                                    let _ = _stream.write(&out[..]);
                                }
                                Err(error) => println!("Processing error: {:?}", error),
                            },
                            Err(error) => println!("Parse error: {:?}", error),
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
