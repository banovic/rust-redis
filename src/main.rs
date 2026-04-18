#![allow(unused_imports)]
use std::{collections::HashMap, error, fmt::format, io::{BufRead, BufReader, Read, Write}, net::TcpListener, ops::{AddAssign, Mul, MulAssign}, sync::{Arc, RwLock}, thread, time::{Duration, Instant, SystemTime}, usize};

#[derive(Debug)]
enum Resp {
    Null,
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    Array(Vec<Resp>)
}

#[derive(Debug,Clone)]
struct RespParseError {
    message: String
}
#[derive(Debug,Clone,Copy)]
struct RespParseContext<'a> {
    content: &'a [u8],
    pos: usize
}
impl<'a> RespParseContext<'a> {
    fn from_vec(content: &'a Vec<u8>) -> Self {
        RespParseContext { content, pos: 0 }
    }
}

type RespParseResult<'a, T> = Result<(T, RespParseContext<'a>), RespParseError>;

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

fn process_command(input: Resp, store: &Arc<RwLock<Store>>) -> Result<Resp, RespParseError> {
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

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let store = Arc::clone(&store);//store.clone();
                thread::spawn(move || {
                    println!("accepted new connection");
                    let mut buffer= [0u8; 1024];

                    while let Ok(n) = _stream.read(&mut buffer) {
                        if n == 0 {
                            break;
                        }
                        match parse_resp(RespParseContext { content: &buffer, pos: 0 }) {
                            Ok((command, _)) => match process_command(command, &store) {
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
