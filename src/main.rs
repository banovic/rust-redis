#![allow(unused_imports)]
use std::{error, fmt::format, io::{BufRead, BufReader, Read, Write}, net::TcpListener, thread, usize};

#[derive(Debug)]
enum Resp {
    SimpleString {
        value: Vec<u8>
    },
    BulkString {
        value: Vec<u8>
    },
    Array {
        elements: Vec<Resp>
    }
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

fn usize<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, usize> {
    let input = &pc.content[pc.pos..];
    let digits = input.iter().take_while(|&b| is_digit(*b)).collect::<Vec<_>>();
    if digits.is_empty() {
        return Err(RespParseError { message: "no digits for usize value".to_string() });
    }
    let mut value: usize = 0;
    for (i, &&v) in digits.iter().enumerate() {
        value += ((v - b'0') as usize) * (10 as usize).pow((digits.len() - i - 1).try_into().unwrap());
    }
    Ok((value, RespParseContext { pos: pc.pos + digits.len(), ..pc}))
}

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

    Ok((Resp::SimpleString { value: s.to_vec() }, rest))
}

fn parse_bulk_string<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    let (_, rest) = tag(pc, &[b'$'])?;
    let (l, rest) = usize(rest)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;
    let (s, rest) = take(rest, l)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    Ok((Resp::BulkString { value: s.to_vec() }, rest))
}

fn parse_array<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, Resp> {
    let (_, rest) = tag(pc, &[b'*'])?;
    let (l, rest) = usize(rest)?;
    let (_, rest) = tag(rest, &[b'\r', b'\n'])?;

    let mut elements: Vec<Resp> = Vec::new();
    let mut new_rest = rest;
    for _ in 0..l {
        let (el, rest) = parse_resp(new_rest)?;
        new_rest = rest;
        elements.push(el);
    }

    Ok((Resp::Array { elements }, new_rest))
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
 * Process command
 */
fn process_command(input: Resp) -> Result<Resp, RespParseError>{
    match input {
        Resp::Array { elements } => {
            match &elements[0] {
                Resp::BulkString {value: command} => {
                    let command = &command[..];
                    let args = &elements[1..];
                    match (command, args) {
                        (b"ECHO", [Resp::BulkString { value }]) => Ok(Resp::BulkString { value: value.to_vec() }),
                        (b"PING", [Resp::BulkString { value }]) => Ok(Resp::BulkString { value: value.to_vec() }),
                        (b"PING", _) => Ok(Resp::SimpleString { value: b"PONG".to_vec() }),
                        _ => Err(RespParseError { message: format!("unsupported command or shape: {:?}", command)})
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
        Resp::SimpleString { value } => {
            write_bytes(&mut out, &[b'+']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        },
        Resp::BulkString { value } => {
            write_bytes(&mut out, &[b'$']);
            write_usize(&mut out, value.len());
            write_bytes(&mut out, &[b'\r', b'\n']);
            write_bytes(&mut out, &value[..]);
            write_bytes(&mut out, &[b'\r', b'\n']);
        },
        Resp::Array { elements } => {
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

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                thread::spawn(move || {
                    println!("accepted new connection");
                    let mut buffer= [0u8; 1024];

                    while let Ok(n) = _stream.read(&mut buffer) {
                        if n == 0 {
                            break;
                        }
                        match parse_resp(RespParseContext { content: &buffer, pos: 0 }) {
                            Ok((command, _)) => match process_command(command) {
                                Ok(resp) => {
                                    println!("Response: {:?}", resp);
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
