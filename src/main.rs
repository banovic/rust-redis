#![allow(unused_imports)]
use std::{fmt::format, io::{BufRead, BufReader, Read, Write}, net::TcpListener, thread, usize};

// #[derive(Debug,Clone)]
// struct ParseError {
//     message: String
// }

// enum RespElement {
//     Empty,
//     Usize {
//         value: usize
//     },
//     BulkString {
//         len: u32,
//         bytes: Vec<u8>
//     },
//     Array {
//         elements: Vec<RespElement>
//     }
// }

// type ParserResult<'a, T> = Result<(T, &'a [u8]), ParseError>;

// trait Parser<'a, T> {
//     fn parse(&self, input: &'a [u8]) -> ParserResult<'a, T>;
// }

// impl<'a, T, F> Parser<'a, T> for F
// where 
// F: Fn(&'a [u8]) -> ParserResult<'a, T>
// {
//     fn parse(&self, input: &'a [u8]) -> ParserResult<'a, T> {
//         self(input)
//     }
// }

// fn tag3<'a>(bs: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
//     move |input: &'a [u8]| {
//         if input.len() < bs.len() {
//             return Err(ParseError {
//                 message: format!("expected {} bytes, got {}", bs.len(), input.len()),
//             });
//         }
//         let (head, rest) = input.split_at(bs.len());
//         if head == bs {
//             Ok((head, rest))
//         } else {
//             Err(ParseError {
//                 message: format!("expected {:?}, got {:?}", bs, head),
//             })
//         }
//     }
// }

// fn usize3<'a>() -> impl Parser<'a, usize> {
//     move |input: &'a [u8]| {
//         let mut value: usize = 0;
//         let digits = input.iter().take_while(|&b| is_digit(*b)).collect::<Vec<_>>();
//         if digits.is_empty() {
//             return Err(ParseError {
//                 message: "no digits".to_string()
//             });
//         }
//         for (i, &&v) in digits.iter().enumerate() {
//             value += (v as usize) * (10 as usize).pow((digits.len() - i).try_into().unwrap());
//         }
//         let (_, rest) = input.split_at(digits.len());
//         Ok((value, rest))
//     }
// }

// /// Read exactly `n` bytes.
// fn take3<'a>(n: usize) -> impl Parser<'a, &'a [u8]> {
//     move |input: &'a [u8]| {
//         if input.len() < n {
//             return Err(ParseError {
//                 message: format!("expected {n} bytes, got {}", input.len()),
//             });
//         }
//         Ok(input.split_at(n))
//     }
// }

#[derive(Debug)]
enum Resp {
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
    if pc.content[pc.pos..].starts_with(tag) {
        return Ok((tag, RespParseContext { pos: pc.pos + tag.len(), ..pc}));
    }
    Err(RespParseError { message: format!("no tag: {:?} found", tag) })
}

fn is_digit(b: u8) -> bool {
    b >= b'0' && b <= b'9'
}

fn usize<'a>(pc: RespParseContext<'a>) -> RespParseResult<'a, usize> {
    println!("PC - usize: {:?}", pc);
    let digits = pc.content[pc.pos..].iter().take_while(|&b| is_digit(*b)).collect::<Vec<_>>();
    if digits.is_empty() {
        return Err(RespParseError { message: "no digits for usize value".to_string() });
    }
    let mut value: usize = 0;
    for (i, &&v) in digits.iter().enumerate() {
        value += (v as usize) * (10 as usize).pow((digits.len() - i).try_into().unwrap());
    }
    Ok((value, RespParseContext { pos: pc.pos + digits.len(), ..pc}))
}

fn take<'a>(pc: RespParseContext<'a>, n: usize) -> RespParseResult<'a, &'a [u8]> {
    if pc.content[pc.pos..].len() < n {
        return Err(RespParseError { message: format!("expected {n} bytes, got {}", pc.content[pc.pos..].len()) });
    }
    let (head, _) = pc.content[pc.pos..].split_at(n);
    Ok((head, RespParseContext { pos: pc.pos + head.len(), ..pc}))
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
                        let r = parse_resp(RespParseContext { content: &buffer, pos: 0 });
                        println!("Parsed resp: {:?}", r);
                        let _ = _stream.write(b"+PONG\r\n");
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
