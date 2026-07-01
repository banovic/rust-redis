//mod crate::parser;
use crate::parser::*;

/// Each request to the server or client should be Resp - any number of Resp elements, since
/// concatenating multiple Resp elements is valid Resp. Every incoming request is decoded into
/// Resp, and every outgoing response is decoded into Resp.
/// Commands are decoded from Resp; results of Command execution might vary - decided later.
/// Commands should return Resp if they have immediate result to return - as said, more later.
#[derive(Debug, Clone)]
pub enum Resp {
    NullBulkString,
    NullArray,
    SimpleString(Vec<u8>),
    BulkString(Vec<u8>),
    SimpleError(Vec<u8>),
    Integer(i64),
    File(Vec<u8>),
    Array(Vec<Resp>),
}

impl Resp {
    // What is default?
    // Default is just a buffer full of  bytes that can contain 1 or more Resp elements.
    pub fn from_bytes(input: &[u8]) -> Option<Resp> {
        if let Ok((resp, _)) = parse_single_resp(input) {
            resp
        } else {
            None
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        encode_resp(self)
    }

    pub fn len(&self) -> usize {
        match self {
            Resp::NullBulkString => 5,
            Resp::NullArray => 5,
            Resp::File(f) => f.len().to_string().as_bytes().len() + f.len() + 3,
            Resp::Integer(i) => 3 + i.to_string().as_bytes().len(),
            Resp::SimpleString(s) => 3 + s.len(),
            Resp::BulkString(s) => 5 + s.len().to_string().as_bytes().len() + s.len(),
            Resp::SimpleError(e) => 3 + e.len(),
            Resp::Array(els) => {
                let s = els.iter().map(|el| el.len()).sum::<usize>();
                els.len().to_string().as_bytes().len() + s + 3
            }
        }
    }

    pub fn get_str(&self) -> Option<&str> {
        match self {
            Resp::NullBulkString => None,
            Resp::NullArray => None,
            Resp::File(_) => None,
            Resp::Integer(_) => None,
            Resp::Array(_) => None,
            Resp::SimpleString(s) => str::from_utf8(s).ok(),
            Resp::BulkString(s) => str::from_utf8(s).ok(),
            Resp::SimpleError(e) => str::from_utf8(e).ok(),
        }
    }

    pub fn get_bytes(&self) -> Option<Vec<u8>> {
        match self {
            Resp::NullBulkString => None,
            Resp::NullArray => None,
            Resp::File(f) => Some(f.clone()),
            Resp::Integer(_) => None,
            Resp::Array(_) => None,
            Resp::SimpleString(s) => Some(s.clone()),
            Resp::BulkString(s) => Some(s.clone()),
            Resp::SimpleError(e) => Some(e.clone()),
        }
    }

    // b"$-1\r\n" ~ |$|-|1|\r|\n| bytes
    pub fn null_bulk_string() -> Resp {
        Resp::NullBulkString
    }

    // b"*-1\r\n" ~ |*|-|1|\r|\n| bytes
    pub fn null_array() -> Resp {
        Resp::NullArray
    }

    // |+|...s...|\r|\n|
    pub fn simple_string(s: &str) -> Resp {
        Resp::SimpleString(s.as_bytes().to_vec())
    }

    // |$|...s.len()...|\r|\n|...s....|\r|\n|
    pub fn bulk_string(s: &str) -> Resp {
        Resp::BulkString(s.as_bytes().to_vec())
    }

    // |-|...s...|\r|\n|
    pub fn simple_error(e: &str) -> Resp {
        Resp::SimpleError(e.as_bytes().to_vec())
    }

    // |:|...i...|\r|\n|
    pub fn integer(i: i64) -> Resp {
        Resp::Integer(i)
    }

    // |$|...bytes.len()...|\r|\n|...bytes...|
    pub fn file(bytes: Vec<u8>) -> Resp {
        Resp::File(bytes)
    }

    // |*|...els.len()...|\r|\n|...els...|
    pub fn array(els: Vec<Resp>) -> Resp {
        Resp::Array(els)
    }
}

fn parse_simple_string<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, s, _), rest) = and!(
        byte(b'+'),
        take_until(&[b'\r', b'\n']),
        tag(&[b'\r', b'\n'])
    )
    .parse(input)?;

    Ok((Some(Resp::SimpleString(s.to_vec())), rest))
}

fn parse_simple_error<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, e, _), rest) = and!(
        byte(b'-'),
        take_until(&[b'\r', b'\n']),
        tag(&[b'\r', b'\n'])
    )
    .parse(input)?;

    Ok((Some(Resp::SimpleError(e.to_vec())), rest))
}

fn parse_integer<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, i, _), rest) =
        and!(byte(b':'), integer::<i64>(), tag(&[b'\r', b'\n'])).parse(input)?;

    Ok((Some(Resp::Integer(i)), rest))
}

fn parse_bulk_string<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, l), rest) = and!(byte(b'$'), integer::<usize>()).parse(input)?;
    let ((_, s, _), rest) =
        and!(tag(&[b'\r', b'\n']), take(l), tag(&[b'\r', b'\n'])).parse(rest)?;

    Ok((Some(Resp::BulkString(s.to_vec())), rest))
}

fn parse_rdb_file<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, l), rest) = and!(byte(b'$'), integer::<usize>()).parse(input)?;
    let ((_, s), rest) = and!(tag(&[b'\r', b'\n']), take(l)).parse(rest)?;

    Ok((Some(Resp::File(s.to_vec())), rest))
}

fn parse_array<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    let ((_, l, _), rest) =
        and!(byte(b'*'), integer::<usize>(), tag(&[b'\r', b'\n'])).parse(input)?;

    let mut elements = Vec::new();
    let mut new_rest = rest;

    for _ in 0..l {
        let (el, rest) = parse_single_resp(new_rest)?;
        if let Some(resp) = el {
            new_rest = rest;
            elements.push(resp);
        } else {
            return Err(ParseError {
                message: format!(
                    "Error parsing Resp array, need {} elements, but reached end of input",
                    l
                ),
            });
        }
    }

    Ok((Some(Resp::Array(elements)), new_rest))
}

fn parse_single_resp<'a>(input: ParserInput<'a>) -> ParseResult<'a, Option<Resp>> {
    // No more input - no error
    if input.is_empty() {
        return Ok((None, input));
    }

    // Short circut for const Resp elements: NullBulkString and NullArray
    if input.starts_with(b"$-1\r\n") {
        return Ok((Some(Resp::null_bulk_string()), &input[5..]));
    }

    if input.starts_with(b"*-1\r\n") {
        return Ok((Some(Resp::null_array()), &input[5..]));
    }

    match input[0] {
        // Rdb file is encoded as same as bulk string - *but* it is missing ending '\r\n' bytes (2 bytes).
        // First match greedy for bulk string, and fallback to rdb file:
        b'$' => parse_bulk_string(input).or_else(|_| parse_rdb_file(input)),
        b'+' => parse_simple_string(input),
        b'-' => parse_simple_string(input),
        b'*' => parse_array(input),
        b':' => parse_integer(input),
        _ => Err(ParseError {
            message: format!("unknown RESP first byte: {}, input: {:?}", input[0], input),
        }),
    }
}

pub fn parse_resp<'a>(input: ParserInput<'a>) -> ParseResult<'a, Vec<Resp>> {
    let mut arrays = Vec::new();
    let mut next_input = input;
    loop {
        match parse_single_resp(next_input) {
            Ok((Some(array), new_input)) => {
                next_input = new_input;
                arrays.push(array);
            }
            Ok((None, new_input)) => {
                next_input = new_input;
                // No more input:
                break;
            }
            Err(e) => {
                panic!("Error parsing Resp: {:?}", e);
            }
        }
    }
    Ok((arrays, next_input))
}

// Encode
fn encode_resp(resp: &Resp) -> Vec<u8> {
    let mut out = Vec::new();

    match resp {
        Resp::NullBulkString => {
            out.extend_from_slice(b"$-1\r\n");
        }
        Resp::NullArray => {
            out.extend_from_slice(b"*-1\r\n");
        }
        Resp::SimpleString(s) => {
            out.extend_from_slice(b"+");
            out.extend_from_slice(s);
            out.extend_from_slice(b"\r\n");
        }
        Resp::BulkString(s) => {
            out.extend_from_slice(b"$");
            out.extend_from_slice(s.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(s);
            out.extend_from_slice(b"\r\n");
        }
        Resp::SimpleError(e) => {
            out.extend_from_slice(b"-");
            out.extend_from_slice(e);
            out.extend_from_slice(b"\r\n");
        }
        Resp::Integer(i) => {
            out.extend_from_slice(b":");
            out.extend_from_slice(i.to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        Resp::Array(els) => {
            out.extend_from_slice(b"*");
            out.extend_from_slice(els.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            for el in els {
                out.extend(encode_resp(el));
            }
        }
        Resp::File(f) => {
            out.extend_from_slice(b"$");
            out.extend_from_slice(f.len().to_string().as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(f);
        }
    }

    out
}
