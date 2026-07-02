use std::{
    fmt,
    fmt::Debug,
    str::{FromStr, from_utf8},
};

#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Parse error: {}", self.message)
    }
}

///
/// Parser input type is slice of bytes:
pub type ParserInput<'a> = &'a [u8];
///
pub type ParseResult<'a, T> = Result<(T, ParserInput<'a>), ParseError>;
///
pub trait Parser<'a, T> {
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
/// Read byte `b`.
pub fn byte<'a>(b: u8) -> impl Parser<'a, u8> {
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
/// Read `tag` bytes.
pub fn tag<'a>(expected: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        // println!("[tag] input: {:?}", input);
        // println!("[tag] expected: {:?}", expected);
        if input.starts_with(expected) {
            Ok(input.split_at(expected.len()))
        } else {
            Err(ParseError {
                message: format!("[tag] no tag: {:?} found", expected),
            })
        }
    }
}

pub fn tag_no_case<'a>(expected: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
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
pub fn take<'a>(n: usize) -> impl Parser<'a, &'a [u8]> {
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
pub fn take_while<'a>(pred: impl Fn(u8) -> bool) -> impl Parser<'a, &'a [u8]> {
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
pub fn take_until<'a>(delimiter: &'static [u8]) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let mut i = 0;
        while i <= input.len() - delimiter.len() {
            if input[i..].starts_with(delimiter) {
                //let x = input.split_at(i); // debug
                // println!("[take_until] 0: {:?}", x.0);
                // println!("[take_until] 1: {:?}", x.1);
                return Ok(input.split_at(i));
            }
            i += 1;
        }

        Err(ParseError {
            message: format!(
                "expected to match limit: {:?}, but haven't: input: {:?}",
                delimiter, input
            ),
        })
    }
}

#[macro_export]
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
pub(crate) use and;

#[macro_export]
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
pub(crate) use or;

/// `opt` combinator, it always succeeds. If it matches input is advanced.
pub fn opt<'a, T: Debug>(p: impl Parser<'a, T>) -> impl Parser<'a, Option<T>> {
    move |input: ParserInput<'a>| match p.parse(input) {
        Ok((result, rest)) => Ok((Some(result), rest)),
        _ => Ok((None, input)),
    }
}

pub fn recognize<'a, T>(p: impl Parser<'a, T>) -> impl Parser<'a, &'a [u8]> {
    move |input: ParserInput<'a>| {
        let (_, rest) = p.parse(input)?;
        let len = input.len() - rest.len();
        Ok((&input[..len], rest))
    }
}

pub fn integer<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr,
{
    move |input: ParserInput<'a>| {
        let digits = || recognize(take_while(|b| b.is_ascii_digit()));
        let sign = || recognize(or!(byte(b'-'), byte(b'+')));
        let number = recognize(and!(opt(sign()), digits()));
        let (bytes, rest) = number.parse(input)?;
        let string = from_utf8(bytes).unwrap();
        let n = match string.parse::<T>() {
            Ok(v) => Ok(v),
            _ => Err(ParseError {
                message: format!("[integer] cannot parse from string: {}", string),
            }),
        }?;
        Ok((n, rest))
    }
}

pub fn float<'a, T>() -> impl Parser<'a, T>
where
    T: FromStr,
{
    move |input: ParserInput<'a>| {
        let digits = || recognize(take_while(|b| b.is_ascii_digit()));
        let sign = || recognize(or!(byte(b'-'), byte(b'+')));
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
