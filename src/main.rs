#![allow(unused_imports)]
use std::{io::{BufRead, BufReader, Read, Write}, net::TcpListener};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");
                let reader_half = _stream.try_clone().unwrap();
                let mut reader = BufReader::new(reader_half);
                let mut line = String::new();

                while let Ok(n) = reader.read_line(&mut line) {
                    if n == 0 {
                        break;
                    }
                    println!("Received line: {}", line);

                    let _ = _stream.write(b"+PONG\r\n");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
