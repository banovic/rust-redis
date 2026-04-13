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
                let mut buffer: Vec<u8> = Vec::new();

                loop {
                    let _ = _stream.read_to_end(&mut buffer);
                    println!("Received line: {:?}", buffer);
                    let _ = _stream.write(b"+PONG\r\n");
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
