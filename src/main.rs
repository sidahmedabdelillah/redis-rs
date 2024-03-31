use std::{io::Write, net::TcpListener, net::TcpStream};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                handle_client(_stream);
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}


fn handle_client(mut stream: TcpStream) {
    loop {
        let pong = "+PONG\r\n";
        stream.write_all(pong.as_bytes()).unwrap();
    }
}