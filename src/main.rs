use anyhow::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[tokio::main]
async fn main()-> Result<(),Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let addr =  "127.0.0.1:8080".to_string();

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (mut socket, _) = listener.accept().await?;
        
    }
}


fn handle_client(mut stream: TcpStream) {
    println!("handling client ");

    tokio::spawn(async move {
        let mut buf = [0; 512];
        let pong = "+PONG\r\n";

        // In a loop, read data from the socket and write the data back.
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .expect("failed to read data from socket");

            if n == 0 {
                return;
            }

            stream
                .write_all(pong.as_bytes())
                .await
                .expect("failed to write data to socket");
        }
    });
}