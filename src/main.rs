use std::f32::consts::E;
use std::process::Command;
use std::ptr::null;

use anyhow::{Context, Error};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod decoder;

use decoder::PacketTypes;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let addr = "127.0.0.1:6379".to_string();

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        handle_client(socket);
    }
}

fn handle_client(mut stream: TcpStream) {
    println!("handling client ");

    tokio::spawn(async move {
        let mut buf = [0; 512];

        // In a loop, read data from the socket and write the data back.
        loop {
            let n = stream
                .read(&mut buf)
                .await
                .expect("failed to read data from socket");

            if n == 0 {
                return;
            }

            let (packet, _) = decoder::parse_message(&buf[..n]).unwrap();

            match (packet) {
                PacketTypes::Array(packets) => {
                    let packet1 = packets.get(0);
                    let packet2 = packets.get(1);
                    match (packet1, packet2) {
                        (
                            Some(PacketTypes::BulkString(bulk1)),
                            Some(PacketTypes::BulkString(bulk2)),
                        ) => {
                            let command = bulk1.as_str();
                            match command.to_uppercase().as_str() {
                                "ECHO" => {
                                    let res = PacketTypes::BulkString(bulk2.to_string());
                                    let echo = res.to_string();
                                    stream.write_all(echo.as_bytes()).await.unwrap();
                                }
                                _ => {
                                    println!("unsupported command {}", command);
                                }
                            }
                        }
                        (Some(PacketTypes::BulkString(bulk1)), None) => {
                            let command = bulk1.as_str();
                            match command.to_uppercase().as_str() {
                                "PING" => {
                                  let res = PacketTypes::SimpleString("PONG".to_string());
                                  let pong = res.to_string();
                                  stream.write_all(pong.as_bytes()).await.unwrap();
                                }
                                _ => {
                                    println!("unsupported command {}", command);
                                }
                            }
                        }
                        _ => {
                            anyhow::anyhow!("Invalid command");
                        }
                    }
                }
                _ => {
                    anyhow::anyhow!("Commands must be of type array");
                }
            }
        }
    });
}
