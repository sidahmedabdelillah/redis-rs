use std::sync::Arc;

use anyhow::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod decoder;
mod store;

use decoder::PacketTypes;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let addr = "127.0.0.1:6379".to_string();
    let store = Arc::new(store::Store::new());

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind(&addr).await?;

    loop {
        let (socket, _) = listener.accept().await?;
        handle_client(socket, &store);
    }
}

fn handle_client(mut stream: TcpStream, store: &Arc<store::Store>)  {
    println!("handling client ");
    let store = Arc::clone(store);
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

            match packet {
                PacketTypes::Array(packets) => {
                    let packet1 = packets.get(0);
                    let packet2 = packets.get(1);
                    let packet3 = packets.get(2);

                    match (packet1, packet2, packet3) {
                        (
                            Some(PacketTypes::BulkString(bulk1)),
                            Some(PacketTypes::BulkString(bulk2)),
                            Some(PacketTypes::BulkString(bulk3)),
                        ) => {
                          let commande = bulk1.as_str().to_uppercase(); 
                          match commande.as_str() {
                            "SET" => {
                                let key = bulk2.to_string();
                                let value = bulk3.to_string();

                                let cmd2 = packets.get(3);
                                let cmd3 = packets.get(4);

                                match (cmd2, cmd3) {
                                    (Some(PacketTypes::BulkString(bulk4)), Some(PacketTypes::BulkString(bulk5))) => {
                                      let commande = bulk4.as_str().to_uppercase(); 
                                      match commande.as_str() {
                                        "PX" => {
                                          let expire_time = bulk5.as_str().parse::<u64>().unwrap();
                                          store.set_with_expiry(key, value, expire_time);
                                          let res = PacketTypes::SimpleString("OK".to_string());
                                          let ok = res.to_string();
                                          stream.write_all(ok.as_bytes()).await.unwrap();
                                        },
                                        _ => {
                                          println!("unsupported sub commande {} for SET", commande);
                                        }
                                      }
                                    },
                                    _ => {
                                        store.set(key, value);
                                        let res = PacketTypes::SimpleString("OK".to_string());
                                        let ok = res.to_string();
                                        stream.write_all(ok.as_bytes()).await.unwrap();
                                    }
                                }
                            },
                            _ => {
                                println!("unsupported command {}", commande);
                            }
                        }},
                        (
                            Some(PacketTypes::BulkString(bulk1)),
                            Some(PacketTypes::BulkString(bulk2)),
                            None,
                        ) => {
                            let command = bulk1.as_str();
                            match command.to_uppercase().as_str() {
                                "ECHO" => {
                                    let res = PacketTypes::BulkString(bulk2.to_string());
                                    let echo = res.to_string();
                                    stream.write_all(echo.as_bytes()).await.unwrap();
                                },
                                "GET" => {
                                  let key = bulk2.to_string();
                                  let value = store.get(key);
                                  if let Some(value) = value {
                                    let res = PacketTypes::BulkString(value.value);
                                    let res = res.to_string();
                                    stream.write_all(res.as_bytes()).await.unwrap();
                                  }else {
                                    let res = PacketTypes::NullBulkString;
                                    let res = res.to_string();
                                    stream.write_all(res.as_bytes()).await.unwrap();
                                  }
                                }
                                _ => {
                                    println!("unsupported command {}", command);
                                }
                            }
                        }
                        (Some(PacketTypes::BulkString(bulk1)), None, None) => {
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
                            panic!("Commands must be of type array");
                        }
                    }
                }
                _ => {
                    panic!("Commands must be of type array");
                }
            }
        }
    });
}
