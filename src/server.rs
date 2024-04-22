use std::collections::HashSet;

use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, Mutex};

use anyhow::Error;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

use crate::commade::{Command, InfoSubCommand};
use crate::decoder::{PacketTypes, Parser};
use crate::event_handler::server_event_handler::{Connection, ServerHandler};
use crate::rdb::{get_rdb_bytes, EMPTY_RDB_FILE};
use crate::store::Store;

#[derive(Debug, PartialEq)]
pub enum ServerRole {
    Master,
    Slave,
}

impl ServerRole {
    pub fn to_string(&self) -> String {
        match self {
            ServerRole::Master => "master".to_string(),
            ServerRole::Slave => "slave".to_string(),
        }
    }
}

pub struct Server {
    pub role: ServerRole,
    pub port: String,
    pub host: String,
    pub replicat_of: Option<(String, String)>,
    pub replid: String,
    pub master_replid: Option<String>,
    pub number_of_replication: Arc<Mutex<u64>>,
}

impl Server {
    pub fn new(
        role: ServerRole,
        port: String,
        host: String,
        replicat_of: Option<(String, String)>,
        master_replid: Option<String>,
    ) -> Self {
        let replid: String = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(40)
            .map(char::from)
            .collect();
        let pending_updates = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));
        let _shared_pending_updates = Arc::clone(&pending_updates);
        let number_of_replication = Arc::new(Mutex::new(0));
        Server {
            role,
            port,
            host,
            replicat_of,
            replid,
            master_replid,
            number_of_replication,
        }
    }

    pub async fn add_replication(&self) {
        let mut number_of_replication = self.number_of_replication.lock().await;
        *number_of_replication += 1;
    }

    pub async fn send_empty_rdb(&self, conn: &Connection) -> Result<(), Error> {
        let res = PacketTypes::RDB(get_rdb_bytes().to_vec());
        let res = res.to_string();
        conn.send_data([res.as_bytes(), &EMPTY_RDB_FILE].concat().as_slice())
            .await;
        Ok(())
    }

    pub async fn send_replication_info(&self, conn: &Connection) -> Result<(), Error> {
        let packet = PacketTypes::new_replication_info(self);
        let info = packet.to_string();
        conn.send_data(info.as_bytes()).await;
        Ok(())
    }

    pub async fn send_ok(&self, conn: &Connection) -> Result<(), Error> {
        let res = PacketTypes::SimpleString("OK".to_string());
        let ok = res.to_string();
        conn.send_data(ok.as_bytes()).await;
        Ok(())
    }

    async fn send_pong(&self, conn: &Connection) -> Result<(), Error> {
        let res = PacketTypes::SimpleString("PONG".to_string());
        let pong = res.to_string();
        conn.send_data(pong.as_bytes()).await;
        Ok(())
    }

    async fn send_bulk_string(&self, conn: &Connection, value: &String) -> Result<(), Error> {
        let res = PacketTypes::BulkString(value.to_string());
        let res = res.to_string();
        conn.send_data(res.as_bytes()).await;
        Ok(())
    }

    async fn send_null_string(&self, conn: &Connection) -> Result<(), Error> {
        let res = PacketTypes::NullBulkString;
        let res = res.to_string();
        conn.send_data(res.as_bytes()).await;
        Ok(())
    }

    pub async fn send_simple_string(&self, conn: &Connection, value: &String) -> Result<(), Error> {
        let res = PacketTypes::SimpleString(value.to_string());
        let res = res.to_string();
        conn.send_data(res.as_bytes()).await;
        Ok(())
    }

    pub async fn send_integer(&self, conn: &Connection, value: i64) -> Result<(), Error> {
        let res = PacketTypes::Integer(value);
        let res = res.to_string();
        conn.send_data(res.as_bytes()).await;
        Ok(())
    }

    pub async fn send_number_of_replications(&self, conn: &Connection) -> Result<(), Error> {
        let number_of_replication = self.number_of_replication.lock().await;
        self.send_integer(conn, *number_of_replication as i64)
            .await
            .unwrap();
        Ok(())
    }
}

pub async fn init_server(store: &Arc<Store>, server: &Arc<Server>) -> Result<(), Error> {
    let addr = format!("{}:{}", &server.host, &server.port);

    let listener = TcpListener::bind(&addr).await?;
    let (replication_tx, _replication_rx) = broadcast::channel::<String>(1);
    loop {
        let (socket, _) = listener.accept().await?;
        let replication_tx = replication_tx.clone();
        let replication_rx = replication_tx.subscribe();
        handle_client(socket, store, &server, replication_tx, replication_rx).await;
    }
}

async fn handle_client(
    stream: TcpStream,
    store: &Arc<Store>,
    server: &Arc<Server>,
    replication_tx: Sender<String>,
    mut replication_rx: Receiver<String>,
) {
    let store = Arc::clone(store);
    let server = Arc::clone(&server);

    tokio::spawn(async move {
        let mut buf = [0; 512];
        let server_handler = ServerHandler::new(stream);
        let conn = server_handler.connection;
        let mut is_replication = false;
        // In a loop, read data from the socket and write the data back.
        loop {
            tokio::select! {
                res = conn.read_data(&mut buf) =>
                {

                    let n = match res {
                        Ok(n) => n,
                        Err(_) => 12012000 as usize,
                    };

                    if n == 12012000 {
                        continue;
                    }

                    if n == 0 {
                        return;
                    }

                    let mut parser = Parser::new(buf[..n].to_vec());
                    let packets = parser.parse().unwrap();
                    for packet in packets {
                    let commande = Command::try_from(packet).unwrap();

                    match commande {
                        Command::Set {
                            key,
                            value,
                            expire_time,
                        } => {
                            let raw_command =std::str::from_utf8(&buf[..n]).unwrap().to_string();
                            replication_tx.send(raw_command).unwrap();
                            if let Some(expiration_time) = expire_time {
                                store.set_with_expiry(key, value, expiration_time);
                                server.send_ok(&conn).await.unwrap();
                            } else {
                                store.set(key, value);
                                server.send_ok(&conn).await.unwrap();
                            }
                        },
                        Command::Get(key) => {
                            let value = store.get(&key);
                            if let Some(value) = value {
                                server
                                    .send_bulk_string(&conn, &value.value)
                                    .await
                                    .unwrap();
                            } else {
                                println!("Debug: Key {} not found.", &key);
                                server.send_null_string(&conn).await.unwrap();
                            }
                        },
                        Command::Replconf(_) => {
                           server.send_ok(&conn).await.unwrap();
                        },
                        Command::Psync => {
                            server
                                .send_simple_string(
                                    &conn,
                                    &format!("+FULLRESYNC {} 0", server.replid)
                                        .to_string(),
                                )
                                .await
                                .unwrap();
                            let server = Arc::clone(&server);
                            server.send_empty_rdb(&conn).await.unwrap();
                            is_replication = true;
                            server.add_replication().await;
                        },
                        Command::Wait => {
                            server.send_number_of_replications(&conn).await.unwrap();
                        },
                        Command::Echo(str) => {
                            server
                                .send_bulk_string(&conn, &str)
                                .await
                                .unwrap()
                        },
                        Command::Info(info_cmd) => {
                            match info_cmd  {
                                InfoSubCommand::Replication => {
                                    server
                                        .send_replication_info(&conn)
                                        .await
                                        .unwrap();
                                },
                                _ => {
                                    println!("unsupported sub command for INFO");
                                }
                            }
                        },
                        Command::Ping => {
                            server.send_pong(&conn).await.unwrap();
                        }
                        _ => { todo!( )}
                    }
                }
            }
            res = replication_rx.recv() => {
                // log
                match res {
                    Ok(res) => {
                        println!("Received replication command");
                        if is_replication {
                            conn.send_data(res.as_bytes()).await;
                        }
                    },
                    Err(_) => {
                        println!("Error receiving replication command");
                    }
                }
            }
            }
        }
    });
}
