use std::collections::HashSet;

use std::sync::Arc;
use tokio::sync::broadcast::{Receiver, Sender};
use tokio::sync::{broadcast, Mutex};

use anyhow::Error;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::decoder::{self, PacketTypes};
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
    pub slaves: Arc<Mutex<Vec<String>>>,
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
        let slaves = Arc::new(Mutex::new(Vec::new()));
        let pending_updates = Arc::new(Mutex::new(HashSet::<(String, String)>::new()));
        let _shared_pending_updates = Arc::clone(&pending_updates);
        Server {
            role,
            port,
            host,
            replicat_of,
            replid,
            master_replid,
            slaves,
        }
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
}

pub async fn init_server(store: &Arc<Store>, server: &Arc<Server>) -> Result<(), Error> {
    let addr = format!("{}:{}", &server.host, &server.port);

    let listener = TcpListener::bind(&addr).await?;
    let (replication_tx, _replication_rx) = broadcast::channel::<String>(10);
    loop {
        let (socket, _) = listener.accept().await?;
        let replication_tx = replication_tx.clone();
        let replication_rx = replication_tx.subscribe();
        handle_client(socket, store, &server, replication_tx, replication_rx).await;
    }
}

#[derive(Clone)]
pub struct Connection {
    pub tcp_stream: Arc<Mutex<TcpStream>>,
    pub stream_id: String,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        let stream_id = stream.peer_addr().unwrap().to_string().clone();
        Connection {
            tcp_stream: Arc::new(Mutex::new(stream)),
            stream_id: stream_id,
        }
    }
    pub async fn send_data(&self, data: &[u8]) {
        let stream = &mut self.tcp_stream.lock().await;
        stream.write_all(data).await.unwrap();
    }

    pub async fn read_data(&self, data: &mut [u8; 512]) -> Result<usize, std::io::Error> {
        let stream = &mut self.tcp_stream.lock().await;
        return stream.read(data).await;
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
        let conn = Connection::new(stream);
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
                                            // raw command string from buf
                                            let raw_command =
                                                std::str::from_utf8(&buf[..n]).unwrap().to_string();
                                            replication_tx.send(raw_command).unwrap();
                                            // add to pending updates
                                            let server = Arc::clone(&server);

                                            let key = bulk2.to_string();
                                            let value = bulk3.to_string();

                                            let cmd2 = packets.get(3);
                                            let cmd3 = packets.get(4);

                                            match (cmd2, cmd3) {
                                                (
                                                    Some(PacketTypes::BulkString(bulk4)),
                                                    Some(PacketTypes::BulkString(bulk5)),
                                                ) => {
                                                    let commande = bulk4.as_str().to_uppercase();
                                                    match commande.as_str() {
                                                        "PX" => {
                                                            let expire_time = bulk5
                                                                .as_str()
                                                                .parse::<u64>()
                                                                .unwrap();
                                                            println!("debug: setting key {} with value {} and expiry {}", key, value,expire_time);
                                                            store.set_with_expiry(
                                                                key,
                                                                value,
                                                                expire_time,
                                                            );
                                                            server.send_ok(&conn).await.unwrap();
                                                        }
                                                        _ => {
                                                            println!(
                                                            "unsupported sub commande {} for SET",
                                                            commande
                                                        );
                                                        }
                                                    }
                                                }
                                                _ => {
                                                    println!(
                                                        "debug: setting key {} with value {}",
                                                        key, value
                                                    );
                                                    store.set(key, value);
                                                    server.send_ok(&conn).await.unwrap();
                                                }
                                            }
                                        }
                                        "REPLCONF" => {
                                            server.send_ok(&conn).await.unwrap();
                                        }
                                        "PSYNC" => {
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
                                        }
                                        _ => {
                                            println!("unsupported command {}", commande);
                                        }
                                    }
                                }
                                (
                                    Some(PacketTypes::BulkString(bulk1)),
                                    Some(PacketTypes::BulkString(bulk2)),
                                    None,
                                ) => {
                                    let command = bulk1.as_str();
                                    match command.to_uppercase().as_str() {
                                        "ECHO" => {
                                            server
                                                .send_bulk_string(&conn, &bulk2.to_string())
                                                .await
                                                .unwrap();
                                        }
                                        "GET" => {
                                            let key = bulk2.to_string();
                                            let value = store.get(key);
                                            if let Some(value) = value {
                                                server
                                                    .send_bulk_string(&conn, &value.value)
                                                    .await
                                                    .unwrap();
                                            } else {
                                                server.send_null_string(&conn).await.unwrap();
                                            }
                                        }
                                        "INFO" => {
                                            let sub = bulk2.as_str();
                                            match sub {
                                                "replication" => {
                                                    server
                                                        .send_replication_info(&conn)
                                                        .await
                                                        .unwrap();
                                                }
                                                _ => {
                                                    println!(
                                                        "unsupported sub command for INFO : {}",
                                                        command
                                                    );
                                                }
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
                                            server.send_pong(&conn).await.unwrap();
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
