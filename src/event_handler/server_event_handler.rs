use std::sync::Arc;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::Mutex,
};

#[derive(Clone)]
pub struct Connection {
    pub tcp_stream: Arc<Mutex<TcpStream>>,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Connection {
            tcp_stream: Arc::new(Mutex::new(stream)),
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
pub struct ServerHandler {
    pub connection: Connection,
    is_replication_client: bool,
}

impl ServerHandler {
    pub fn new(stream: TcpStream) -> Self {
        return Self {
            connection: Connection::new(stream),
            is_replication_client: false,
        };
    }
}
