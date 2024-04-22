

use crate::Server;
use anyhow::anyhow;
use anyhow::Error;
use anyhow::Result;

#[derive(Debug, Clone)]
pub enum PacketTypes {
    SimpleString(String),
    BulkString(String),
    NullBulkString,
    Array(Vec<PacketTypes>),
    RDB(Vec<u8>),
}

pub struct Parser {
    buffer: Vec<u8>,
    pos: usize,
}

impl Parser {
    pub fn new(buffer: Vec<u8>) -> Self {
        Parser { buffer, pos: 0 }
    }

    fn read_exact(&mut self, size: usize) -> Result<&[u8]> {
        if self.buffer.len() < self.pos + size {
            return Err(anyhow!("Buffer too small"));
        }
        let data = &self.buffer[self.pos..self.pos + size];
        self.pos += size;
        return Ok(data);
    }

    fn read_until_crlf(&mut self) -> Result<&[u8]> {
        let mut i = self.pos;
        while i < self.buffer.len() {
            if self.buffer[i - 1] == b'\r' && self.buffer[i] == b'\n' {
                let data = &self.buffer[self.pos..(i - 1)];
                self.pos = i + 1;
                return Ok(data);
            }
            i += 1;
        }
        return Err(anyhow!("Can't find CRLF"));
    }

    fn is_done_parsing(&self) -> bool {
        return self.pos == self.buffer.len();
    }

    fn get_left_to_read(&self) -> usize {
        return self.buffer.len() - self.pos;
    }

    fn peek_ahead(&self, n: usize) -> u8 {
        return self.buffer[self.pos + n];
    }

    // parsing
    fn parse_simple_string(&mut self) -> Result<PacketTypes, Error> {
        if let Ok(simple_string) = self.read_until_crlf() {
            let simple_string = std::str::from_utf8(simple_string)?.to_string();
            return Ok(PacketTypes::SimpleString(simple_string));
        } else {
            return Err(anyhow::anyhow!("Can't parse simple string"));
        }
    }

    fn parse_bulk_string(&mut self) -> Result<PacketTypes, Error> {
        let bulk_str_len = if let Ok(bulk_str_len) = self.read_until_crlf() {
            let bulk_str_len = std::str::from_utf8(bulk_str_len)?.parse::<usize>()?;
            bulk_str_len
        } else {
            return Err(anyhow::anyhow!("Invalid command"));
        };
    
        // let end_of_bulk_str = pos + bulk_str_len;
        let left_to_read = self.get_left_to_read();
        if left_to_read == bulk_str_len {
            let data = self.read_exact(bulk_str_len)?.to_vec();
            return Ok(PacketTypes::RDB(data));
        }
    
        if self.peek_ahead(bulk_str_len) == b'\r' {
            let string_bytes = self.read_exact(bulk_str_len)?;
            let string = std::str::from_utf8(string_bytes)?.to_string();
            self.read_exact(2)?;
            return Ok(PacketTypes::BulkString(string));
        } else {
            let data = self.read_exact(bulk_str_len)?.to_vec();
            return Ok(PacketTypes::RDB(data));
        }
    }

    fn parse_array(&mut self) -> Result<PacketTypes, Error> {
        let array_len = if let Ok(array_len_bytes) = self.read_until_crlf() {
            let array_len_str = std::str::from_utf8(array_len_bytes)?;
            let array_len = array_len_str.parse::<usize>()?;
            array_len
        } else {
            return Err(anyhow::anyhow!("can't read to crlf"));
        };
    
        let mut array = Vec::new();
        for _ in 0..array_len {
            let packet = self.parse_packet()?;
            array.push(packet);
        }
    
        return Ok(PacketTypes::Array(array));
    }

    fn parse_packet(&mut self) -> Result<PacketTypes, Error> {
        let first_char = self.read_exact(1)?[0];
        match first_char as char {
            '+' => {
                return self.parse_simple_string();
            }
            '$' => {
                return self.parse_bulk_string();
            }
            '*' => {
                return self.parse_array();
            }
            _ => {
                return Err(anyhow::anyhow!("Invalid start {}", first_char));
            }
        }
    }

    pub fn parse(&mut self) -> Result<Vec<PacketTypes>> {
        let mut packets: Vec<PacketTypes> = vec![];
        while !self.is_done_parsing() {
            if let Ok(packet) = self.parse_packet() {
                packets.push(packet);
            } else {
                return Err(anyhow!("Error parsing"));
            }
        }
        Ok(packets)
    }
}

impl PacketTypes {
    pub fn new_replication_info(server: &Server) -> Self {
        let mut text = format!("role:{}\n", server.role.to_string());
        let master_replid = match &server.master_replid {
            Some(replid) => replid,
            None => &server.replid,
        };
        text.push_str(&format!("master_replid:{}\n", master_replid));
        text.push_str(format!("master_repl_offset:{}\n", '0').as_str());
        let packet = PacketTypes::SimpleString(text);
        return packet;
    }

    

    pub fn to_string(&self) -> String {
        match self {
            PacketTypes::SimpleString(s) => format!("+{}\r\n", s),
            PacketTypes::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            PacketTypes::Array(a) => {
                let mut result = String::from("*");
                result.push_str(&a.len().to_string());
                result.push_str("\r\n");
                for packet in a {
                    result.push_str(&packet.to_string());
                }
                result
            }
            PacketTypes::NullBulkString => "$-1\r\n".to_string(),
            PacketTypes::RDB(rdb) => {
                let mut result = String::from("$");
                result.push_str(&rdb.len().to_string());
                result.push_str("\r\n");
                result
            }
        }
    }
}






