use anyhow::Error;

use crate::Server;

#[derive(Debug)]
pub enum PacketTypes {
    SimpleString(String),
    INTEGER(usize),
    BulkString(String),
    NullBulkString,
    Array(Vec<PacketTypes>),
}

pub enum ComandTypes {
    PING,
    ECHO,
}

impl PacketTypes {
    pub fn new_replication_info(server: &Server) -> Self{
        let text = format!("role:{}\n", server.role.to_string());
        let packet = PacketTypes::SimpleString(text);
        return packet;
    }

    pub fn parse(buffer: &[u8]) -> Result<(Self, usize), Error> {
        match buffer[0] as char {
            '+' => {
                return parse_simple_string(buffer);
            }
            '$' => {
                return parse_bulk_string(buffer);
            }
            '*' => {
                return parse_array(buffer);
            }
            _ => {
                println!("Invalid command {}", buffer[0] as char);
                return Err(anyhow::anyhow!("Invalid command {}", buffer[0]));
            }
        }
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
            },
            PacketTypes::NullBulkString => "$-1\r\n".to_string(),
            _ => {
                println!("Invalid command");
                return String::from("");
            },
        }
    }
}

fn parse_simple_string(buffer: &[u8]) -> Result<(PacketTypes, usize), Error> {
    if let Some((simple_string, pos)) = read_until_crlf(&buffer[1..]) {
        let simple_string = std::str::from_utf8(simple_string)?.to_string();
        return Ok((PacketTypes::SimpleString(simple_string), pos));
    } else {
        return Err(anyhow::anyhow!("Can't parse simple string"));
    }
}

fn parse_bulk_string(buffer: &[u8]) -> Result<(PacketTypes, usize), Error> {
    let (bulk_str_len, pos) = if let Some((bulk_str_len, pos)) = read_until_crlf(&buffer[1..]) {
        let bulk_str_len = std::str::from_utf8(bulk_str_len)?.parse::<usize>()?;
        (bulk_str_len, pos)
    } else {
        return Err(anyhow::anyhow!("Invalid command"));
    };

    let end_of_bulk_str = pos + bulk_str_len;

    let string = std::str::from_utf8(&buffer[pos..end_of_bulk_str]).unwrap();

    let total_parsed = end_of_bulk_str + 2;
    return Ok((PacketTypes::BulkString(string.to_string()), total_parsed));
}

fn parse_array(buffer: &[u8]) -> Result<(PacketTypes, usize), Error> {
    let (array_len, mut pos) = if let Some((array_len, pos)) = read_until_crlf(&buffer[1..]) {
        let array_len_str = std::str::from_utf8(array_len)?;
        let array_len = array_len_str.parse::<usize>()?;
        (array_len, pos)
    } else {
        println!("can't read to crlf");
        return Err(anyhow::anyhow!("Invalid command"));
    };

    let mut array = Vec::new();
    for _ in 0..array_len {
        let (packet, len) = PacketTypes::parse(&buffer[pos..])?;
        array.push(packet);
        pos += len;
    }

    return Ok((PacketTypes::Array(array), pos));
}

pub fn parse_message(buffer: &[u8]) -> Result<(PacketTypes, usize), anyhow::Error> {
    return PacketTypes::parse(buffer);
}

fn read_until_crlf(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 2 as usize));
        }
    }

    return None;
}
