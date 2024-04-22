use crate::decoder::PacketTypes;

pub enum InfoSubCommand {
    Replication,
}

pub enum ReplconfCommand {
    GetAck(String),
    ListeningPort(String),
    Capa(String)
}

pub enum Command {
    Set {
        key: String,
        value: String,
        expire_time: Option<u64>,
    },
    Replconf(ReplconfCommand),
    Psync,
    Wait,
    Echo(String),
    Get(String),
    Info(InfoSubCommand),
    Ping,
    RDB(Vec<u8>),
    SimpleString(String),
}

impl TryFrom<PacketTypes> for Command {
    type Error = ();
    fn try_from(packet: PacketTypes) -> Result<Self, Self::Error> {
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
                                    (
                                        Some(PacketTypes::BulkString(bulk4)),
                                        Some(PacketTypes::BulkString(bulk5)),
                                    ) => {
                                        let commande = bulk4.as_str().to_uppercase();
                                        if commande.as_str() == "PX" {
                                            let expire_time =
                                                bulk5.as_str().parse::<u64>().unwrap();
                                            return Ok(Command::Set {
                                                value,
                                                key,
                                                expire_time: Some(expire_time),
                                            });
                                        } else {
                                            return Err(());
                                        }
                                    }
                                    _ => {
                                        return Ok(Command::Set {
                                            key,
                                            value,
                                            expire_time: None,
                                        })
                                    }
                                }
                            }
                            "REPLCONF" => {
                                let subcommand = bulk2.as_str();
                                let value = bulk3.as_str();
                                match subcommand {
                                    "GETACK" => {
                                        return Ok(Command::Replconf(ReplconfCommand::GetAck(
                                            value.to_string(),
                                        )));
                                    }
                                    "listening-port" => {
                                        return Ok(Command::Replconf(
                                            ReplconfCommand::ListeningPort(value.to_string()),
                                        ));
                                    },
                                    "capa" => {
                                        return Ok(Command::Replconf(
                                            ReplconfCommand::Capa(value.to_string()),
                                        ));
                                    }
                                    _ => {
                                        return Err(());
                                    }
                                }
                            }
                            "PSYNC" => return Ok(Command::Psync),
                            "WAIT" => return Ok(Command::Wait),
                            _ => return Err(()),
                        }
                    }
                    (
                        Some(PacketTypes::BulkString(bulk1)),
                        Some(PacketTypes::BulkString(bulk2)),
                        None,
                    ) => {
                        let command = bulk1.as_str();
                        match command.to_uppercase().as_str() {
                            "ECHO" => return Ok(Command::Echo(bulk2.to_string())),
                            "GET" => {
                                let key = bulk2.to_string();
                                return Ok(Command::Get(key));
                            }
                            "INFO" => {
                                let sub = bulk2.as_str();
                                let subcmd = match sub {
                                    "replication" => Some(InfoSubCommand::Replication),
                                    _ => None,
                                };
                                match subcmd {
                                    Some(sub) => return Ok(Command::Info(sub)),
                                    None => return Err(()),
                                }
                            }
                            _ => return Err(()),
                        }
                    }
                    (Some(PacketTypes::BulkString(bulk1)), None, None) => {
                        let command = bulk1.as_str();
                        match command.to_uppercase().as_str() {
                            "PING" => {
                                return Ok(Command::Ping);
                            }
                            _ => {
                                return Err(());
                            }
                        }
                    }
                    _ => return Err(()),
                }
            }
            PacketTypes::RDB(data) => return Ok(Command::RDB(data)),
            PacketTypes::SimpleString(str) => return Ok(Command::SimpleString(str)),
            _ => {
                return Err(());
            }
        }
    }
}
