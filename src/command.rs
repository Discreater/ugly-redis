use std::{num::ParseIntError, time::Duration, u64, vec};
use thiserror::Error;
use tracing::{error, trace};

use crate::{
    db::{EntryId, StreamEntry, Value},
    message::Message,
};

type ReplId = String;
type ReplOffset = usize;

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ReqCommand {
    Ping,
    Echo(String),
    Set {
        key: String,
        value: String,
        px: Option<Duration>,
    },
    Get(String),
    KEYS(String),
    Config(ConfigSubCommand),
    Info(Option<String>),
    Replconf(ReplconfSubcommand),
    Psync {
        id: Option<ReplId>,
        offset: Option<ReplOffset>,
    },
    Wait {
        number_replicas: usize,
        time_out: usize,
    },
    Type(String),
    XADD {
        stream_key: String,
        entry_id: String,
        pairs: Vec<(String, String)>,
    },
    XRANGE {
        stream_key: String,
        /// inclusive
        start_id: EntryId,
        /// inclusive
        end_id: EntryId,
    },
    XREAD {
        block_time: Option<usize>,
        streams: Vec<XReadItemRaw>,
    },
    Incr(String),
    Multi,
}

#[derive(Debug, Clone)]
pub enum ReplconfSubcommand {
    ListeningPort(u16),
    Capa(String),
    Getack,
}

#[derive(Debug, Clone)]
pub struct XReadItemRaw {
    pub stream_key: String,
    /// exclusive
    pub start: String,
}

#[derive(Debug, Clone)]
pub struct XReadItem {
    pub stream_key: String,
    /// exclusive
    pub start: EntryId,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RespCommand {
    Pong,
    Ok,
    Replconf(ReplConfSubresponse),
    FullResync { repl_id: String, offset: usize },
    RdbFile(Vec<u8>),
    Bulk(String),
    Bulks(Vec<Option<String>>),
    Nil,
    Simple(&'static str),
    Int(i64),
    StreamEntries(Vec<StreamEntry>),
}

impl From<Value> for RespCommand {
    fn from(value: Value) -> Self {
        match value {
            Value::String(s) => RespCommand::Bulk(s),
            Value::Integer(i) => RespCommand::Bulk(i.to_string()),
            _ => unimplemented!(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ReplConfSubresponse {
    Ack(usize),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConfigSubCommand {
    GET(String),
}

#[derive(Error, Debug)]
pub enum ParseMessageError {
    #[error("Unexpected eof, require: {0}")]
    UnexpectedEof(String),
    #[error("Expect String, but get: {0:?}")]
    ExpectString(Message),
    #[error("Unsupported: {0}")]
    Unsupported(String),
}

impl From<Message> for ParseMessageError {
    fn from(message: Message) -> ParseMessageError {
        ParseMessageError::ExpectString(message)
    }
}

impl From<&'_ Message> for ParseMessageError {
    fn from(message: &Message) -> ParseMessageError {
        ParseMessageError::ExpectString(message.clone())
    }
}

impl From<ParseIntError> for ParseMessageError {
    fn from(value: ParseIntError) -> Self {
        ParseMessageError::Unsupported(value.to_string())
    }
}

impl ParseMessageError {
    pub(crate) fn expect(s: impl Into<String>) -> Self {
        Self::UnexpectedEof(s.into())
    }

    pub(crate) fn unsupported(s: impl Into<String>) -> Self {
        Self::Unsupported(s.into())
    }
}

impl ReqCommand {
    pub(crate) fn parse(messages: Vec<Message>) -> Result<Self, ParseMessageError> {
        let mut messages = messages.into_iter();
        let message = messages
            .next()
            .ok_or_else(|| ParseMessageError::expect("messages"))?;
        match message.get_string() {
            Ok(data) => match data.to_uppercase().as_str() {
                "PING" => Ok(ReqCommand::Ping),
                "ECHO" => {
                    let message = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("ECHO message"))?;
                    let data = message.get_string()?;
                    Ok(ReqCommand::Echo(data))
                }
                "SET" => {
                    let key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("SET key"))?
                        .get_string()?;
                    let value = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("SET value"))?
                        .get_string()?;
                    let mut px = None;
                    while let Some(message) = messages.next() {
                        match message.get_string() {
                            Ok(option) => match option.to_uppercase().as_str() {
                                "PX" => {
                                    let data = messages
                                        .next()
                                        .ok_or_else(|| ParseMessageError::expect("PX value"))?
                                        .get_string()?;
                                    match data.parse::<u64>() {
                                        Ok(ms) => {
                                            px = Some(Duration::from_millis(ms));
                                        }
                                        Err(_) => {
                                            error!("Set command has invalid PX value: {}", data)
                                        }
                                    }
                                }
                                s => {
                                    error!("Set command has invalid option: {}", s)
                                }
                            },
                            Err(message) => {
                                error!("SET command has invalid message: {:?}", message);
                            }
                        }
                    }
                    Ok(ReqCommand::Set { key, value, px })
                }
                "GET" => {
                    let key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("GET key"))?
                        .get_string()?;
                    Ok(ReqCommand::Get(key))
                }
                "KEYS" => {
                    let pattern = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("KEYS patter"))?
                        .get_string()?;
                    Ok(ReqCommand::KEYS(pattern))
                }
                "CONFIG" => {
                    let sub_command = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("CONFIG sub command"))?
                        .get_string()?;
                    match sub_command.to_uppercase().as_str() {
                        "GET" => {
                            let key = messages
                                .next()
                                .ok_or_else(|| ParseMessageError::expect("CONFIG GET key"))?
                                .get_string()?;
                            Ok(ReqCommand::Config(ConfigSubCommand::GET(key)))
                        }
                        _ => Err(ParseMessageError::unsupported(format!(
                            "sub command: {:?}",
                            sub_command
                        ))),
                    }
                }
                "INFO" => {
                    if let Some(section) = messages.next() {
                        let section = section.get_string()?;
                        Ok(ReqCommand::Info(Some(section)))
                    } else {
                        Ok(ReqCommand::Info(None))
                    }
                }
                "REPLCONF" => {
                    let sub_command = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("REPLCONF sub command"))?
                        .get_string()?;
                    match sub_command.to_uppercase().as_str() {
                        "LISTENING-PORT" => {
                            let port = messages
                                .next()
                                .ok_or_else(|| {
                                    ParseMessageError::expect("REPLCONF LISTENING-PORT value")
                                })?
                                .get_string()?
                                .parse()?;
                            Ok(ReqCommand::Replconf(ReplconfSubcommand::ListeningPort(
                                port,
                            )))
                        }
                        "CAPA" => {
                            let capa = messages
                                .next()
                                .ok_or_else(|| ParseMessageError::expect("REPLCONF CAPA value"))?
                                .get_string()?;
                            Ok(ReqCommand::Replconf(ReplconfSubcommand::Capa(capa)))
                        }
                        "GETACK" => {
                            let _star = messages
                                .next()
                                .ok_or_else(|| ParseMessageError::expect("REPLCONF GETACK value"))?
                                .get_string()?;
                            Ok(ReqCommand::Replconf(ReplconfSubcommand::Getack))
                        }
                        _ => Err(ParseMessageError::unsupported(format!(
                            "unsupported REPLCONF sub command: {}",
                            sub_command
                        ))),
                    }
                }
                "PSYNC" => {
                    let id = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("PSYNC id"))?
                        .get_string()?;
                    let id = if id == "?" { None } else { Some(id) };
                    let offset = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("PSYNC offset"))?
                        .get_string()?;
                    let offset = if offset == "-1" {
                        None
                    } else {
                        Some(offset.parse().map_err(|_| {
                            ParseMessageError::unsupported(format!(
                                "PSYNC offset value: {}",
                                offset
                            ))
                        })?)
                    };
                    Ok(ReqCommand::Psync { id, offset })
                }
                "WAIT" => {
                    let number_replicas = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("WAIT number_replicas"))?
                        .get_string()?
                        .parse()?;
                    let timeout = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("WAIT timeout"))?
                        .get_string()?
                        .parse()?;
                    Ok(ReqCommand::Wait {
                        number_replicas,
                        time_out: timeout,
                    })
                }
                "TYPE" => {
                    let key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("TYPE key"))?
                        .get_string()?;
                    Ok(ReqCommand::Type(key))
                }
                "XADD" => {
                    let stream_key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XADD stream key"))?
                        .get_string()?;
                    let entry_id = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XADD entry id"))?
                        .get_string()?;
                    let mut pairs = Vec::with_capacity(messages.len());
                    while let Some(message) = messages.next() {
                        let key = message.get_string()?;
                        let value = messages
                            .next()
                            .ok_or_else(|| ParseMessageError::expect("XADD value"))?
                            .get_string()?;
                        pairs.push((key, value));
                    }
                    Ok(ReqCommand::XADD {
                        stream_key,
                        entry_id,
                        pairs,
                    })
                }
                "XRANGE" => {
                    let stream_key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XRANGE stream key"))?
                        .get_string()?;
                    let start_id = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XRANGE start id"))?
                        .get_string()?;
                    let end_id = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XRANGE start id"))?
                        .get_string()?;

                    Ok(ReqCommand::XRANGE {
                        stream_key,
                        start_id: EntryId::parse::<true>(&start_id)?,
                        end_id: EntryId::parse::<false>(&end_id)?,
                    })
                }
                "XREAD" => {
                    let mut sub_command = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("XREAD sub type"))?
                        .get_string()?;
                    let mut block_time = None;
                    if sub_command == "block" {
                        let time = messages
                            .next()
                            .ok_or_else(|| ParseMessageError::expect("XREAD block time"))?
                            .get_string()?
                            .parse()?;
                        block_time.replace(time);
                        sub_command = messages
                            .next()
                            .ok_or_else(|| ParseMessageError::expect("XREAD sub type"))?
                            .get_string()?;
                    }

                    if sub_command != "streams" {
                        return Err(ParseMessageError::unsupported(format!(
                            "sub command: {:?}",
                            sub_command
                        )));
                    }

                    let messages: Vec<_> = messages.collect();
                    if messages.len() % 2 != 0 {
                        return Err(ParseMessageError::unsupported("XREAD items not even"));
                    }
                    let (keys, start_ids) = messages.split_at(messages.len() / 2);
                    let keys: Vec<_> = keys
                        .iter()
                        .map(|m| m.get_string_cloned())
                        .collect::<Result<_, _>>()?;
                    let start_ids: Vec<_> = start_ids
                        .iter()
                        .map(|m| {
                            let start_id = m.get_string_ref()?;
                            Ok(start_id.to_string())
                        })
                        .collect::<Result<_, ParseMessageError>>()?;
                    let items = keys
                        .into_iter()
                        .zip(start_ids)
                        .map(|(k, s)| XReadItemRaw {
                            stream_key: k,
                            start: s,
                        })
                        .collect();
                    Ok(ReqCommand::XREAD {
                        streams: items,
                        block_time,
                    })
                }
                "INCR" => {
                    let key = messages
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("INCR key"))?
                        .get_string()?;
                    Ok(ReqCommand::Incr(key))
                }
                "MULTI" => Ok(ReqCommand::Multi),
                _ => Err(ParseMessageError::unsupported(format!("command: {}", data))),
            },
            Err(message) => Err(ParseMessageError::unsupported(format!(
                "unsupported message: {:?}",
                message
            ))),
        }
    }
}

impl RespCommand {
    fn parse_array(v: &Vec<Message>) -> Result<Option<Self>, ParseMessageError> {
        let mut iter = v.iter();
        let first = iter.next();
        if let Some(first) = first {
            let first = first.get_string_cloned()?.to_uppercase();
            match first.as_str() {
                "REPLCONF" => {
                    let sub = iter
                        .next()
                        .ok_or_else(|| ParseMessageError::expect("REPLCONF sub command"))?
                        .get_string_cloned()?
                        .to_uppercase();
                    match sub.as_str() {
                        "ACK" => {
                            let ack = iter
                                .next()
                                .ok_or_else(|| ParseMessageError::expect("REPLCONF ACK value"))?
                                .get_string_cloned()?
                                .parse()
                                .map_err(|_| {
                                    ParseMessageError::unsupported(format!(
                                        "REPLCONF ACK value: {}",
                                        sub
                                    ))
                                })?;
                            Ok(Some(RespCommand::Replconf(ReplConfSubresponse::Ack(ack))))
                        }
                        _ => Ok(None),
                    }
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse(message: Message) -> Result<Self, ParseMessageError> {
        match message {
            Message::SimpleStrings(s) => {
                let upper_s = s.to_ascii_uppercase();
                if upper_s == "PONG" {
                    Ok(RespCommand::Pong)
                } else if upper_s == "OK" {
                    Ok(RespCommand::Ok)
                } else if upper_s.starts_with("FULLRESYNC") {
                    let splitted: Vec<&str> = upper_s.split(' ').collect();
                    if splitted.len() != 3 {
                        return Err(ParseMessageError::unsupported(format!(
                            "invalid fullresync message: {:?}",
                            s
                        )));
                    }
                    let id = splitted[1].to_string();
                    let offset = splitted[2].parse().map_err(|_| {
                        ParseMessageError::unsupported(format!(
                            "fullresync offset: {}",
                            splitted[2]
                        ))
                    })?;

                    Ok(RespCommand::FullResync {
                        repl_id: id,
                        offset,
                    })
                } else {
                    Err(ParseMessageError::unsupported(format!(
                        "simple string message: {:?}",
                        s
                    )))
                }
            }
            Message::BulkStrings(Some(s)) => Ok(RespCommand::Bulk(s)),
            Message::BulkStrings(None) => Ok(RespCommand::Nil),
            Message::Arrays(v) => {
                if let Some(resp) = Self::parse_array(&v)? {
                    return Ok(resp);
                }
                v.into_iter()
                    .map(|message| match message {
                        Message::BulkStrings(Some(s)) => Ok(Some(s)),
                        Message::BulkStrings(None) => Ok(None),
                        _ => Err(ParseMessageError::unsupported(format!(
                            "message: {:?}",
                            message
                        ))),
                    })
                    .collect::<Result<Vec<Option<String>>, _>>()
                    .map(RespCommand::Bulks)
            }
            Message::Rdb(content) => Ok(RespCommand::RdbFile(content)),
            _ => Err(ParseMessageError::unsupported(format!(
                "unsupported message: {:?}",
                message
            ))),
        }
    }
}

impl From<RespCommand> for Message {
    fn from(value: RespCommand) -> Self {
        trace!("Convert RespCommand to Message: {:?}", value);
        match value {
            RespCommand::Pong => Message::SimpleStrings("PONG".to_string()),
            RespCommand::Ok => Message::SimpleStrings("OK".to_string()),
            RespCommand::Bulk(s) => Message::BulkStrings(Some(s)),
            RespCommand::Bulks(v) => {
                Message::Arrays(v.into_iter().map(Message::BulkStrings).collect())
            }
            RespCommand::Simple(s) => Message::SimpleStrings(s.to_string()),
            RespCommand::Nil => Message::BulkStrings(None),
            RespCommand::FullResync { repl_id, offset } => {
                Message::SimpleStrings(format!("FULLRESYNC {} {}", repl_id, offset))
            }
            RespCommand::RdbFile(content) => Message::Rdb(content),
            RespCommand::Replconf(sub) => {
                let mut messages = vec![Message::SimpleStrings("REPLCONF".to_string())];
                match sub {
                    ReplConfSubresponse::Ack(ack) => {
                        messages.push(Message::SimpleStrings("ACK".to_string()));
                        messages.push(Message::SimpleStrings(ack.to_string()));
                    }
                }
                Message::Arrays(messages)
            }
            RespCommand::Int(v) => Message::Integers(v),
            RespCommand::StreamEntries(entries) => Message::Arrays(
                entries
                    .into_iter()
                    .map(|e| {
                        let mut kv_messages = Vec::with_capacity(e.pairs.len() * 2);
                        for (k, v) in e.pairs {
                            kv_messages.push(Message::BulkStrings(Some(k)));
                            kv_messages.push(Message::BulkStrings(Some(v)));
                        }
                        Message::Arrays(vec![
                            Message::SimpleStrings(e.id.to_string()),
                            Message::Arrays(kv_messages),
                        ])
                    })
                    .collect(),
            ),
        }
    }
}

impl From<ReqCommand> for Message {
    fn from(value: ReqCommand) -> Self {
        match value {
            ReqCommand::Ping => Message::Arrays(vec![Message::SimpleStrings("PING".to_string())]),
            ReqCommand::Config(cfg) => match cfg {
                ConfigSubCommand::GET(key) => Message::Arrays(vec![
                    Message::SimpleStrings("CONFIG".to_string()),
                    Message::SimpleStrings("GET".to_string()),
                    Message::BulkStrings(Some(key)),
                ]),
            },
            ReqCommand::Echo(data) => Message::Arrays(vec![
                Message::SimpleStrings("ECHO".to_string()),
                Message::BulkStrings(Some(data)),
            ]),
            ReqCommand::Get(key) => Message::Arrays(vec![
                Message::SimpleStrings("GET".to_string()),
                Message::BulkStrings(Some(key)),
            ]),
            ReqCommand::Info(key) => {
                if let Some(key) = key {
                    Message::Arrays(vec![
                        Message::SimpleStrings("INFO".to_string()),
                        Message::BulkStrings(Some(key)),
                    ])
                } else {
                    Message::Arrays(vec![Message::SimpleStrings("INFO".to_string())])
                }
            }
            ReqCommand::KEYS(pattern) => Message::Arrays(vec![
                Message::SimpleStrings("KEYS".to_string()),
                Message::BulkStrings(Some(pattern)),
            ]),
            ReqCommand::Set { key, value, px } => {
                let mut messages = vec![
                    Message::SimpleStrings("SET".to_string()),
                    Message::BulkStrings(Some(key)),
                    Message::BulkStrings(Some(value)),
                ];
                if let Some(px) = px {
                    messages.push(Message::SimpleStrings("PX".to_string()));
                    messages.push(Message::SimpleStrings(px.as_millis().to_string()));
                }
                Message::Arrays(messages)
            }
            ReqCommand::Replconf(repl) => {
                let mut messages = vec![Message::SimpleStrings("REPLCONF".to_string())];

                let mut subs = match repl {
                    ReplconfSubcommand::ListeningPort(port) => {
                        vec![
                            Message::SimpleStrings("listening-port".to_string()),
                            Message::SimpleStrings(port.to_string()),
                        ]
                    }
                    ReplconfSubcommand::Capa(capa) => {
                        vec![
                            Message::SimpleStrings("capa".to_string()),
                            Message::BulkStrings(Some(capa.to_string())),
                        ]
                    }
                    ReplconfSubcommand::Getack => {
                        vec![
                            Message::SimpleStrings("GETACK".to_string()),
                            Message::SimpleStrings("*".to_string()),
                        ]
                    }
                };
                messages.append(&mut subs);
                Message::Arrays(messages)
            }
            ReqCommand::Psync { id, offset } => {
                let id = Message::BulkStrings(Some(id.unwrap_or("?".to_string())));
                let offset = Message::BulkStrings(Some(
                    offset
                        .map(|offset| offset.to_string())
                        .unwrap_or("-1".to_string()),
                ));
                Message::Arrays(vec![
                    Message::SimpleStrings("PSYNC".to_string()),
                    id,
                    offset,
                ])
            }
            ReqCommand::Wait {
                number_replicas,
                time_out,
            } => Message::Arrays(vec![
                Message::SimpleStrings("WAIT".to_string()),
                Message::SimpleStrings(number_replicas.to_string()),
                Message::SimpleStrings(time_out.to_string()),
            ]),
            ReqCommand::Type(key) => Message::Arrays(vec![
                Message::SimpleStrings("TYPE".to_string()),
                Message::BulkStrings(Some(key)),
            ]),
            ReqCommand::XADD {
                stream_key,
                entry_id,
                pairs,
            } => {
                let mut messages = Vec::with_capacity(pairs.len() * 2 + 2);
                messages.push(Message::SimpleStrings("XADD".to_string()));
                messages.push(Message::BulkStrings(Some(stream_key)));
                messages.push(Message::BulkStrings(Some(entry_id)));
                for (k, v) in pairs {
                    messages.push(Message::BulkStrings(Some(k)));
                    messages.push(Message::BulkStrings(Some(v)));
                }
                Message::Arrays(messages)
            }
            ReqCommand::XRANGE {
                stream_key,
                start_id,
                end_id,
            } => Message::Arrays(vec![
                Message::SimpleStrings("XRANGE".to_string()),
                Message::BulkStrings(Some(stream_key)),
                Message::SimpleStrings(start_id.to_string()),
                Message::SimpleStrings(end_id.to_string()),
            ]),
            ReqCommand::XREAD {
                block_time,
                streams,
            } => {
                let mut key_messages = Vec::with_capacity(streams.len() * 2 + 2);
                let mut start_messages = Vec::with_capacity(streams.len());
                key_messages.push(Message::SimpleStrings("XREAD".to_string()));
                if let Some(block_time) = block_time {
                    key_messages.push(Message::SimpleStrings("block".to_string()));
                    key_messages.push(Message::SimpleStrings(block_time.to_string()));
                }

                key_messages.push(Message::SimpleStrings("streams".to_string()));
                for item in streams {
                    key_messages.push(Message::BulkStrings(Some(item.stream_key)));
                    start_messages.push(Message::SimpleStrings(item.start.to_string()));
                }
                key_messages.extend(start_messages);
                Message::Arrays(key_messages)
            }
            ReqCommand::Incr(key) => Message::Arrays(vec![
                Message::SimpleStrings("INCR".to_string()),
                Message::BulkStrings(Some(key)),
            ]),
            ReqCommand::Multi => Message::Arrays(vec![Message::SimpleStrings("Multi".to_string())]),
        }
    }
}
