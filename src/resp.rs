use bytes::Buf;
use std::{io, time::Duration, vec};
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, trace};

const CRLF: &'static [u8] = b"\r\n";

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
}

#[derive(Debug, Clone)]
pub enum ReplconfSubcommand {
    ListeningPort(u16),
    Capa(String),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum RespCommand {
    Pong,
    Ok,
    FullResync { repl_id: String, offset: usize },
    RdbFile(Vec<u8>),
    Bulk(String),
    Bulks(Vec<Option<String>>),
    Nil,
    Simple(&'static str),
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ConfigSubCommand {
    GET(String),
}

impl From<Message> for io::Error {
    fn from(message: Message) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid message: {:?}", message),
        )
    }
}

impl ReqCommand {
    fn parse(messages: Vec<Message>) -> Result<Self, std::io::Error> {
        let mut messages = messages.into_iter();
        let message = messages
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "empty messages"))?;
        match message.get_string() {
            Ok(data) => match data.to_uppercase().as_str() {
                "PING" => Ok(ReqCommand::Ping),
                "ECHO" => {
                    let message = messages.next().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "missing ECHO data")
                    })?;
                    let data = message.get_string()?;
                    Ok(ReqCommand::Echo(data))
                }
                "SET" => {
                    let key = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing SET key")
                        })?
                        .get_string()?;
                    let value = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing SET value")
                        })?
                        .get_string()?;
                    let mut px = None;
                    while let Some(message) = messages.next() {
                        match message.get_string() {
                            Ok(option) => match option.to_uppercase().as_str() {
                                "PX" => {
                                    let data = messages
                                        .next()
                                        .ok_or_else(|| {
                                            io::Error::new(
                                                io::ErrorKind::InvalidData,
                                                "missing PX value",
                                            )
                                        })?
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
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing GET key")
                        })?
                        .get_string()?;
                    Ok(ReqCommand::Get(key))
                }
                "KEYS" => {
                    let pattern = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing KEYS pattern")
                        })?
                        .get_string()?;
                    Ok(ReqCommand::KEYS(pattern))
                }
                "CONFIG" => {
                    let sub_command = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing CONFIG sub command")
                        })?
                        .get_string()?;
                    match sub_command.to_uppercase().as_str() {
                        "GET" => {
                            let key = messages
                                .next()
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "missing CONFIG GET key",
                                    )
                                })?
                                .get_string()?;
                            Ok(ReqCommand::Config(ConfigSubCommand::GET(key)))
                        }
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unsupported CONFIG sub command: {}", sub_command),
                        )),
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
                        .ok_or_else(|| {
                            io::Error::new(
                                io::ErrorKind::InvalidData,
                                "missing REPLCONF sub command",
                            )
                        })?
                        .get_string()?;
                    match sub_command.to_uppercase().as_str() {
                        "LISTENING-PORT" => {
                            let port = messages
                                .next()
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "missing REPLCONF LISTENING-PORT value",
                                    )
                                })?
                                .get_string()?;
                            match port.parse::<u16>() {
                                Ok(port) => Ok(ReqCommand::Replconf(
                                    ReplconfSubcommand::ListeningPort(port),
                                )),
                                Err(_) => Err(io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    format!("invalid REPLCONF LISTENING-PORT value: {}", port),
                                )),
                            }
                        }
                        "CAPA" => {
                            let capa = messages
                                .next()
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidData,
                                        "missing REPLCONF CAPA value",
                                    )
                                })?
                                .get_string()?;
                            Ok(ReqCommand::Replconf(ReplconfSubcommand::Capa(capa)))
                        }
                        _ => Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unsupported REPLCONF sub command: {}", sub_command),
                        )),
                    }
                }
                "PSYNC" => {
                    let id = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing PSYNC id")
                        })?
                        .get_string()?;
                    let id = if id == "?" { None } else { Some(id) };
                    let offset = messages
                        .next()
                        .ok_or_else(|| {
                            io::Error::new(io::ErrorKind::InvalidData, "missing PSYNC offset")
                        })?
                        .get_string()?;
                    let offset = if offset == "-1" {
                        None
                    } else {
                        Some(
                            offset
                                .parse()
                                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?,
                        )
                    };
                    Ok(ReqCommand::Psync { id, offset })
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("unsupported command: {}", data),
                )),
            },
            Err(message) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported message: {:?}", message),
            )),
        }
    }
}

impl RespCommand {
    fn parse(message: Message) -> Result<Self, io::Error> {
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
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("invalid fullresync message: {:?}", s),
                        ));
                    }
                    let id = splitted[1].to_string();
                    let offset = splitted[2].parse().map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, format!("invalid offset: {}", e))
                    })?;

                    Ok(RespCommand::FullResync {
                        repl_id: id,
                        offset,
                    })
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported simple string message: {:?}", s),
                    ))
                }
            }
            Message::BulkStrings(Some(s)) => Ok(RespCommand::Bulk(s)),
            Message::BulkStrings(None) => Ok(RespCommand::Nil),
            Message::Arrays(v) => v
                .into_iter()
                .map(|message| match message {
                    Message::BulkStrings(Some(s)) => Ok(Some(s)),
                    Message::BulkStrings(None) => Ok(None),
                    _ => Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unsupported message: {:?}", message),
                    )),
                })
                .collect::<Result<Vec<Option<String>>, io::Error>>()
                .map(RespCommand::Bulks),
            Message::Rdb(content) => Ok(RespCommand::RdbFile(content)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported message: {:?}", message),
            )),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Message {
    // RESP2 	Simple 	+
    SimpleStrings(String),
    // RESP2 	Simple 	-
    SimpleErrors,
    // RESP2 	Simple 	:
    Integers,
    // RESP2 	Aggregate 	$
    BulkStrings(Option<String>),
    // RESP2 	Aggregate 	*
    Arrays(Vec<Message>),
    // RESP3 	Simple 	_
    Nulls,
    // RESP3 	Simple 	#
    Booleans,
    // RESP3 	Simple 	,
    Doubles,
    // RESP3 	Simple 	(
    BigNumbers,
    // RESP3 	Aggregate 	!
    BulkErrors,
    // RESP3 	Aggregate 	=
    VerbatimStrings,
    // RESP3 	Aggregate 	%
    Maps,
    // RESP3 	Aggregate 	`
    Attributes,
    // RESP3 	Aggregate 	~
    Sets,
    // RESP3 	Aggregate 	>
    Pushes,
    // bulk strings withouth the trailing CRLF.
    Rdb(Vec<u8>),
}

impl Message {
    fn get_string(self) -> Result<String, Self> {
        Ok(match self {
            Message::BulkStrings(Some(data)) => data,
            Message::SimpleStrings(data) => data,
            _ => return Err(self),
        })
    }

    pub fn parse_resp(self) -> Result<RespCommand, io::Error> {
        RespCommand::parse(self)
    }

    pub fn parse_req(self) -> Result<ReqCommand, io::Error> {
        match self {
            Message::Arrays(messages) => Ok(ReqCommand::parse(messages)?),
            message => Err(std::io::Error::new(
                io::ErrorKind::Unsupported,
                format!("unsupported top message: {:?}", message),
            )),
        }
    }
}

pub struct MessageFramer;

impl Decoder for MessageFramer {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut parser = Parser::new(src);
        let message = parser.parse()?;

        if message.is_some() {
            src.advance(parser.idx);
        }
        Ok(message)
    }
}

impl From<RespCommand> for Message {
    fn from(value: RespCommand) -> Self {
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
        }
    }
}
impl Encoder<Message> for MessageFramer {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        item.encode_to(dst)
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
                    Message::SimpleStrings(key),
                ]),
            },
            ReqCommand::Echo(data) => Message::Arrays(vec![
                Message::SimpleStrings("ECHO".to_string()),
                Message::SimpleStrings(data),
            ]),
            ReqCommand::Get(key) => Message::Arrays(vec![
                Message::SimpleStrings("GET".to_string()),
                Message::SimpleStrings(key),
            ]),
            ReqCommand::Info(key) => {
                if let Some(key) = key {
                    Message::Arrays(vec![
                        Message::SimpleStrings("INFO".to_string()),
                        Message::SimpleStrings(key),
                    ])
                } else {
                    Message::Arrays(vec![Message::SimpleStrings("INFO".to_string())])
                }
            }
            ReqCommand::KEYS(pattern) => Message::Arrays(vec![
                Message::SimpleStrings("KEYS".to_string()),
                Message::SimpleStrings(pattern),
            ]),
            ReqCommand::Set { key, value, px } => {
                let mut messages = vec![
                    Message::SimpleStrings("SET".to_string()),
                    Message::SimpleStrings(key),
                    Message::SimpleStrings(value),
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
        }
    }
}

impl Message {
    fn encode_to(self, dst: &mut bytes::BytesMut) -> Result<(), io::Error> {
        match self {
            Message::Arrays(messages) => {
                dst.extend_from_slice(b"*");
                dst.extend_from_slice(messages.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                for message in messages {
                    message.encode_to(dst)?;
                }
            }
            Message::BulkStrings(Some(data)) => {
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(data.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                dst.extend_from_slice(data.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            Message::BulkStrings(None) => {
                dst.extend_from_slice(b"$-1");
                dst.extend_from_slice(CRLF);
            }
            Message::SimpleStrings(data) => {
                debug_assert!(data.find('\r').is_none());
                debug_assert!(data.find('\n').is_none());
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(data.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            Message::Rdb(content) => {
                // bulk strings withouth the trailing CRLF.
                dst.extend_from_slice(b"$");
                dst.extend_from_slice(content.len().to_string().as_bytes());
                dst.extend_from_slice(CRLF);
                dst.extend_from_slice(&content);
            }
            _ => {
                error!("unsupported message: {:?}", self);
                unimplemented!()
            }
        }
        Ok(())
    }
}

struct Parser<'a> {
    data: &'a bytes::BytesMut,
    idx: usize,
}

impl Parser<'_> {
    fn new(data: &bytes::BytesMut) -> Parser {
        Parser { data, idx: 0 }
    }

    fn parse(&mut self) -> Result<Option<Message>, io::Error> {
        // Clients send commands to a Redis server as an array of bulk strings.
        // The first (and sometimes also the second) bulk string in the array is the command's name.
        // Subsequent elements of the array are the arguments for the command.
        if self.remain().is_empty() {
            trace!("remain is empty");
            return Ok(None);
        }
        let data_type = self.consume_one_unchecked();
        match data_type {
            b'*' => {
                let len = self.consume_decimal_line()?;
                let mut messages = Vec::with_capacity(len);
                for _ in 0..len {
                    let message = self.parse()?;
                    if let Some(message) = message {
                        messages.push(message);
                    } else {
                        // not enough data
                        return Ok(None);
                    }
                }
                return Ok(Some(Message::Arrays(messages)));
            }
            b'$' => {
                let len = self.consume_nullable_decimal_line()?;
                if let Some(len) = len {
                    if self.remain().len() < len {
                        return Ok(None);
                    }
                    let data = self.advance_unchecked(len).to_vec();

                    if self.remain().starts_with(CRLF) {
                        let data = String::from_utf8(data).map_err(|_| {
                            io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 string")
                        })?;
                        self.idx += 2;
                        return Ok(Some(Message::BulkStrings(Some(data))));
                    } else if self.remain().is_empty() {
                        trace!("rdb file");
                        return Ok(Some(Message::Rdb(data)));
                    } else {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected CRLF"));
                    }
                } else {
                    return Ok(Some(Message::BulkStrings(None)));
                }
            }
            b'+' => {
                let end = self.remain().iter().position(|&b| b == b'\r');
                if let Some(end) = end {
                    let data = self.advance_unchecked(end);
                    let data = String::from_utf8(data.to_vec()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 string")
                    })?;
                    if !self.remain().starts_with(CRLF) {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected CRLF"));
                    }
                    self.idx += 2;
                    return Ok(Some(Message::SimpleStrings(data)));
                } else {
                    Ok(None)
                }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("invalid data type: {}", data_type as char),
                ))
            }
        }
    }

    #[inline(always)]
    fn remain(&self) -> &[u8] {
        &self.data[self.idx..]
    }

    fn consume_one_unchecked(&mut self) -> u8 {
        let byte = self.data[self.idx];
        self.idx += 1;
        byte
    }

    fn consume_one(&mut self) -> Result<u8, io::Error> {
        if self.idx >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        let byte = self.consume_one_unchecked();
        Ok(byte)
    }

    fn look_one_unchecked(&self) -> u8 {
        self.data[self.idx]
    }

    fn look_one(&self) -> Result<u8, io::Error> {
        if self.idx >= self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            ));
        }
        Ok(self.look_one_unchecked())
    }

    fn consume_decimal_line(&mut self) -> Result<usize, io::Error> {
        let mut num = 0;
        loop {
            let byte = self.consume_one()?;
            if byte == b'\r' {
                if self.consume_one()? != b'\n' {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "expected CRLF"));
                }
                return Ok(num);
            }
            if !byte.is_ascii_digit() {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "expected digit"));
            }
            num = num * 10 + (byte - b'0') as usize;
        }
    }

    fn consume_nullable_decimal_line(&mut self) -> Result<Option<usize>, io::Error> {
        let byte = self.look_one()?;
        if byte == b'-' {
            if self.remain().starts_with(b"-1\r\n") {
                self.idx += 4;
                Ok(None)
            } else {
                Err(io::Error::new(io::ErrorKind::InvalidData, "expected -1"))
            }
        } else {
            self.consume_decimal_line().map(Some)
        }
    }

    fn advance_unchecked(&mut self, n: usize) -> &[u8] {
        let data = &self.data[self.idx..self.idx + n];
        self.idx += n;
        data
    }
}
