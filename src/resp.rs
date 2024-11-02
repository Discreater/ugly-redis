use bytes::Buf;
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{debug, error, warn};

use crate::utils::MapNone;

const CRLF: &'static [u8] = b"\r\n";

#[derive(Debug)]
#[non_exhaustive]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String },
    Get(String),
}

pub struct Commander(std::vec::IntoIter<Message>);

impl Commander {
    pub fn new(messages: Vec<Message>) -> Commander {
        Commander(messages.into_iter())
    }
}

impl<'a> Iterator for Commander {
    type Item = Command;

    fn next(&mut self) -> Option<Self::Item> {
        let message = self.0.next()?;
        match message {
            Message::BulkStrings(Some(data)) => match data.to_uppercase().as_str() {
                "PING" => Some(Command::Ping),
                "ECHO" => {
                    let message = self.0.next()?;
                    let data = message.get_string()?;
                    Some(Command::Echo(data))
                }
                "SET" => {
                    let key = self
                        .0
                        .next()
                        .and_then(Message::get_string)
                        .map_none(|| warn!("SET command missing key"))?;
                    let value = self.0.next().and_then(Message::get_string).map_none(|| {
                        warn!("SET command missing value");
                    })?;
                    Some(Command::Set { key, value })
                }
                "GET" => {
                    let key = self
                        .0
                        .next()
                        .and_then(Message::get_string)
                        .map_none(|| warn!("Get command missing key"))?;
                    Some(Command::Get(key))
                }
                _ => {
                    error!("unsupported command: {}", data);
                    None
                }
            },
            _ => {
                error!("unsupported message type: {:?}", message);
                return None;
            }
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
}

impl Message {
    pub fn get_string(self) -> Option<String> {
        Some(match self {
            Message::BulkStrings(Some(data)) => data,
            Message::SimpleStrings(data) => data,
            _ => return None,
        })
    }
}

pub struct MessageFramer;

impl Decoder for MessageFramer {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut parser = Parser::new(src);
        let message = parser.parse()?;
        if message.is_none() {
            return Ok(None);
        }
        src.advance(parser.idx);
        Ok(message)
    }
}

impl Encoder<Message> for MessageFramer {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        match item {
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
                dst.extend_from_slice(b"+");
                dst.extend_from_slice(data.as_bytes());
                dst.extend_from_slice(CRLF);
            }
            _ => {
                error!("unsupported message: {:?}", item);
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
            debug!("remain is empty");
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
                    if self.remain().len() < len + 2 {
                        return Ok(None);
                    }
                    let data = self.advance_unchecked(len);
                    let data = String::from_utf8(data.to_vec()).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid utf8 string")
                    })?;
                    if self.remain().starts_with(CRLF) {
                        self.idx += 2;
                        return Ok(Some(Message::BulkStrings(Some(data))));
                    } else {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, "expected CRLF"));
                    }
                } else {
                    return Ok(Some(Message::BulkStrings(None)));
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
