use bytes::Buf;
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use tracing::{error, trace};

use crate::command::{ParseMessageError, ReqCommand, RespCommand};

const CRLF: &[u8] = b"\r\n";

#[derive(thiserror::Error, Debug)]
pub enum RespError {
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    StreamEntryIdDecrease,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    StreamEntryIdZero,
}

#[derive(Debug, PartialEq, Clone)]
pub enum Message {
    // RESP2 	Simple 	+
    SimpleStrings(String),
    // RESP2 	Simple 	-
    SimpleErrors(String),
    // RESP2 	Simple 	:
    Integers(i64),
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
    pub(crate) fn get_string(self) -> Result<String, Self> {
        Ok(match self {
            Message::BulkStrings(Some(data)) => data,
            Message::SimpleStrings(data) => data,
            _ => return Err(self),
        })
    }

    pub(crate) fn get_string_cloned(&self) -> Result<String, ParseMessageError> {
        match self {
            Message::BulkStrings(Some(data)) => Ok(data.clone()),
            Message::SimpleStrings(data) => Ok(data.clone()),
            _ => Err(ParseMessageError::ExpectString(self.clone())),
        }
    }

    pub fn parse_resp(self) -> Result<RespCommand, ParseMessageError> {
        RespCommand::parse(self)
    }

    pub fn parse_req(self) -> Result<ReqCommand, ParseMessageError> {
        match self {
            Message::Arrays(messages) => ReqCommand::parse(messages),
            message => Err(ParseMessageError::unsupported(format!(
                "unsupported top message: {:?}",
                message
            ))),
        }
    }

    /// Slow, may be fixed
    pub fn count_bytes(&self) -> Result<usize, io::Error> {
        let mut buffer = bytes::BytesMut::new();
        self.encode_to(&mut buffer)?;
        Ok(buffer.len())
    }
}

#[derive(Default, Debug)]
pub struct MessageFramer;

impl Decoder for MessageFramer {
    type Item = (Message, usize);
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let mut parser = Parser::new(src);
        let message = parser.parse()?;
        let consumed = parser.idx;

        if message.is_some() {
            src.advance(consumed);
        } else {
            trace!("not enough data");
        }
        Ok(message.map(|m| (m, consumed)))
    }
}

impl Encoder<Message> for MessageFramer {
    type Error = std::io::Error;

    fn encode(&mut self, item: Message, dst: &mut bytes::BytesMut) -> Result<(), Self::Error> {
        item.encode_to(dst)
    }
}

impl Message {
    fn encode_to(&self, dst: &mut bytes::BytesMut) -> Result<(), io::Error> {
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
            Message::SimpleErrors(data) => {
                debug_assert!(data.find('\r').is_none());
                debug_assert!(data.find('\n').is_none());
                dst.extend_from_slice(b"-");
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
            Message::Integers(v) => {
                dst.extend_from_slice(b":");
                dst.extend_from_slice(v.to_string().as_bytes());
                dst.extend_from_slice(CRLF);
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
                    } else {
                        trace!("rdb file");
                        return Ok(Some(Message::Rdb(data)));
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
            b'-' => {
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
                    return Ok(Some(Message::SimpleErrors(data)));
                } else {
                    Ok(None)
                }
            }
            b':' => {
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
                    let data: i64 = data.parse().map_err(|e| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("parse to i64 failed: {:?}", e),
                        )
                    })?;
                    return Ok(Some(Message::Integers(data)));
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
