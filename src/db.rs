use core::fmt;
use std::{collections::HashMap, u64};

use tracing::trace;

use crate::{command::ParseMessageError, message::RespError};

pub type KvTable = HashMap<String, Value>;
pub type ExpireTable = HashMap<String, u64>;

#[derive(Default)]
pub struct Db {
    pub kv: KvTable,
    pub expire: ExpireTable,
}

#[derive(Clone, Debug)]
pub enum Value {
    String(String),
    Integer(i64),
    List,
    Set,
    ZSet,
    Hash,
    Stream(Vec<StreamEntry>),
}

#[derive(thiserror::Error, Debug)]
pub enum ValueError {
    #[error("Type error, expect {expect}, but got {got}")]
    TypeError { expect: String, got: String },
    #[error(transparent)]
    RespError(#[from] RespError),
    #[error("unsupported entry id: {0}")]
    Unsupported(String),
    #[error("parse id error while parsing: {0}")]
    ParseIdError(String),
}

impl Value {
    fn seq_after(time: u64, top: &EntryId) -> u64 {
        if time == top.time {
            top.seq + 1
        } else {
            0
        }
    }
    pub fn push_stream_entry(
        &mut self,
        id: &str,
        pairs: Vec<(String, String)>,
    ) -> Result<EntryId, ValueError> {
        let stream = self.stream_entries_mut()?;
        if id == "0-0" {
            return Err(RespError::StreamEntryIdZero.into());
        }
        let top = stream.last().map(|e| &e.id).unwrap_or(&EntryId::ZERO);

        let id = if id == "*" {
            // current unix time in milliseconds
            let time = chrono::Utc::now().timestamp_millis() as u64;
            let seq = Value::seq_after(time, top);
            EntryId { time, seq }
        } else {
            let splitted = id.split('-').collect::<Vec<_>>();
            if splitted.len() != 2 {
                return Err(ValueError::Unsupported(format!("entry id: {id}")));
            }
            let time: u64 = splitted[0]
                .parse()
                .map_err(|_| ValueError::ParseIdError("time".to_string()))?;
            let seq = splitted[1];

            let seq = if seq == "*" {
                Value::seq_after(time, top)
            } else {
                seq.parse()
                    .map_err(|_| ValueError::ParseIdError("sequence".to_string()))?
            };
            EntryId { time, seq }
        };
        trace!("pushing id: {id}, top id: {top}");
        if &id <= top {
            return Err(RespError::StreamEntryIdDecrease.into());
        }

        stream.push(StreamEntry { id, pairs });
        Ok(id)
    }

    pub fn xrange(&self, start: &EntryId, end: &EntryId) -> Result<Vec<StreamEntry>, ValueError> {
        let entries = self.stream_entries()?;
        let start_pos = match entries.binary_search_by_key(start, |e| e.id) {
            Ok(i) => i,
            Err(i) => i,
        };
        let end_pos = match entries.binary_search_by_key(end, |e| e.id) {
            Ok(i) => i + 1,
            Err(i) => i,
        };
        Ok(entries[start_pos..end_pos].to_vec())
    }

    pub fn map_xread_raw(&self, id: &str) -> Result<EntryId, ValueError> {
        let entries = self.stream_entries()?;
        if id == "$" {
            return Ok(entries.last().map(|last| last.id).unwrap_or(EntryId::ZERO));
        }
        let id = EntryId::parse::<false>(id)
            .map_err(|_| ValueError::ParseIdError(format!("id: {id}")))?;
        Ok(id)
    }

    pub fn xread(&self, start: &EntryId) -> Result<Vec<StreamEntry>, ValueError> {
        let entries = self.stream_entries()?;
        let start_pos = match entries.binary_search_by_key(start, |e| e.id) {
            Ok(i) => i + 1, // exclude `start``
            Err(i) => i,
        };
        Ok(entries[start_pos..].to_vec())
    }

    pub fn incr(&mut self) -> Result<i64, ValueError> {
        match self {
            Value::String(s) => {
                let v: i64 = s.parse().map_err(|_| ValueError::TypeError {
                    expect: "integer".to_string(),
                    got: s.to_string(),
                })?;
                let v = v + 1;
                *self = Value::Integer(v);
                Ok(v)
            }
            Value::Integer(i) => {
                *i += 1;
                Ok(*i)
            }
            _ => Err(ValueError::TypeError {
                expect: "String".to_string(),
                got: self.ty().to_string(),
            }),
        }
    }

    fn stream_entries(&self) -> Result<&Vec<StreamEntry>, ValueError> {
        match self {
            Value::Stream(entries) => Ok(entries),
            _ => Err(ValueError::TypeError {
                expect: "stream".to_string(),
                got: self.ty().to_string(),
            }),
        }
    }
    fn stream_entries_mut(&mut self) -> Result<&mut Vec<StreamEntry>, ValueError> {
        match self {
            Value::Stream(entries) => Ok(entries),
            _ => Err(ValueError::TypeError {
                expect: "stream".to_string(),
                got: self.ty().to_string(),
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryId {
    time: u64,
    seq: u64,
}

impl fmt::Display for EntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.time, self.seq)
    }
}

impl EntryId {
    pub const ZERO: Self = Self { time: 0, seq: 0 };
    pub const MAX: Self = Self {
        time: u64::MAX,
        seq: u64::MAX,
    };

    pub fn new(time: u64, seq: u64) -> Self {
        Self { time, seq }
    }

    pub fn next(&self) -> Self {
        if self.seq == u64::MAX {
            Self {
                time: self.time + 1,
                seq: self.seq,
            }
        } else {
            Self {
                time: self.time,
                seq: self.seq + 1,
            }
        }
    }

    pub fn parse<const IS_START: bool>(id: &str) -> Result<EntryId, ParseMessageError> {
        if IS_START {
            if id == "-" {
                return Ok(EntryId::ZERO);
            }
        } else {
            if id == "+" {
                return Ok(EntryId::MAX);
            }
        }
        if id.contains('-') {
            let splitted = id.split('-').collect::<Vec<_>>();
            if splitted.len() != 2 {
                return Err(ParseMessageError::unsupported(format!("entry id: {}", id)));
            }
            let time: u64 = splitted[0].parse()?;
            let seq: u64 = splitted[1].parse()?;
            Ok(EntryId::new(time, seq))
        } else {
            let time = id.parse()?;
            Ok(EntryId::new(time, if IS_START { 0 } else { u64::MAX }))
        }
    }
}

impl Ord for EntryId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.time.cmp(&other.time) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        self.seq.cmp(&other.seq)
    }
}

impl PartialOrd for EntryId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub id: EntryId,
    pub pairs: Vec<(String, String)>,
}

impl Ord for StreamEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl PartialOrd for StreamEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl Value {
    pub fn ty(&self) -> &'static str {
        match self {
            Value::String(_) => "string",
            Value::List => "list",
            Value::Set => "set",
            Value::ZSet => "zset",
            Value::Hash => "hash",
            Value::Stream(_) => "stream",
            Value::Integer(_) => "string",
        }
    }

    pub fn tyn(v: Option<&Self>) -> &'static str {
        match v {
            Some(v) => v.ty(),
            None => "none",
        }
    }
}
