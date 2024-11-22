use core::fmt;
use std::collections::HashMap;

use tracing::trace;

use crate::message::RespError;

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
        match self {
            Value::Stream(stream) => {
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
            _ => Err(ValueError::TypeError {
                expect: "stream".to_string(),
                got: Value::ty(Some(self)).to_string(),
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
    const ZERO: Self = Self { time: 0, seq: 0 };
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

#[derive(Clone, Debug)]
pub struct StreamEntry {
    pub id: EntryId,
    pub pairs: Vec<(String, String)>,
}

impl Value {
    pub fn ty(v: Option<&Self>) -> &'static str {
        match v {
            Some(Value::String(_)) => "string",
            Some(Value::List) => "list",
            Some(Value::Set) => "set",
            Some(Value::ZSet) => "zset",
            Some(Value::Hash) => "hash",
            Some(Value::Stream(_)) => "stream",
            None => "none",
        }
    }
}
