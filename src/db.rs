use std::collections::HashMap;

use anyhow::bail;

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
}

impl Value {
    pub fn push_stream_entry(&mut self, entry: StreamEntry) -> Result<(), ValueError> {
        match self {
            Value::Stream(stream) => {
                if entry.id <= EntryId::ZERO {
                    return Err(RespError::StreamEntryIdZero.into());
                }
                if let Some(top) = stream.last() {
                    if top.id >= entry.id {
                        return Err(RespError::StreamEntryIdDecrease.into());
                    }
                }
                stream.push(entry);
            }
            _ => {
                return Err(ValueError::TypeError {
                    expect: "stream".to_string(),
                    got: Value::ty(Some(self)).to_string(),
                })
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryId {
    time: u64,
    seq: u64,
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

impl StreamEntry {
    pub fn new(id: &str, pairs: Vec<(String, String)>) -> anyhow::Result<Self> {
        let id = if id == "*" {
            unimplemented!()
        } else {
            let splitted = id.split('-').collect::<Vec<_>>();
            if splitted.len() != 2 {
                bail!("unsuppored entry id: {id}");
            }
            let time = splitted[0].parse()?;
            let seq = splitted[1].parse()?;
            EntryId { time, seq }
        };
        Ok(StreamEntry { id, pairs })
    }
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
