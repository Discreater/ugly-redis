use std::collections::HashMap;

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
    Stream,
}

impl Value {
    pub fn ty(v: Option<&Self>) -> &'static str {
        match v {
            Some(Value::String(_)) => "string",
            Some(Value::List) => "list",
            Some(Value::Set) => "set",
            Some(Value::ZSet) => "zset",
            Some(Value::Hash) => "hash",
            Some(Value::Stream) => "stream",
            None => "none",
        }
    }
}
