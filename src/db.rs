use std::collections::HashMap;

pub type KvTable = HashMap<String, String>;
pub type ExpireTable = HashMap<String, u64>;


#[derive(Default)]
pub struct Db {
    pub kv: KvTable,
    pub expire: ExpireTable,
}

