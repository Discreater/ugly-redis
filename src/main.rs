use futures_util::{SinkExt, StreamExt};
use redis_starter_rust::{
    db::Db,
    rdb,
    resp::{Command, ConfigComand, Message, MessageFramer},
};
use std::{
    collections::HashMap,
    io,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{error, info, trace, Level};

use clap::Parser;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    dir: Option<String>,
    #[arg(long)]
    dbfilename: Option<String>,
    #[arg(long, default_value = "6379")]
    port: u16,
    #[arg(long)]
    replicaof: Option<String>,
}

impl Args {
    fn is_master(&self) -> bool {
        self.replicaof.is_none()
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let log_level = if std::env::var("REDIS_LOG").is_ok() {
        Level::TRACE
    } else {
        Level::WARN
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();
    let args = Args::parse();
    let db = rdb::init(&args.dir, &args.dbfilename)?;

    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), args.port))
        .await
        .unwrap();
    let db = Arc::new(RwLock::new(db));
    let config = Arc::new(args);

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        let config = config.clone();
        tokio::spawn(async move {
            process_socket(socket, db, config)
                .await
                .expect("process socket error")
        });
    }
}

async fn process_socket(socket: TcpStream, db: Arc<RwLock<Db>>, cfg: Arc<Args>) -> io::Result<()> {
    let mut socket = tokio_util::codec::Framed::new(socket, MessageFramer);

    loop {
        let message = match socket.next().await {
            None => {
                trace!("connection closed");
                return Ok(());
            }
            Some(message) => message?,
        };

        match message {
            Message::Arrays(messages) => match Command::from(messages)? {
                Command::Ping => {
                    info!("received command PING");
                    socket
                        .send(Message::SimpleStrings("PONG".to_string()))
                        .await?;
                }
                Command::Echo(data) => {
                    info!("received command ECHO with data: {}", data);
                    socket.send(Message::BulkStrings(Some(data))).await?;
                }
                Command::Get(key) => {
                    info!("received command GET with key: {}", key);
                    let (value, expire) = {
                        let db = db.read().await;
                        (db.kv.get(&key).cloned(), db.expire.get(&key).cloned())
                    };
                    let value = if let Some(value) = value {
                        if let Some(expire) = expire {
                            if UNIX_EPOCH + Duration::from_millis(expire)
                                < std::time::SystemTime::now()
                            {
                                None
                            } else {
                                Some(value)
                            }
                        } else {
                            Some(value)
                        }
                    } else {
                        None
                    };
                    socket.send(Message::BulkStrings(value)).await?;
                }
                Command::Set { key, value, px } => {
                    info!(
                        "received command SET with key: {} and value: {}, px: {:?}",
                        key, value, px
                    );
                    let expired_time = px.and_then(|px| {
                        (std::time::SystemTime::now() + px)
                            .duration_since(UNIX_EPOCH)
                            .ok()
                            .map(|d| d.as_millis() as u64)
                    });
                    {
                        let mut db = db.write().await;
                        db.kv.insert(key.clone(), value);

                        if let Some(expired_time) = expired_time {
                            db.expire.insert(key.clone(), expired_time);
                        } else {
                            db.expire.remove_entry(&key);
                        }
                    }
                    socket
                        .send(Message::SimpleStrings("OK".to_string()))
                        .await?;
                }
                Command::Config(ConfigComand::GET(key)) => {
                    info!("received command CONFIG GET with key: {}", key);
                    let value = match key.as_str() {
                        "dir" => cfg.dir.clone(),
                        "dbfilename" => cfg.dbfilename.clone(),
                        _ => None,
                    };
                    let bulk_strs =
                        vec![Message::BulkStrings(Some(key)), Message::BulkStrings(value)];
                    socket.send(Message::Arrays(bulk_strs)).await?;
                }
                Command::KEYS(pattern) => {
                    info!("received commadn KEYS with pattern: {}", pattern);
                    let keys = {
                        let db = db.read().await;
                        db.kv.keys().cloned().collect::<Vec<String>>()
                    };
                    let keys = keys
                        .into_iter()
                        .filter(|key| simple_pattern_match(&pattern, key))
                        .map(|key| Message::BulkStrings(Some(key)))
                        .collect::<Vec<Message>>();
                    socket.send(Message::Arrays(keys)).await?;
                }
                Command::Info(section) => {
                    info!("received command INFO with section: {:?}", section);
                    match section.as_ref().map(|s| s.as_str()) {
                        Some("replication") => {
                            let info = replication_info(&cfg)
                                .into_iter()
                                .map(|(k, v)| format!("{}:{}", k, v))
                                .collect::<Vec<String>>()
                                .join("\r\n");
                            socket.send(Message::BulkStrings(Some(info))).await?;
                        }
                        None => {
                            let info = all_info(&cfg)
                                .into_iter()
                                .map(|(k, v)| format!("{}:{}", k, v))
                                .collect::<Vec<String>>()
                                .join("\r\n");
                            socket.send(Message::BulkStrings(Some(info))).await?;
                        }
                        s => {
                            error!("unsupported INFO section: {:?}", s);
                        }
                    }
                }
                cmd => {
                    error!("unsupported command: {:?}", cmd);
                }
            },
            _ => {
                error!("unsupported message: {:?}", message);
            }
        }
    }
}

fn replication_info(cfg: &Args) -> HashMap<String, String> {
    let mut info = HashMap::new();
    if cfg.is_master() {
        info.insert("role".to_string(), "master".to_string());
        info.insert("master_replid".to_string(), REPLICATION_ID.to_string());
        info.insert("master_repl_offset".to_string(), "0".to_string());
    } else {
        info.insert("role".to_string(), "slave".to_string());
    }
    info
}

fn all_info(cfg: &Args) -> HashMap<String, String> {
    replication_info(cfg)
}

fn simple_pattern_match(pattern: &str, key: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.starts_with("*") && pattern.ends_with("*") {
        return key.contains(&pattern[1..pattern.len() - 1]);
    }
    if pattern.starts_with("*") {
        return key.ends_with(&pattern[1..]);
    }
    if pattern.ends_with("*") {
        return key.starts_with(&pattern[..pattern.len() - 1]);
    }
    false
}
