use futures_util::{SinkExt, StreamExt};
use redis_starter_rust::resp::{Command, ConfigComand, Message, MessageFramer};
use std::{collections::HashMap, io, path::PathBuf, sync::Arc};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{error, info, trace, Level};

use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    dir: Option<PathBuf>,
    dbfilename: Option<String>,
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
    let config = Arc::new(RwLock::new(args));

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(RwLock::new(HashMap::new()));

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

#[derive(Debug, Clone)]
struct Entry {
    value: String,
    expired_time: Option<std::time::Instant>,
}

async fn process_socket(
    socket: TcpStream,
    db: Arc<RwLock<HashMap<String, Entry>>>,
    cfg: Arc<RwLock<Args>>,
) -> io::Result<()> {
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
                    let value = {
                        let db = db.read().await;
                        db.get(&key).cloned()
                    };
                    let value = if let Some(entry) = value {
                        if let Some(expired_time) = entry.expired_time {
                            if expired_time < std::time::Instant::now() {
                                None
                            } else {
                                Some(entry.value)
                            }
                        } else {
                            Some(entry.value)
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
                    let expired_time = px.map(|px| std::time::Instant::now() + px);
                    {
                        let mut db = db.write().await;

                        db.insert(
                            key,
                            Entry {
                                value,
                                expired_time,
                            },
                        );
                    }
                    socket
                        .send(Message::SimpleStrings("OK".to_string()))
                        .await?;
                }
                Command::Config(ConfigComand::GET(key)) => {
                    info!("received command CONFIG GET with key: {}", key);
                    let cfg = cfg.read().await;
                    let value = match key.as_str() {
                        "dir" => cfg
                            .dir
                            .as_ref()
                            .map(|dir: &PathBuf| dir.to_string_lossy().to_string()),
                        "dbfilename" => cfg.dbfilename.as_ref().cloned(),
                        _ => None,
                    };
                    let bulk_strs =
                        vec![Message::BulkStrings(Some(key)), Message::BulkStrings(value)];
                    socket.send(Message::Arrays(bulk_strs)).await?;
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
