mod resp;
mod utils;

use futures_util::{SinkExt, StreamExt};
use resp::{Command, MessageFramer};
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::RwLock,
};
use tracing::{error, info, trace, Level};

use resp::Message;

#[tokio::main]
async fn main() -> io::Result<()> {
    let log_level = if std::env::var("REDIS_LOG").is_ok() {
        Level::TRACE
    } else {
        Level::WARN
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db = Arc::new(RwLock::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        tokio::spawn(async move {
            process_socket(socket, db)
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
            Message::Arrays(messages) => {
                match Command::from(messages)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid messages"))?
                {
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
                }
            }
            _ => {
                error!("unsupported message type: {:?}", message);
            }
        }
    }
}
