mod resp;
mod utils;

use futures_util::{SinkExt, StreamExt};
use resp::{Command, Commander, MessageFramer};
use std::{collections::HashMap, io, sync::Arc};
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

async fn process_socket(
    socket: TcpStream,
    db: Arc<RwLock<HashMap<String, String>>>,
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
                for command in Commander::new(messages) {
                    match command {
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
                            socket.send(Message::BulkStrings(value)).await?;
                        }
                        Command::Set { key, value } => {
                            info!(
                                "received command SET with key: {} and value: {}",
                                key, value
                            );
                            {
                                let mut db = db.write().await;
                                db.insert(key, value);
                            }
                            socket
                                .send(Message::SimpleStrings("OK".to_string()))
                                .await?;
                        }
                    }
                }
            }
            _ => {
                error!("unsupported message type: {:?}", message);
            }
        }
    }
}
