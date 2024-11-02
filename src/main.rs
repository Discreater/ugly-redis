mod resp;

use futures_util::{SinkExt, StreamExt};
use resp::{Command, Commander, MessageFramer};
use std::io;
use tokio::net::{TcpListener, TcpStream};
use tracing::{error, info, trace, Level};

use resp::Message;

#[tokio::main]
async fn main() -> io::Result<()> {
    if let Ok(_) = std::env::var("REDIS_LOG") {
        tracing_subscriber::fmt()
            .with_max_level(Level::TRACE)
            .init();
    }

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move { process_socket(socket).await.expect("process socket error") });
    }
}

async fn process_socket(socket: TcpStream) -> io::Result<()> {
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
                    }
                }
            }
            _ => {
                error!("unsupported message type: {:?}", message);
            }
        }
    }
}
