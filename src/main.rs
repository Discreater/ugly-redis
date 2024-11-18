use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use hex_literal::hex;
use redis_starter_rust::{
    command::{ConfigSubCommand, ReplConfSubresponse, ReplconfSubcommand, ReqCommand, RespCommand},
    db::Db,
    message::MessageFramer,
    rdb,
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, Mutex, RwLock},
};
use tokio_util::codec;
use tracing::{error, info, trace, warn, Level};

use clap::Parser;

const REPLICATION_ID: &str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

const EMPTY_RDB_CONTENT: &[u8] = &hex!("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2");

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    dir: Option<String>,
    #[arg(long)]
    dbfilename: Option<String>,
    #[arg(long, default_value = "6379")]
    port: u16,
    #[arg(long, value_parser = parse_socket_addr_v4)]
    replicaof: Option<SocketAddrV4>,
}

fn parse_socket_addr_v4(s: &str) -> Result<SocketAddrV4, String> {
    let (ip, port) = s.split_once(" ").ok_or("invalid socket address")?;
    let ip = if ip == "localhost" {
        Ipv4Addr::LOCALHOST
    } else {
        ip.parse().map_err(|_| "invalid ip address")?
    };
    let port = port.parse().map_err(|_| "invalid port")?;
    Ok(SocketAddrV4::new(ip, port))
}

impl Args {
    fn is_master(&self) -> bool {
        self.replicaof.is_none()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let log_level = if std::env::var("REDIS_LOG").is_ok() {
        Level::TRACE
    } else {
        Level::WARN
    };
    tracing_subscriber::fmt().with_max_level(log_level).init();
    let args = Args::parse();
    let db =
        rdb::init(&args.dir, &args.dbfilename).context("init from redis database file error")?;

    let listener = TcpListener::bind(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), args.port))
        .await
        .unwrap();
    let db = Arc::new(RwLock::new(db));
    let config = Arc::new(args);

    let state = Arc::new(State::default());
    if let Some(replicaof) = config.replicaof.clone() {
        let db = db.clone();
        let config = config.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let socket = handshake_with_master(replicaof, config.clone())
                .await
                .expect("handshake with master");
            process_client_socket::<false>(socket, db, config, state)
                .await
                .expect("process socket erorr");
        });
    }

    loop {
        let (socket, _) = listener.accept().await?;
        let db = db.clone();
        let config = config.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let socket = tokio_util::codec::Framed::new(socket, MessageFramer::default());
            process_client_socket::<true>(socket, db, config, state)
                .await
                .expect("process socket error")
        });
    }
}

const BROADCAST_CAPACITY: usize = 64;

#[derive(Debug)]
struct State {
    tx: broadcast::Sender<ReqCommand>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            tx: broadcast::Sender::new(BROADCAST_CAPACITY),
        }
    }
}

async fn handshake_with_master(
    replicaof: SocketAddrV4,
    config: Arc<Args>,
) -> anyhow::Result<codec::Framed<TcpStream, MessageFramer>> {
    let master = TcpStream::connect(replicaof)
        .await
        .context("cannot connect to master")?;
    let mut master = tokio_util::codec::Framed::new(master, MessageFramer::default());
    master.send(ReqCommand::Ping.into()).await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")?
        .context("decode response message error")?
        .0
        .parse_resp()?;
    trace!("response of ping: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Pong));

    master
        .send(ReqCommand::Replconf(ReplconfSubcommand::ListeningPort(config.port)).into())
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of replconf: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Ok));

    master
        .send(ReqCommand::Replconf(ReplconfSubcommand::Capa("psync2".to_string())).into())
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of replconf: {:?}", response);
    debug_assert!(matches!(response, RespCommand::Ok));

    master
        .send(
            ReqCommand::Psync {
                id: None,
                offset: None,
            }
            .into(),
        )
        .await?;
    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of psync: {:?}", response);
    debug_assert!(matches!(response, RespCommand::FullResync { .. }));

    let response = master
        .next()
        .await
        .context("connection closed by master")??
        .0
        .parse_resp()?;
    trace!("response of handshake: {:?}", response);
    assert!(matches!(response, RespCommand::RdbFile(_)));
    Ok(master)
}

async fn process_client_socket<const NOT_SLAVE: bool>(
    socket: codec::Framed<TcpStream, MessageFramer>,
    db: Arc<RwLock<Db>>,
    cfg: Arc<Args>,
    state: Arc<State>,
) -> anyhow::Result<()> {
    let (sender, mut receiver) = socket.split();

    let sender = Arc::new(Mutex::new(sender));

    let mut replica_task = None;
    let mut received_bytes = 0;
    while let Some(message) = receiver.next().await {
        trace!("received message: {:?}", message);
        let (message, message_bytes) = message?;
        let req_command = message.parse_req()?;
        match &req_command {
            ReqCommand::Ping => {
                info!("received command PING");
                if NOT_SLAVE {
                    sender.lock().await.send(RespCommand::Pong.into()).await?;
                }
            }
            ReqCommand::Echo(data) => {
                info!("received command ECHO with data: {}", data);
                if NOT_SLAVE {
                    sender
                        .lock()
                        .await
                        .send(RespCommand::Bulk(data.clone()).into())
                        .await?;
                }
            }
            ReqCommand::Get(key) => {
                info!("received command GET with key: {}", key);
                let (value, expire) = {
                    let db = db.read().await;
                    (db.kv.get(key).cloned(), db.expire.get(key).cloned())
                };
                let value = if let Some(value) = value {
                    if let Some(expire) = expire {
                        if UNIX_EPOCH + Duration::from_millis(expire) < std::time::SystemTime::now()
                        {
                            RespCommand::Nil
                        } else {
                            RespCommand::Bulk(value)
                        }
                    } else {
                        RespCommand::Bulk(value)
                    }
                } else {
                    RespCommand::Nil
                };
                if NOT_SLAVE {
                    sender.lock().await.send(value.into()).await?;
                }
            }
            ReqCommand::Set { key, value, px } => {
                info!(
                    "received command SET with key: {} and value: {}, px: {:?}",
                    key, value, px
                );
                if state.tx.receiver_count() != 0 {
                    state
                        .tx
                        .send(req_command.clone())
                        .context("send message to replica error")?;
                }
                let expired_time = px.and_then(|px| {
                    (std::time::SystemTime::now() + px)
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .map(|d| d.as_millis() as u64)
                });
                {
                    let mut db = db.write().await;
                    db.kv.insert(key.clone(), value.clone());

                    if let Some(expired_time) = expired_time {
                        db.expire.insert(key.clone(), expired_time);
                    } else {
                        db.expire.remove_entry(key);
                    }
                }
                if NOT_SLAVE {
                    sender
                        .lock()
                        .await
                        .send(RespCommand::Simple("OK").into())
                        .await?;
                }
            }
            ReqCommand::Config(ConfigSubCommand::GET(key)) => {
                info!("received command CONFIG GET with key: {}", key);
                let value = match key.as_str() {
                    "dir" => cfg.dir.clone(),
                    "dbfilename" => cfg.dbfilename.clone(),
                    _ => None,
                };
                if NOT_SLAVE {
                    sender
                        .lock()
                        .await
                        .send(RespCommand::Bulks(vec![Some(key.clone()), value]).into())
                        .await?;
                }
            }
            ReqCommand::KEYS(pattern) => {
                info!("received commadn KEYS with pattern: {}", pattern);
                let keys = {
                    let db = db.read().await;
                    db.kv.keys().cloned().collect::<Vec<String>>()
                };
                let keys = keys
                    .into_iter()
                    .filter(|key| simple_pattern_match(&pattern, key))
                    .map(Option::Some)
                    .collect();
                if NOT_SLAVE {
                    sender
                        .lock()
                        .await
                        .send(RespCommand::Bulks(keys).into())
                        .await?;
                }
            }
            ReqCommand::Info(section) => {
                info!("received command INFO with section: {:?}", section);
                match section.as_ref().map(|s| s.as_str()) {
                    Some("replication") => {
                        let info = replication_info(&cfg)
                            .into_iter()
                            .map(|(k, v)| format!("{}:{}", k, v))
                            .collect::<Vec<String>>()
                            .join("\r\n");
                        sender
                            .lock()
                            .await
                            .send(RespCommand::Bulk(info).into())
                            .await?;
                    }
                    None => {
                        let info = all_info(&cfg)
                            .into_iter()
                            .map(|(k, v)| format!("{}:{}", k, v))
                            .collect::<Vec<String>>()
                            .join("\r\n");
                        sender
                            .lock()
                            .await
                            .send(RespCommand::Bulk(info).into())
                            .await?;
                    }
                    s => {
                        error!("unsupported INFO section: {:?}", s);
                    }
                }
            }
            ReqCommand::Replconf(subc) => {
                info!("recieved command REPLCONF: {:?}", subc);
                if NOT_SLAVE {
                    sender.lock().await.send(RespCommand::Ok.into()).await?;
                } else {
                    match subc {
                        ReplconfSubcommand::Getack => {
                            sender
                                .lock()
                                .await
                                .send(
                                    RespCommand::Replconf(ReplConfSubresponse::Ack(received_bytes))
                                        .into(),
                                )
                                .await?;
                        }
                        _ => warn!("only response getack when in replica mode"),
                    }
                }
            }
            ReqCommand::Psync { id, offset } => {
                info!("recieved command PSYNC, id: {:?}, offset: {:?}", id, offset);
                sender
                    .lock()
                    .await
                    .send(
                        RespCommand::FullResync {
                            repl_id: REPLICATION_ID.to_string(),
                            offset: 0,
                        }
                        .into(),
                    )
                    .await?;
                sender
                    .lock()
                    .await
                    .send(RespCommand::RdbFile(EMPTY_RDB_CONTENT.to_vec()).into())
                    .await?;

                if replica_task.is_none() {
                    let sender_in_replica = Arc::clone(&sender);
                    let state_in_recv = Arc::clone(&state);
                    replica_task = Some(tokio::spawn(async move {
                        let sender = sender_in_replica;
                        let state = state_in_recv;
                        let mut receiver = state.tx.subscribe();
                        while let Ok(message) = receiver.recv().await {
                            trace!("send message to replica: {:?}", message);
                            sender.lock().await.send(message.into()).await.unwrap();
                        }
                    }));
                }
            }
            ReqCommand::Wait { n0, n1 } => {
                info!("received command WAIT, n0: {}, n1: {}", n0, n1);
                if NOT_SLAVE {
                    // replica number is assert to small than i64::max;
                    let replicas = state.tx.receiver_count() as i64;
                    sender
                        .lock()
                        .await
                        .send(RespCommand::Int(replicas as i64).into())
                        .await?;
                }
            }
            cmd => {
                error!("unsupported command: {:?}", cmd);
            }
        }
        received_bytes += message_bytes;
    }
    if let Some(replic_task) = replica_task {
        replic_task.abort();
    }
    Ok(())
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
