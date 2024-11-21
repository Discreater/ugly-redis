use anyhow::{bail, Context};
use futures_util::{SinkExt, StreamExt};
use hex_literal::hex;
use redis_starter_rust::{
    command::{ConfigSubCommand, ReplConfSubresponse, ReplconfSubcommand, ReqCommand, RespCommand},
    db::{Db, StreamEntry, Value},
    message::MessageFramer,
    rdb,
    replica::{ReplicaManager, ReplicaNotifyMessage},
};
use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddrV4},
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::{
    net::{TcpListener, TcpStream},
    select,
    sync::RwLock,
};
use tokio_util::codec;
use tracing::{error, info, trace, trace_span, warn, Instrument, Level};

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

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
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

    let self_addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), args.port);
    let listener = TcpListener::bind(&self_addr).await.unwrap();
    let db = Arc::new(RwLock::new(db));
    let config = Arc::new(args);

    let replica_manager = Arc::new(ReplicaManager::default());

    if let Some(replicaof) = config.replicaof.clone() {
        let db = db.clone();
        let config = config.clone();
        let replica_manager = replica_manager.clone();
        tokio::spawn(async move {
            let socket = handshake_with_master(replicaof, config.clone())
                .await
                .expect("handshake with master");
            process_client_socket::<false>(socket, db, config, replica_manager)
                .await
                .expect("process socket erorr");
        });
    }

    loop {
        let (socket, _) = listener.accept().await?;
        let addr = socket.peer_addr()?;
        let db = db.clone();
        let config = config.clone();
        let state = replica_manager.clone();
        tokio::spawn(
            async move {
                let socket = tokio_util::codec::Framed::new(socket, MessageFramer::default());
                process_client_socket::<true>(socket, db, config, state)
                    .await
                    .inspect_err(|e| error!("process socket error: {:?}", e))
                    .expect("process socket error")
            }
            .instrument(trace_span!("client", addr = %addr)),
        );
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
    mut socket: codec::Framed<TcpStream, MessageFramer>,
    db: Arc<RwLock<Db>>,
    cfg: Arc<Args>,
    replica_manager: Arc<ReplicaManager>,
) -> anyhow::Result<()> {
    let mut received_bytes = 0;
    while let Some(message) = socket.next().await {
        trace!("received message: {:?}", message);
        let (message, message_bytes) = message?;
        let req_command = message.parse_req()?;
        info!("parsed command: {:?}", req_command);
        match &req_command {
            ReqCommand::Ping => {
                if NOT_SLAVE {
                    socket.send(RespCommand::Pong.into()).await?;
                }
            }
            ReqCommand::Echo(data) => {
                if NOT_SLAVE {
                    socket.send(RespCommand::Bulk(data.clone()).into()).await?;
                }
            }
            ReqCommand::Get(key) => {
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
                            value.into()
                        }
                    } else {
                        value.into()
                    }
                } else {
                    RespCommand::Nil
                };
                if NOT_SLAVE {
                    socket.send(value.into()).await?;
                }
            }
            ReqCommand::Set { key, value, px } => {
                replica_manager
                    .notify(req_command.clone())
                    .await
                    .context("notify message to replica error")?;
                let expired_time = px.and_then(|px| {
                    (std::time::SystemTime::now() + px)
                        .duration_since(UNIX_EPOCH)
                        .ok()
                        .map(|d| d.as_millis() as u64)
                });
                {
                    let mut db = db.write().await;
                    db.kv.insert(key.clone(), Value::String(value.clone()));

                    if let Some(expired_time) = expired_time {
                        db.expire.insert(key.clone(), expired_time);
                    } else {
                        db.expire.remove_entry(key);
                    }
                }
                if NOT_SLAVE {
                    socket.send(RespCommand::Simple("OK").into()).await?;
                }
            }
            ReqCommand::Config(ConfigSubCommand::GET(key)) => {
                let value = match key.as_str() {
                    "dir" => cfg.dir.clone(),
                    "dbfilename" => cfg.dbfilename.clone(),
                    _ => None,
                };
                if NOT_SLAVE {
                    socket
                        .send(RespCommand::Bulks(vec![Some(key.clone()), value]).into())
                        .await?;
                }
            }
            ReqCommand::KEYS(pattern) => {
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
                    socket.send(RespCommand::Bulks(keys).into()).await?;
                }
            }
            ReqCommand::Info(section) => match section.as_ref().map(|s| s.as_str()) {
                Some("replication") => {
                    let info = replication_info(&cfg)
                        .into_iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<String>>()
                        .join("\r\n");
                    socket.send(RespCommand::Bulk(info).into()).await?;
                }
                None => {
                    let info = all_info(&cfg)
                        .into_iter()
                        .map(|(k, v)| format!("{}:{}", k, v))
                        .collect::<Vec<String>>()
                        .join("\r\n");
                    socket.send(RespCommand::Bulk(info).into()).await?;
                }
                s => {
                    error!("unsupported INFO section: {:?}", s);
                }
            },
            ReqCommand::Replconf(subc) => {
                if NOT_SLAVE {
                    socket.send(RespCommand::Ok.into()).await?;
                } else {
                    match subc {
                        ReplconfSubcommand::Getack => {
                            socket
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
            ReqCommand::Psync { .. } => {
                socket
                    .send(
                        RespCommand::FullResync {
                            repl_id: REPLICATION_ID.to_string(),
                            offset: 0,
                        }
                        .into(),
                    )
                    .await?;
                socket
                    .send(RespCommand::RdbFile(EMPTY_RDB_CONTENT.to_vec()).into())
                    .await?;

                return process_replica_socket(socket, replica_manager).await;
            }
            ReqCommand::Wait {
                number_replicas,
                time_out,
            } => {
                if NOT_SLAVE {
                    // replica number is assert to small than i64::max;
                    let replicas = replica_manager.wait(*number_replicas, *time_out).await?;
                    socket
                        .send(RespCommand::Int(replicas as i64).into())
                        .await?;
                }
            }
            ReqCommand::Type(key) => {
                if NOT_SLAVE {
                    let ty = {
                        let db = db.read().await;
                        Value::ty(db.kv.get(key))
                    };
                    socket.send(RespCommand::Simple(ty).into()).await?
                }
            }
            ReqCommand::XADD {
                stream_key,
                entry_id,
                pairs,
            } => {
                if NOT_SLAVE {
                    if let Some(entry_id) = entry_id {
                        {
                            let mut db = db.write().await;
                            let entry = db
                                .kv
                                .entry(stream_key.clone())
                                .or_insert_with(|| Value::Stream(vec![]));
                            match entry {
                                Value::Stream(stream) => {
                                    stream.push(StreamEntry {
                                        id: entry_id.to_string(),
                                        pairs: pairs.clone(),
                                    });
                                }
                                _ => bail!("value of key '{stream_key}' is not a stream!")
                            }
                        }
                        socket
                            .send(RespCommand::Bulk(entry_id.to_string()).into())
                            .await?
                    }
                }
            }
            cmd => {
                error!("unsupported command: {:?}", cmd);
            }
        }
        received_bytes += message_bytes;
    }
    Ok(())
}

async fn process_replica_socket(
    socket: codec::Framed<TcpStream, MessageFramer>,
    manager: Arc<ReplicaManager>,
) -> anyhow::Result<()> {
    info!("replica connected.");
    let (mut socket_tx, mut socket_rx) = socket.split();
    let mut replica_broadcast_rx = manager.subscribe();

    loop {
        select! {
            message = replica_broadcast_rx.recv() => {
                let propagated = message?;
                trace!("replica received broadcast message: {:?}", propagated);
                match propagated {
                    ReplicaNotifyMessage::Propagated(propagated) => {
                        socket_tx.send(propagated).await?;
                    }
                    ReplicaNotifyMessage::Wait => {
                        socket_tx
                        .send(ReqCommand::Replconf(ReplconfSubcommand::Getack).into())
                        .await?;
                    }
                }
            }
            message = socket_rx.next() => {
                trace!("received message: {:?}", message);
                let (message, _) = message.context("connection closed by remote")??;
                let resp_command = message.parse_resp()?;
                info!("parsed command: {resp_command:?}");
                match &resp_command {
                    RespCommand::Replconf(ReplConfSubresponse::Ack(n)) => {
                        manager.ack(*n).await;
                    }
                    resp => {
                        bail!("unaccpet response in replica: {resp:?}")
                    }
                }
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
