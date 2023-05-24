use tokio::{
    time::sleep,
    sync::{mpsc, broadcast, Mutex},
    net::TcpListener,
    io::{BufReader, AsyncWriteExt, AsyncBufReadExt},
};
use std::{sync::Arc, time::Duration};
use std::collections::HashMap;
use std::env;
use serde::{Serialize, Deserialize};
use serde_json;
use rand::random;

#[macro_use]
extern crate lazy_static;

lazy_static! {
    /// Port to port mapping, for which sockets should be proxied to each other.
    static ref PORT_MAPPINGS: HashMap<u64, u64> = if let Ok(var) = env::var("PORT_MAPPINGS") {
        let mut map = HashMap::new();
        let x: Vec<Vec<u64>> = serde_json::from_str(&var).expect("wrong config format");
        for mapping in x {
            if mapping.len() != 2 {
                panic!("wrong config format");
            }
            map.insert(mapping[0], mapping[1]);
            map.insert(mapping[1], mapping[0]);
        }
        map 
    } else {
        panic!("missing config")
    };
    static ref CLIENT_PORTS: Vec<u64> = if let Ok(var) = env::var("CLIENT_PORTS") {
        serde_json::from_str(&var).expect("wrong config format")
    } else {
        panic!("missing config")
    };
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: u64,
}

/// Same as in KV demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum APICommand {
    Append(KeyValue),
}

/// Same as in KV demo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum APIResponse {
    Decided(u64),
}

#[tokio::main]
async fn main() {
    // setup client sockets to talk to nodes
    let api_sockets = Arc::new(Mutex::new(HashMap::new()));
    for port in CLIENT_PORTS.iter() {
        let api_sockets = api_sockets.clone();
        tokio::spawn(async move {
            let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
            loop {
                let (socket, _addr) = listener.accept().await.unwrap();
                let (reader, writer) = socket.into_split();
                api_sockets.lock().await.insert(port, writer);
                // receiver actor
                tokio::spawn(async move {
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut data = vec![];
                        reader.read_until(b'\n', &mut data).await.unwrap();
                        let msg: APIResponse = serde_json::from_slice(&data).expect("could not deserialize command");
                        println!("From {}: {:?}", port, msg); // TODO: show in dashboard
                    }
                });
            }
        });
    }

    let api = api_sockets.clone();
    tokio::spawn(async move {
        // let mut rng = rand::rngs::();
        loop {
            sleep(Duration::from_millis(10)).await;
            for port in CLIENT_PORTS.iter() {
                if let Some(writer) = api.lock().await.get_mut(port) {
                    let cmd = APICommand::Append(KeyValue {key: random::<u64>().to_string(), value: random()});
                    let mut data = serde_json::to_vec(&cmd).expect("could not serialize cmd");
                    data.push(b'\n');
                    writer.write_all(&data).await.unwrap();
                }
            }
        }
    });
    
    // setup intra-cluster communication
    let partitions: Arc<Mutex<Vec<(u64, u64, f32)>>> = Arc::new(Mutex::new(vec![]));
    let mut out_channels = HashMap::new();
    for port in PORT_MAPPINGS.keys() {
        let (sender, _rec) = broadcast::channel::<Vec<u8>>(10000);
        let sender = Arc::new(sender);
        out_channels.insert(*port, sender.clone());
    }
    let out_channels = Arc::new(out_channels);

    let (central_sender, mut central_receiver) = mpsc::channel(10000);
    let central_sender = Arc::new(central_sender);

    for port in PORT_MAPPINGS.keys() {
        let out_chans = out_channels.clone();
        let central_sender = central_sender.clone();
        tokio::spawn(async move {
            let central_sender = central_sender.clone();
            let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();
            loop {
                let (socket, _addr) = listener.accept().await.unwrap();
                let (reader, mut writer) = socket.into_split();
                // sender actor
                let out_channels = out_chans.clone();
                tokio::spawn(async move {
                    let mut receiver = out_channels.get(port).unwrap().clone().subscribe();
                    while let Ok(data) = receiver.recv().await {
                        writer.write_all(&data).await.unwrap();
                    }
                });
                // receiver actor
                let central_sender = central_sender.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut data = vec![];
                        reader.read_until(b'\n', &mut data).await.unwrap();
                        if let Err(e) = central_sender.send((port, PORT_MAPPINGS.get(port).unwrap(), data)).await {
                            println!("DEBUG: senderror on central_sender: {:?}", e);
                        };
                    }
                });
            }
        });
    }

    // the one central actor that sees all messages
    while let Some((from_port, to_port, msg)) = central_receiver.recv().await {
        // drop message if network is partitioned between sender and receiver
        for (from, to, _probability) in partitions.lock().await.iter() {
            if from == from_port && to == to_port {
                continue
            }
        }
        let sender = out_channels.get(to_port).unwrap().clone();
        if let Err(e) = sender.send(msg) {
            println!("DEBUG: senderror on out_sender for port {:?}: {:?}", to_port, e);
        };
    }
}
