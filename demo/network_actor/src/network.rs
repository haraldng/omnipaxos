use tokio::{
    time::sleep,
    sync::{mpsc, broadcast, Mutex},
    net::TcpListener,
    io::{BufReader, AsyncWriteExt, AsyncBufReadExt},
};
use std::{sync::Arc, time::Duration};
use std::collections::HashMap;
use serde_json;
use rand::random;

use crate::{Message, KeyValue, CLIENT_PORTS, PORT_MAPPINGS, KVCommand};

pub async fn run() {
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
                        let msg: Message = serde_json::from_slice(&data).expect("could not deserialize command");
                        println!("From {}: {:?}", port, msg); // TODO: handle APIResponse
                    }
                });
            }
        });
    }

    let api = api_sockets.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_millis(10)).await;
            for port in CLIENT_PORTS.iter() {
                if let Some(writer) = api.lock().await.get_mut(port) {
                    let cmd = Message::APICommand(KVCommand::Put(KeyValue {key: random::<u64>().to_string(), value: random::<u64>().to_string()}));
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
