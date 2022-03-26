use core::time;
use std::{io, thread};
use std::net::SocketAddr;

use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::mpsc;

mod messages;
use crate::messages::*;

const CLIENT: &str = "127.0.0.1:8000";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{

    //lisent for incoming result from kv store
    tokio::spawn(async move{
        let listener = TcpListener::bind(CLIENT).await.unwrap();

        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (r, _) = socket.split();
            let mut reader = BufReader::new(r);
            let mut buf = String::new();
            loop {
                let bytes_read = reader.read_line(&mut buf).await.unwrap();
                    //EOF
                    if bytes_read == 0{
                        break;
                    }
                println!("{}",&&buf);
            }
        }
    });

    println!("client boot succesfully!");
    //println!("please enter command: --id --r/w/s --key --value");
    loop {
        println!("please enter command: --id --r/w/s --key --value");
        //get user input
        let mut buff = String::new(); 
        io::stdin().read_line(&mut buff).expect("Reading from stdin failed");
        //replace the CRLF
        // println!("before: {:?}",buff);
        let buff = buff.replace("\r\n", "");
        //println!("after: {:?}",buff);
        let cmd:Vec<&str> = buff.split(" ").collect();
        //check port
        if let Ok(id) = cmd[0].parse::<u64>(){
            let mut addr = "127.0.0.1:".to_string();
            let port = 8080 + id;
            addr += &port.to_string();
            let addr: SocketAddr = addr.parse().unwrap();
            // println!("index: 0---{}",cmd[0]);
            // println!("index: 1---{}",cmd[1]);
            // println!("index: 2---{}",cmd[2]);
            // println!("index: 3---{}",cmd[3]);
            //check operation
            match cmd[1]{
                "r" =>{
                    // //create message
                    let message = CMDMessage{
                        operation: Operaiton::Read,
                        kv: KeyValue {
                            key: String::from(cmd[2]),
                            value: 0,
                        },
                    };
                    // //wrap messages
                    let wrapped_msg = Package{
                        types: Types::CMD,
                        msg: Msg::CMD(message),
                    };
                    //serialization
                    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
                    //send
                    if let Ok(mut tcp_stream) = TcpStream::connect(addr).await{
                        let (_, mut w) = tcp_stream.split();
                        w.write_all(serialized.as_bytes()).await.unwrap();
                    }else {
                        // println!("romote ip: {}", addr);
                        // println!("message: {:?}",serialized);
                        println!("Network failure");
                    }
                }
                
                "w" =>{
                    //check length
                    if cmd.len() != 4{
                        println!("illegal operation");
                        continue;
                    }
                    if let Ok(v) = cmd[3].parse(){
                        // //create message
                        let message = CMDMessage{
                            operation: Operaiton::Write,
                            kv: KeyValue {
                                key: String::from(cmd[2]),
                                value: v,
                            },
                        };
                        let wrapped_msg = Package{
                            types: Types::CMD,
                            msg: Msg::CMD(message),
                        };
                        //serialization
                        let serialized = serde_json::to_string(&wrapped_msg).unwrap();
                        //send
                        if let Ok(mut tcp_stream) = TcpStream::connect(addr).await{
                            let (_, mut w) = tcp_stream.split();
                            w.write_all(serialized.as_bytes()).await.unwrap();
                        }else {
                            // println!("romote ip: {}", addr);
                            // println!("message: {:?}",serialized);
                            println!("Network failure");
                        }
                    }else{
                        println!("illegal write value");
                    }
                }
                "s" =>{
                    // //create message
                    let message = CMDMessage{
                        operation: Operaiton::Snap,
                        kv: KeyValue {
                            key: String::from("snapshot"),
                            value: 0,
                        },
                    };
                    let wrapped_msg = Package{
                        types: Types::CMD,
                        msg: Msg::CMD(message),
                    };
                    //serialization
                    let serialized = serde_json::to_string(&wrapped_msg).unwrap();
                    //send
                    if let Ok(mut tcp_stream) = TcpStream::connect(addr).await{
                        let (_, mut w) = tcp_stream.split();
                        w.write_all(serialized.as_bytes()).await.unwrap();
                    }else {
                        println!("Network failure");
                    }
                }
                _other => {
                    println!("illegal operation");
                    continue;
                }
            }
        }else {
            println!("illegal node");
        }
        //sleep for wait the message from kv store
        thread::sleep(time::Duration::from_millis(10));
    }


}