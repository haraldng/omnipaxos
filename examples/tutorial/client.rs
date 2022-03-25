use std::io;
use std::net::SocketAddr;

use tokio::io::{AsyncWriteExt, AsyncBufReadExt, BufReader};
use tokio::net::{TcpStream, TcpListener};
use tokio::sync::mpsc;

mod messages;
use crate::messages::*;
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>{

    let (tx, mut rx) = mpsc::channel::<String>(24);
    // tokio::spawn(async move{

    // })

    
    // tokio::select!{
    //     result = 
    // }
    println!("client boot succesfully!");
    println!("please enter command: --id --r/w --key --value");
    loop {
        //get user input
        let mut buff = String::new(); 
        io::stdin().read_line(&mut buff).expect("Reading from stdin failed");
        let cmd:Vec<&str> = buff.split(" ").collect();
        //check port
        if let Ok(id) = cmd[0].parse::<u64>(){
            let mut addr = "127.0.0.1:".to_string();
            let port = 8080 + id;
            addr += &port.to_string();
            let addr: SocketAddr = addr.parse().unwrap();

            //check operation
            match cmd[1]{
                "r" =>{
                    // //create message
                    // let messages = CMDMessage{
                    //     operation: Operaiton::Read,
                    //     kv
                    // }
                    // //wrap messages
                    // let wrapped_msg = Package{
                    //     types: Types::CMD,
                    //     msg: Msg::CMD(message),
                    // };
                }
                "w" =>{
                    // //create message
                    // let messages = CMDMessage{
                    //     operation: Operaiton::Write,
                    //     kv
                    // }
                    // //wrap messages
                    // let wrapped_msg = Package{
                    //     types: Types::CMD,
                    //     msg: Msg::CMD(message),
                    // };
                }
                other => {
                    println!("illegal operation");
                }
            }
        }else {
            println!("illegal node");
        }
        println!("please enter command: --id --r/w --key --value");

    }


}