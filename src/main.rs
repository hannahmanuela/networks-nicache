use clap::Parser;


mod kv_store;
mod client;
mod rdma_utils;
mod soc;
mod host;
pub use rdma_utils::*;
pub use kv_store::*;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    host: bool,
    #[arg(long)]
    soc: bool,
    #[arg(long)]
    client:bool,
    soc_addr: String,
    host_addr: String, 
    #[arg(default_value = "8080")]
    port: String, // just have soc and host use the same port for now
    #[arg(default_value = "100")]
    ratio: u64,
    #[arg(long)]
    one_value: bool,
}

// the order is: host runs, soc runs and connects to host, client runs and connects to soc and host
// (this is fine because the soc won't listen for connections until it connects to the host)

// when the host/soc start, they will need to provide both their own address and the other's
// address so they can communicate to set things up.

// here is the flow of the whole thing:
// host starts, creates its own index and populates it with values, listen for soc connection
// soc starts, connects to host
// soc reads index from host, filling keys with some of its own values
// client starts, connects to soc (needs host and client addresses)
// client connects to host
// client gets the base address of the cached index, using the cached bit to determine whether
//   the key is on the host or the soc

fn main() {
    // initialize args
    let mut args = Args::parse();

    args.port.push_str("\0");
    let port = args.port.as_str();

    args.host_addr.push_str("\0");
    let host_addr = args.host_addr.as_str();

    args.soc_addr.push_str("\0");
    let soc_addr = args.soc_addr.as_str();
    
    if args.host {
	    println!("Starting host KVS");
        host::run_host(host_addr, port, args.ratio, args.one_value).unwrap();
    } else if args.soc {
	    println!("Starting SoC KVS");
        soc::run_soc(host_addr, soc_addr, port, args.ratio, args.one_value).unwrap();
    } else if args.client {
	    println!("Starting client"); 
        client::run_client(soc_addr, port, host_addr, port).unwrap();
    }
}
