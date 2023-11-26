use std::collections::HashMap;
use async_rdma::{LocalMrReadAccess, LocalMrWriteAccess, RdmaBuilder};



struct KVEntry {
    value: String,
    
}

struct KVKey {
    key: u32,
    is_cached: bool,
    num_accesses: u32
}


fn busy_poll_memory() {

    


}


fn main() {

    // let mut kv_store = HashMap::<KVKey, KVEntry>::new();


    while true {
        busy_poll_memory();
    }
    
}


