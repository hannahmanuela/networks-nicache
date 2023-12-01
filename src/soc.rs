use rdma_sys::*;
use std::ptr::null_mut;

use crate::rdma_utils::*;
use crate::kv_store::*;

/// processes a single request, whose communication id is in id
fn setup_client_conn(
    id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: KVS,
) -> Result<(), Error> {
    
    // ---------------------------------------
    //      HANDLE CONN -- SETUP
    // ---------------------------------------

    // "gets the attributes specified in attr_mask for the
    //    new conns qp and returns them through the pointers qp_attr (which we never use again) and init"
    let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    query_qp(id, &mut qp_attr, init).unwrap();

    // check that the queue allows for at least 64 bits inline
    // TODO why 64? What's this sge which presumable is the default?
    let mut send_flags = 0;
    if init.cap.max_inline_data >= 64 {
        send_flags = ibv_send_flags::IBV_SEND_INLINE.0;
    } else {
        println!("rdma server: device doesn't support IBV_SEND_INLINE, using sge sends");
    }

    // ---------------------------------------
    //      HANDLE CONN -- REGISTER
    // ---------------------------------------

    // Client needs index base address, index remote key, values rkey
    println!("sending index base addr: 0x{:x}", kvs.soc_index_base);
    let mut index_base_buf = kvs.soc_index_base.to_le_bytes();
    let index_mem = reg_read(id, kvs.soc_index_base, 256*8).unwrap();
    let mut index_rkey_buf = kvs.soc_index_rkey.to_le_bytes();
    let index_rkey_mem = reg_read(id, index_rkey_buf.as_ptr() as u64, index_rkey_buf.len()).unwrap();
    let mut values_rkey_buf = kvs.soc_values_rkey.to_le_bytes();
    let values_rkey_mem = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();
    
    // ---------------------------------------
    //      HANDLE CONN -- COMMUNICATE
    // ---------------------------------------

    // officially accept client connection
    accept(id).unwrap();

    // send the index address (this posts it to the send queue)
    post_send_and_wait(id, &mut index_base_buf, index_mem, send_flags).unwrap();
    // send the index rkey
    post_send_and_wait(id, &mut index_rkey_buf, index_rkey_mem, send_flags).unwrap();
    // send the cached values remote key
    post_send_and_wait(id, &mut values_rkey_buf, values_rkey_mem, send_flags).unwrap();

    // send the value region remote key
    Ok(())

}


/// runs a central server listener, that listens on the listen_id socket and processes requests as they come in
fn run_soc_client_listen(
    listen_id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: KVS,
) -> Result<(), Error> {

    println!("listening for client conns");
    // listen for incoming conns
    listen(listen_id).unwrap();

    loop {
        // put received conn in id
        let mut id: *mut rdma_cm_id = null_mut();
        get_request(listen_id, &mut id).unwrap();
        println!("got client conn");
        setup_client_conn(id, init, kvs).unwrap();
    }
}

/// register memory that clients will need access to
/// right now is just the index, and the dummy value
fn register_mem(
    id: *mut rdma_cm_id,
    kvs: &mut KVS,
    val_addr: u64,
    val_size: usize
) -> Result<(), Error> {
    // register the index
    let index_mem = reg_read(id, kvs.soc_index_base, INDEX_SIZE).unwrap();
    // register the values (which for now will just be the test string)
    let values_mem = reg_read(id, val_addr, val_size).unwrap();

    kvs.soc_index_rkey = unsafe { (*index_mem).rkey };
    kvs.soc_values_rkey = unsafe { (*values_mem).rkey };

     Ok(())
}

///wait for the host to connect, then gets its partial KVStore?
fn setup_host(
    host_conn_id: *mut rdma_cm_id,
    kvs: &mut KVS,
) -> Result<(), Error> {

    // REGISTER MEM

     // register mem region to hold addr of host's index base
     let mut host_index_base_buf = [0u8; 8];
     let index_base_mr = reg_read(host_conn_id, host_index_base_buf.as_ptr() as u64, host_index_base_buf.len()).unwrap();
     // register mem for index rkey
     let mut host_index_rkey_buf = [0u8; 4];
     let index_rkey_mr = reg_read(host_conn_id, host_index_rkey_buf.as_ptr() as u64, host_index_rkey_buf.len()).unwrap();
     // register mem for values rkey
     let mut host_values_rkey_buf = [0u8; 4];
     let values_rkey_mr = reg_read(host_conn_id, host_values_rkey_buf.as_ptr() as u64, host_values_rkey_buf.len()).unwrap();
     
    // GET VALUES
    // connect using socket in id
    connect(host_conn_id).unwrap();
    println!("connected to host");
 
    // wait for host to send remote addr - post it to recv queue
    post_recv_and_wait(host_conn_id, &mut host_index_base_buf, index_base_mr).unwrap();
    post_recv_and_wait(host_conn_id, &mut host_index_rkey_buf, index_rkey_mr).unwrap();
    post_recv_and_wait(host_conn_id, &mut host_values_rkey_buf, values_rkey_mr).unwrap();

    println!("got vals from host");

    // write gotten values into kvs
    kvs.host_index_base = u64::from_le_bytes(host_index_base_buf);
    kvs.host_index_rkey = u32::from_le_bytes(host_index_rkey_buf);
    kvs.host_values_rkey = u32::from_le_bytes(host_values_rkey_buf);


    // register mem for soc's index which we will now write hosts values into
    let mut curr_val_addr_buf: [u8; 8] = [0u8; 8];
    let curr_value_mr = reg_read(host_conn_id, curr_val_addr_buf.as_ptr() as u64, curr_val_addr_buf.len()).unwrap();

    println!("index base addr: 0x{:x}", kvs.soc_index_base);
    // read index from host
    for key_val in 0..N_KEYS_ON_HOST as u64 {
        // read addresses (already serialized) into buffer
        post_read_and_wait(host_conn_id, &mut curr_val_addr_buf, curr_value_mr, kvs.host_index_base + ((8 * key_val) as u64), kvs.host_index_rkey).unwrap();
        // write address to soc's index
        let offset = key_val * 8;
        let ass_addr = (kvs.soc_index_base + offset) as *mut u64;
        println!("writing host val addr at index addr: 0x{:x}", kvs.soc_index_base + offset);
        unsafe { *ass_addr =  u64::from_le_bytes(curr_val_addr_buf) };
    }

    println!("wrote to index");

    Ok(())
}



/// sets up the soc rdma connection, then listens for incoming connections and processes them
pub fn run_soc(host_addr: &str, soc_addr: &str, port: &str) -> Result<(), Error> {

    // inits a soc-local view of the kv store
    let mut kvs = init_kv_store(true);

    // SETTING UP -- conn to host
    let mut host_init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    host_init.cap.max_send_wr = 3;
    host_init.cap.max_recv_wr = 3;
    host_init.cap.max_send_sge = 3;
    host_init.cap.max_recv_sge = 3;
    host_init.cap.max_inline_data = 64;
    host_init.sq_sig_all = 1;
    
    println!("setting up host conn");
    let mut host_id: *mut rdma_cm_id = null_mut();
    get_new_cm_id(host_addr, port, &mut host_id, &mut host_init, false)?;
    setup_host(host_id, &mut kvs).unwrap();

    // need to now write full index
    let test_str = "Hello from soc!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].copy_from_slice(test_str);
    let val_addr = send_msg.as_ptr() as u64;

    put_addr_in_index_for_appropriate_keys(&kvs, val_addr, true);

    // ready to accept client conns
    let mut client_init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    client_init.cap.max_send_wr = 3;
    client_init.cap.max_recv_wr = 3;
    client_init.cap.max_send_sge = 3;
    client_init.cap.max_recv_sge = 3;
    client_init.cap.max_inline_data = 64;
    client_init.sq_sig_all = 1;
    let mut client_listen_id: *mut rdma_cm_id = null_mut();
    get_new_cm_id(soc_addr, port, &mut client_listen_id, &mut client_init, true)?;
    
    // register memory that clients will need
    register_mem(client_listen_id, &mut kvs, val_addr, send_msg.len()).unwrap();
    // loop to accept client connections
    run_soc_client_listen(client_listen_id, &mut client_init, kvs)
}
