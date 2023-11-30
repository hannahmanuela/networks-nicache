use rdma_sys::*;
use std::ptr::null_mut;

use crate::rdma_utils::*;
use crate::kv_store::*;


fn setup_client_conn(
    id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: KVS,
) -> Result<(), Error> {
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

    // Client needs index base address, index remote key, soc values rkey, host values rkey
    // register mem containing pointer to index base
    let mut host_values_rkey_buf = kvs.host_values_rkey.to_le_bytes();
    let host_values_rkey_mem = reg_read(id, host_values_rkey_buf.as_ptr() as u64, host_values_rkey_buf.len()).unwrap();
    
    // officially accept client connection
    accept(id).unwrap();

    // send the host values remote key
    post_send_and_wait(id, &mut host_values_rkey_buf, host_values_rkey_mem, send_flags).unwrap();
    
    Ok(())
}

/// waits for the SoC to connect, then sends it the index base, and keys to read the index as well as the value(s)
fn setup_soc(kvs: &mut KVS, val_addr: u64, send_msg: [u8; 64],  listen_id: *mut rdma_cm_id, soc_addr: &str) -> Result<(), Error> {
    
    //wait for soc to connect
    listen(listen_id)?;
    let mut soc_id: *mut rdma_cm_id = null_mut();
    get_request(listen_id, &mut soc_id)?;

    // REGISTRATION STUFF

    // register the index
    let index_mem = reg_read(listen_id, kvs.host_index_base, INDEX_SIZE).unwrap();
    kvs.host_index_rkey = unsafe { (*index_mem).rkey };
    // register the value(s)
    let values_mem = reg_read(listen_id, val_addr, send_msg.len()).unwrap();
    kvs.host_values_rkey = unsafe { (*values_mem).rkey };

    // put all of the values into buffers, register those
    let mut host_index_base_buf = kvs.host_index_base.to_le_bytes();
    let host_index_base_mem = reg_read(soc_id, host_index_base_buf.as_ptr() as u64, host_index_base_buf.len()).unwrap();

    let mut host_index_rkey_buf = kvs.host_index_rkey.to_le_bytes();
    let host_index_rkey_mem = reg_read(soc_id, host_index_rkey_buf.as_ptr() as u64, host_index_rkey_buf.len()).unwrap();
    
    let mut host_values_rkey_buf = kvs.host_values_rkey.to_le_bytes();
    let host_values_rkey_mem = reg_read(soc_id, host_values_rkey_buf.as_ptr() as u64, host_values_rkey_buf.len()).unwrap();

    // accept connection from soc
    accept(listen_id)?;

    // post sends for all three
    post_send_and_wait(soc_id, &mut host_index_base_buf, host_index_base_mem, 0)?;
    post_send_and_wait(soc_id, &mut host_index_rkey_buf, host_index_rkey_mem, 0)?;
    post_send_and_wait(soc_id, &mut host_values_rkey_buf, host_values_rkey_mem, 0)?;
    
    Ok(())
}

fn run_host_listen(listen_id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: KVS) -> Result<(), Error> {

    // listen for incoming conns
    listen(listen_id).unwrap();

    loop {
        // put received conn in id
        let mut id: *mut rdma_cm_id = null_mut();
        get_request(listen_id, &mut id).unwrap();
        setup_client_conn(id, init, kvs).unwrap();
    }
}

pub fn run_host(host_addr: &str, soc_addr: &str, port: &str) -> Result<(), Error> {
    // create the KVS / index
    // - for this toy example, the host will populate all entries of the index with
    let test_str = "Hello from host!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].copy_from_slice(test_str);
    let val_addr = send_msg.as_ptr() as u64;
    
    let mut kvs = init_kv_store(false);
    put_addr_in_index_for_appropriate_keys(&kvs, val_addr, false);

    // setup rdma
    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 3;
    init.cap.max_recv_wr = 3;
    init.cap.max_send_sge = 3;
    init.cap.max_recv_sge = 3;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;

    // create new connection
    let mut listen_id: *mut rdma_cm_id = null_mut();
    get_new_cm_id(host_addr, port, &mut listen_id, &mut init, true).unwrap();

    setup_soc(&mut kvs, val_addr, send_msg, listen_id, soc_addr)?;

    run_host_listen(listen_id, &mut init, kvs)
}
