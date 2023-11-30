use rdma_sys::*;
use std::ptr::null_mut;

use crate::rdma_utils::*;
use crate::kv_store::*;

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
    Ok(())
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
