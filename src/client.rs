use std::ptr::null_mut;
use rdma_sys::*;
use crate::{rdma_utils::*, deserialize_kv_addr};


struct Connection {
    conn_id : *mut rdma_cm_id,
    index_base: u64,
    index_read_key: u32,
    addr_mr: *mut ibv_mr,
    val_read_key: u32,
    val_mr: *mut ibv_mr,
}

fn setup_host_conn(addr: &str, port: &str, val_buf: &mut [u8; 64]) -> Result<Connection, Error> {
    // initalize queues
    // TODO: do we like these values long-term
    let mut id: *mut rdma_cm_id = null_mut();
    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 2;
    init.cap.max_recv_wr = 2;
    init.cap.max_send_sge = 2;
    init.cap.max_recv_sge = 2;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;
    init.qp_context = id.cast();

    // create a socket in id
    get_new_cm_id(addr, port, &mut id, &mut init, false).unwrap();

    // ---------------------------------------
    //      GET VALUE - REGISTER
    // ---------------------------------------

    // register mem for values rkey
    let mut values_rkey_buf = [0u8; 4];
    let values_rkey_mr = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();
    // register mem for value
    let val_mr = reg_read(id, val_buf.as_ptr() as u64, val_buf.len()).unwrap();
    
    // ---------------------------------------
    //      GET VALUE - RUN GETS
    // ---------------------------------------

    // connect using socket in id
    connect(id).unwrap();

    // wait for host to send remote addr - post it to recv queue
    post_recv_and_wait(id, &mut values_rkey_buf, values_rkey_mr).unwrap();
    
    println!("values rkey: 0x{:x}", u32::from_le_bytes(values_rkey_buf));

    let conn: Connection = Connection { 
        conn_id: id,
        index_base: 0,
        index_read_key: 0,
        addr_mr: null_mut(),
        val_read_key: u32::from_le_bytes(values_rkey_buf),
        val_mr: val_mr
    };

    Ok(conn)
}

fn setup_soc_conn(addr: &str, port: &str, addr_buf: &mut [u8; 8], val_buf: &mut [u8; 64]) -> Result<Connection, Error> {
    // initalize queues
    // TODO: do we like these values long-term
    let mut id: *mut rdma_cm_id = null_mut();
    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 2;
    init.cap.max_recv_wr = 2;
    init.cap.max_send_sge = 2;
    init.cap.max_recv_sge = 2;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;
    init.qp_context = id.cast();

    // create a socket in id
    get_new_cm_id(addr, port, &mut id, &mut init, false).unwrap();

    // ---------------------------------------
    //      GET VALUE - REGISTER
    // ---------------------------------------

    // register mem region to hold addr of value
    let mut index_base_buf = [0u8; 8];
    let index_base_mr = reg_read(id, index_base_buf.as_ptr() as u64, index_base_buf.len()).unwrap();
    // register mem for index rkey
    let mut index_rkey_buf = [0u8; 4];
    let index_rkey_mr = reg_read(id, index_rkey_buf.as_ptr() as u64, index_rkey_buf.len()).unwrap();
    // register mem for values rkey
    let mut values_rkey_buf = [0u8; 4];
    let values_rkey_mr = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();
    // register mem for value's addr
    let addr_mr = reg_read(id, addr_buf.as_ptr() as u64, addr_buf.len()).unwrap();
    // register mem for value
    let val_mr = reg_read(id, val_buf.as_ptr() as u64, val_buf.len()).unwrap();
    
    // ---------------------------------------
    //      GET VALUE - RUN GETS
    // ---------------------------------------

    // connect using socket in id
    connect(id).unwrap();

    // wait for host to send remote addr - post it to recv queue
    post_recv_and_wait(id, &mut index_base_buf, index_base_mr).unwrap();
    post_recv_and_wait(id, &mut index_rkey_buf, index_rkey_mr).unwrap();
    post_recv_and_wait(id, &mut values_rkey_buf, values_rkey_mr).unwrap();
    
    println!("index base: 0x{:x}", u64::from_le_bytes(index_base_buf));
    println!("index rkey: 0x{:x}", u32::from_le_bytes(index_rkey_buf));
    println!("values rkey: 0x{:x}", u32::from_le_bytes(values_rkey_buf));

    let conn: Connection = Connection { 
        conn_id: id,
        index_base: u64::from_le_bytes(index_base_buf),
        index_read_key: u32::from_le_bytes(index_rkey_buf),
        addr_mr,
        val_read_key: u32::from_le_bytes(values_rkey_buf),
        val_mr: val_mr
    };

    Ok(conn)
}


pub fn run_client(soc_addr: &str, soc_port: &str, host_addr: &str, host_port: &str) -> Result<(), Error> {

    let mut val_buf = [0u8; 64];
    let mut addr_buf = [0u8; 8];

    let soc_conn = setup_soc_conn(soc_addr, soc_port, &mut addr_buf, &mut val_buf).unwrap();
    let host_conn = setup_host_conn(host_addr, host_port, &mut val_buf).unwrap();
    
    // ---------------------------------------
    //      GET VALUE - RUN GETS
    // ---------------------------------------


    for x in 0..8 {
        // get pointer to value from index
        post_read_and_wait(soc_conn.conn_id, &mut addr_buf, soc_conn.addr_mr, soc_conn.index_base + (8 * x), 
        soc_conn.index_read_key,).unwrap();

        //sanity check
        println!("pointer to val: 0x{:x}", u64::from_le_bytes(addr_buf));

        let kv_addr = deserialize_kv_addr(u64::from_le_bytes(addr_buf));
        let conn_to_use = if kv_addr.is_cached {
            &soc_conn
        } else {
            &host_conn
        };

        
        post_read_and_wait(conn_to_use.conn_id, &mut val_buf, conn_to_use.val_mr, kv_addr.addr,
            conn_to_use.val_read_key,).unwrap();
        
        let s = match std::str::from_utf8(&val_buf) {
            Ok(v) => v,
            Err(e) => panic!("invalid string: {}", e),
        };
        
        //also sanity check
        println!("value received: {}", s);
    }
    
    Ok(())
}
