use std::ptr::null_mut;
use rdma_sys::*;
use crate::rdma_utils::*;

pub fn run_client(addr: &str, port: &str) -> Result<(), Error> {

    // ---------------------------------------
    //      SETUP
    // ---------------------------------------

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
    // register mem for value ptr
    let mut val_ptr_buf = [0u8; 8];
    let val_ptr_mr = reg_read(id, val_ptr_buf.as_ptr() as u64, val_ptr_buf.len()).unwrap();
    // register mem for value
    let mut val_buf = [0u8; 64];
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


    for x in 0..8 {
        // get pointer to value from index
        post_read_and_wait(id, &mut val_ptr_buf, val_ptr_mr, u64::from_le_bytes(index_base_buf) + (8 * x), 
        u32::from_le_bytes(index_rkey_buf),).unwrap();

        //sanity check
        println!("pointer to val: 0x{:x}", u64::from_le_bytes(val_ptr_buf));
        
        post_read_and_wait(id, &mut val_buf, val_mr, u64::from_le_bytes(val_ptr_buf),
            u32::from_le_bytes(values_rkey_buf),).unwrap();
        
        let s = match std::str::from_utf8(&val_buf) {
            Ok(v) => v,
            Err(e) => panic!("invalid string: {}", e),
        };
        
        //also sanity check
        println!("value received: {}", s);
    }
    
    Ok(())
}
