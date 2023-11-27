use clap::Parser;
use rdma_sys::*;
use std::ptr::null_mut;
use memmap::MmapMut;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    server: bool,
    #[arg(short, long)]
    addr: String,
    #[arg(short, long)]
    port: String,
}

struct KVAddr {
    addr: u64,
    is_cached: bool,
    num_accesses: u8
}

fn serialize_kv_addr(struct_kv: KVAddr) -> u64 {

    let mut to_ret: u64;

    to_ret = struct_kv.addr;
    if !struct_kv.is_cached {
        to_ret = to_ret + 1;
    }
    let accesses: u64 = struct_kv.num_accesses as u64;
    to_ret = to_ret + (accesses << 50);

    return to_ret;
}

fn deserialize_kv_addr(addr_val: u64) -> KVAddr {

    let cached_bit = addr_val & 1;
    let num_accesses: u64 = addr_val >> 50;
    let trunc_num_accesses: u8 = num_accesses as u8;

    let addr_no_access = (addr_val << 14) >> 14;
    let addr_no_cached_bit = (addr_no_access >> 1) << 1;

    return KVAddr { addr: addr_no_cached_bit, is_cached: cached_bit == 1, num_accesses: trunc_num_accesses };
}


/// places a serialized kvaddr for all 8-bit keys, starting at base_pointer, that point to addr_to_put
fn put_addr_in_for_all_keys(base_pointer: u64, addr_to_put: u64) {

    for key_val in 0..256 {

        let offset = key_val * 8;
        let ass_addr = (base_pointer + offset) as *mut u64;
        
        let to_put = serialize_kv_addr(KVAddr{
            addr: addr_to_put,
            is_cached: true,
            num_accesses: 0,});
        
        unsafe { *ass_addr = to_put };
    }
}


fn addr_given_key(key: u8, base_pointer: u64) -> u64 {

    let offset = (key * 8) as u64;
    let ass_addr = base_pointer + offset;

    return ass_addr;

}



/// initialize the key value store by memmaping a region of memory that can hold 256 addresses, returning the base pointer of the mapped region
fn init_kv_store() -> u64 {

    // TODO is this correct?
    let key_size: usize = 256 * 8;
    let res = MmapMut::map_anon(key_size);
    match res {
        Ok(res_val) => {
            return res_val.as_ptr().cast::<u8>() as u64;
        }
        Err(_) => return 0,
    }
    
} 


/// processes a single request, whose communication id is in id
fn set_up_client_conn(id: *mut rdma_cm_id, init: &mut ibv_qp_init_attr) -> i32 {

    let mut ret ;

    // a dummy test, this will later place the index (and the initial values?)
    let test_str = "Hello from server!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].clone_from_slice(test_str);

    // ---------------------------------------
    //      HANDLE CONN -- SETUP
    // ---------------------------------------

    // "gets the attributes specified in attr_mask for the
    //    new conns qp and returns them through the pointers qp_attr (which we never use again) and init"
    let mut qp_attr = unsafe { std::mem::zeroed::<ibv_qp_attr>() };
    ret = unsafe {ibv_query_qp( (*id).qp, &mut qp_attr, ibv_qp_attr_mask::IBV_QP_CAP.0.try_into().unwrap(),init,)};
    if ret != 0 {
        println!("ibv_query_qp");
        unsafe { rdma_destroy_ep(id); }
        return ret;
    }

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

    // register mem containing pointer to send_msg
    let reg_mem = unsafe { rdma_reg_read(id, send_msg.as_mut_ptr().cast(), send_msg.len()) };
    if reg_mem.is_null() {
        ret = -1;
        println!("rdma_reg_read for read_msg");
        unsafe { rdma_dereg_mr(reg_mem); }
        return ret;
    }

    // register mem containing addr of send_msg
    let addr = send_msg.as_mut_ptr().cast::<u8>() as u64;
    let mut addr_buf = addr.to_le_bytes();

    println!("write region address: 0x{:x}", addr);

    let mut addr_send_mr = null_mut();
    if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) == 0 {
        addr_send_mr = unsafe { rdma_reg_read(id, addr_buf.as_mut_ptr().cast(), 8) };
        if addr_send_mr.is_null() {
            ret = -1;
            println!("error registering region for remote read region");
            unsafe {
                rdma_dereg_mr(addr_send_mr);
            }
            return ret;
        }
    }

    // register mem containing addr of key
    let rkey: u32 = unsafe { (*reg_mem).rkey };
    let mut rkey_buf = rkey.to_le_bytes();

    println!("rkey: 0x{:x}", rkey);

    let mut rkey_send_mr = null_mut();
    if (send_flags & ibv_send_flags::IBV_SEND_INLINE.0) == 0 {
        rkey_send_mr = unsafe { rdma_reg_msgs(id, rkey_buf.as_mut_ptr().cast(), 4) };
        if rkey_send_mr.is_null() {
            ret = -1;
            println!("error registering region for remote read region");
            unsafe {
                rdma_dereg_mr(rkey_send_mr);
            }
            return ret;
        }
    }

    // ---------------------------------------
    //      HANDLE CONN -- COMMUNICATE
    // ---------------------------------------

    // officially accept client connection
    ret = unsafe { rdma_accept(id, null_mut()) };
    if ret != 0 {
        println!("rdma_accept");
        return ret;
    }

    // send the write buffer address (this posts it to the send queue)
    ret = unsafe {
        rdma_post_send(
            id,
            null_mut(),
            addr_buf.as_mut_ptr().cast(),
            8,
            addr_send_mr,
            send_flags.try_into().unwrap(),
        )
    };
    if ret != 0 {
        println!("rdma_post_send");
        unsafe { rdma_disconnect(id); }
        return ret;
    }

    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    // retrieve completed work req
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
    }

    // send the key
    ret = unsafe {
        rdma_post_send(
            id,
            null_mut(),
            rkey_buf.as_mut_ptr().cast(),
            4,
            rkey_send_mr,
            send_flags.try_into().unwrap(),
        )
    };
    if ret != 0 {
        println!("rdma_post_send");
        unsafe { rdma_disconnect(id); }
        return ret;
    }

    // retrieve completed work req
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_send_comp");
    } else {
        ret = 0;
    }

    ret

}


/// runs a central server listener, that listens on the listen_id socket and processes requests as they come in
fn run_server_listen(listen_id: *mut rdma_cm_id, init: &mut ibv_qp_init_attr) -> i32 {

    let mut ret ;

    // listen for incoming conns
    // TODO would this be where the loop starts?
    ret = unsafe { rdma_listen(listen_id, 0) };
    if ret != 0 {
        println!("rdma_listen");
        unsafe { rdma_destroy_ep(listen_id); }
        return ret;
    }

    loop {
        // put received conn in id
        let mut id: *mut rdma_cm_id = null_mut();
        ret = unsafe { rdma_get_request(listen_id, &mut id) };
        if ret != 0 {
            println!("rdma_get_request");
            unsafe { rdma_destroy_ep(listen_id); }
            return ret;
        }

        ret = set_up_client_conn(id, init);
        if ret != 0 {
            println!("error processing request");
            return ret;
        }
    }
}


/// sets up the server rdma connection, then listens for incoming connections and processes them
fn run_server(addr: &str, port: &str, index_base_addr: u64) -> i32 {
    
    // create addr info
    let mut addr_info: *mut rdma_addrinfo = null_mut();
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
    let mut ret =
    unsafe { rdma_getaddrinfo(addr.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut addr_info) };
    if ret != 0 {
        println!("rdma_getaddrinfo");
        return ret;
    }

    // initalize queues
    // TODO: do we like these values long-term
    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 2;
    init.cap.max_recv_wr = 2;
    init.cap.max_send_sge = 2;
    init.cap.max_recv_sge = 2;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;

    // create a socket in listen_id
    let mut listen_id = null_mut();
    ret = unsafe { rdma_create_ep(&mut listen_id, addr_info, null_mut(), &mut init) };
    if ret != 0 {
        println!("rdma_create_ep");
        unsafe { rdma_freeaddrinfo(addr_info); }
        return ret;
    }

    // run server
    return run_server_listen(listen_id, &mut init);

}


fn run_client(addr: &str, port: &str, index_base_addr: u64) -> i32 {

    // ---------------------------------------
    //      SETUP
    // ---------------------------------------
    
    // create addr info
    let mut addr_info: *mut rdma_addrinfo = null_mut();
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
    let mut ret =
        unsafe { rdma_getaddrinfo(addr.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut addr_info) };
    if ret != 0 {
        println!("rdma_getaddrinfo");
        return ret;
    }

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
    ret = unsafe { rdma_create_ep(&mut id, addr_info, null_mut(), &mut init) };
    if ret != 0 {
        println!("rdma_create_ep");
        unsafe {
            rdma_freeaddrinfo(addr_info);
        }
        return ret;
    }

    // ---------------------------------------
    //      GET VALUE - REGISTER
    // ---------------------------------------

    // register memory region to hold addr of value
    let mut remote_addr = [0u8; 8];
    let addr_mr = unsafe { rdma_reg_msgs(id, remote_addr.as_mut_ptr().cast(), 8) };
    if addr_mr.is_null() {
        println!("rdma_reg_write for remote addr");
        unsafe {
            rdma_destroy_ep(id);
        }
        return -1;
    }

    // register memory region to hold key
    let mut rkey = [0u8; 4];
    let rkey_mr = unsafe { rdma_reg_msgs(id, rkey.as_mut_ptr().cast(), 4) };
    if rkey_mr.is_null() {
        println!("rdma_reg_write for remote key");
        unsafe {
            rdma_destroy_ep(id);
        }
        return -1;
    }

    // register memory region to hold value
    // TODO when it's not just a test message we won't actually know how big the value is, there will have to be a max size
    let mut recv_msg = [0u8; 64];
    let recv_mr = unsafe { rdma_reg_read(id, recv_msg.as_mut_ptr().cast(), recv_msg.len()) };
    if recv_mr.is_null() {
        println!("rdma_reg_write for remote key");
        unsafe {
            rdma_destroy_ep(id);
        }
        return -1;
    }

    // ---------------------------------------
    //      GET VALUE - RUN GETS
    // ---------------------------------------

    // connect using socket in id
    ret = unsafe { rdma_connect(id, null_mut()) };
    if ret != 0 {
        println!("rdma_connect");
        unsafe { rdma_disconnect(id) };
    }

    // wait for host to send remote addr - post it to recv queue
    ret =
        unsafe { rdma_post_recv(id, null_mut(), remote_addr.as_mut_ptr().cast(), 8, addr_mr) };
    if ret != 0 {
        println!("error getting remote address");
        unsafe {
            rdma_dereg_mr(addr_mr);
        }
        return -1;
    }
    
    // retrieve completed work req
    ret = 0;
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_recv_comp");
    }

    // wait for host to send rkey - post it to recv queue
    ret = unsafe { rdma_post_recv(id, null_mut(), rkey.as_mut_ptr().cast(), 4, rkey_mr) };
    if ret != 0 {
        println!("error getting remote address");
        unsafe {
            rdma_dereg_mr(rkey_mr);
        }
        return -1;
    }

    // retrieve completed work req
    ret = 0;
    wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_recv_comp");
    }

    // print results
    let remote_addr = u64::from_le_bytes(remote_addr);
    let rkey = u32::from_le_bytes(rkey);

    println!("remote addr: 0x{:x}", remote_addr);
    println!("rkey: 0x{:x}", rkey);

    // post read to id's queue for value, pass retrieved key and remote addr
    ret = unsafe {
        rdma_post_read( id, null_mut(), recv_msg.as_mut_ptr().cast(), 
            recv_msg.len(), recv_mr, 0, remote_addr, rkey,)
    };
    if ret != 0 {
        println!("rdma_post_read");
        unsafe { rdma_disconnect(id); }
        return ret;
    }

    // retrieve completed work req
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
        println!("rdma_get_recv_comp");
    } else {
        ret = 0;
    }

    // check that the received value is as expected, print it
    let s = match std::str::from_utf8(&recv_msg) {
        Ok(v) => v,
        Err(e) => panic!("invalid string: {}", e),
    };

    println!("data read: {:?}", recv_msg);
    println!("string read: {}", s);

    ret


}



fn main() {

    // initialize args
    let mut args = Args::parse();
    args.addr.push_str("\0");
    args.port.push_str("\0");
    let addr = args.addr.as_str();
    let port = args.port.as_str();

    // initalize KV store (for now all keys will point to the same value, which is a string that says hello from server)
    let value = "Hello from server!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..value.len()].clone_from_slice(value);
    let val_addr = send_msg.as_mut_ptr().cast::<u8>() as u64;

    let index_base_addr = init_kv_store();
    put_addr_in_for_all_keys(index_base_addr, val_addr);

    let ret = if args.server {
        run_server(addr, port, index_base_addr)
    } else {
        run_client(addr, port, index_base_addr)
    };

    if ret != 0 {
        println!(
            "rdma_client: ret error {:?}",
            std::io::Error::from_raw_os_error(-ret)
        );
        if ret == -1 {
            println!(
                "rdma_client: last os error {:?}",
                std::io::Error::last_os_error()
            );
        }
    }
}
