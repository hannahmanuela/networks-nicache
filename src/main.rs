use clap::Parser;
use rdma_sys::*;
use std::ptr::null_mut;
use libc;
use libc::c_void;

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

struct KVS {
    index: u64
}

struct KVAddr {
    addr: u64,
    is_cached: bool,
    num_accesses: u8
}

#[derive(Debug)]
enum Error {
    RegisterMem,
    Accept,
    Connect,
    PostSend,
    PostRecv,
    WaitForSend,
    WaitForRecv,
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
fn put_addr_in_for_all_keys(kv: &KVS, addr_to_put: u64) {

    for key_val in 0..256 {

        let offset = key_val * 8;
        let ass_addr = (kv.index + offset) as *mut u64;
        
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

fn reg_read(id: *mut rdma_cm_id, base: u64, len: usize) -> Result<*mut ibv_mr, Error> {
    let reg_mem = unsafe { rdma_reg_read(id, base as *mut c_void, len) };
    if reg_mem.is_null() {
        unsafe { rdma_dereg_mr(reg_mem); }
        return Err(Error::RegisterMem);
    }
    Ok(reg_mem)
}

fn accept(id: *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_accept(id, null_mut()) };
    if ret != 0 {
	return Err(Error::Accept);
    }
    Ok(())
}

fn connect(id: *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_connect(id, null_mut()) };
    if ret != 0 {
        unsafe { rdma_disconnect(id) };
	return Err(Error::Connect);
    }
    Ok(())
}

fn post_send_and_wait(id: *mut rdma_cm_id, src_buf: &mut[u8], mr: *mut ibv_mr, flags: u32) -> Result<(), Error> {
    post_send(id, src_buf, mr, flags)?;
    wait_for_send(id)?;
    Ok(())
}

fn post_send(id: *mut rdma_cm_id, src_buf: &mut[u8], mr: *mut ibv_mr, flags: u32) -> Result<(), Error> {
    let ret = unsafe {
	rdma_post_send(
            id,
            null_mut(),
            src_buf.as_mut_ptr().cast(),
            src_buf.len(),
            mr,
            flags.try_into().unwrap(),
        )
    };

    if ret != 0 {
	return Err(Error::PostSend);
    }

    Ok(())
}

fn wait_for_send(id: *mut rdma_cm_id) -> Result<(), Error> {
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    let mut ret = 0;
    while ret == 0 {
        ret = unsafe { rdma_get_send_comp(id, &mut wc) };
    }
    if ret < 0 {
	return Err(Error::WaitForSend);
    }

    Ok(())
}

fn post_recv_and_wait(id: *mut rdma_cm_id, buf: &mut [u8], mr: *mut ibv_mr) -> Result<(), Error> {
    post_recv(id, buf, mr)?;
    wait_for_recv(id)?;
    Ok(())
}

fn post_recv(id: *mut rdma_cm_id, buf: &mut [u8], mr: *mut ibv_mr) -> Result<(), Error> {
    let ret = unsafe { rdma_post_recv(id, null_mut(), buf.as_mut_ptr().cast(), buf.len(), mr) };
    if ret != 0 {
        return Err(Error::PostRecv);
    }
    Ok(())
}

fn wait_for_recv(id: *mut rdma_cm_id) -> Result<(), Error> {
    let mut wc = unsafe { std::mem::zeroed::<ibv_wc>() };
    let mut ret = 0;
    while ret == 0 {
        ret = unsafe { rdma_get_recv_comp(id, &mut wc) };
    }
    if ret < 0 {
	return Err(Error::WaitForRecv);
    }
    Ok(())
}

/// initialize the key value store by memmaping a region of memory that can hold 256 addresses, returning the base pointer of the mapped region
fn init_kv_store() -> KVS {

    // TODO is this correct?
    let key_size: usize = 256 * 8;
    let res = unsafe {
	libc::mmap(
	    null_mut(),
	    key_size, 
	    libc::PROT_READ | libc::PROT_WRITE,
	    libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
	    0,
	    0,
	)
    };

    if res == libc::MAP_FAILED {
	panic!("mapping KVS memory failed");
    }


    KVS { index: res as u64 }
} 


/// processes a single request, whose communication id is in id
fn set_up_client_conn(id: *mut rdma_cm_id, init: &mut ibv_qp_init_attr, index_base_addr: u64) -> i32 {

    let mut ret;

    // a dummy test, this will later place the index (and the initial values?)
    let test_str = "Hello from server!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].clone_from_slice(test_str);

    let mut index_base_buf: [u8; 8] = index_base_addr.to_le_bytes();
    
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

    // Client needs index base address, index remote key, values remote key
    
    // register mem containing pointer to index base

    // register the index
    let index_mem = reg_read(id, index_base_addr, 256 * 8).unwrap();
    // get the remote key from the index memory region
    let index_rkey = unsafe { (*index_mem).rkey };
    // register the index base address
    let _index_base_mem = reg_read(id, index_base_buf.as_ptr() as u64, index_base_buf.len()).unwrap();
    // register the index remote key
    let mut index_rkey_buf = index_rkey.to_le_bytes();
    let index_rkey_mem = reg_read(id, index_rkey_buf.as_ptr() as u64, index_rkey_buf.len()).unwrap();

    // register the values (which for now will just be the test string)
    let values_mem = reg_read(id, send_msg.as_ptr() as u64, test_str.len()).unwrap();
    // get values remote key
    let values_rkey = unsafe { (*values_mem).rkey };
    // register values remote key
    let mut values_rkey_buf = values_rkey.to_le_bytes();
    let values_rkey_mem = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();


    println!("sending index base: 0x{:x}", index_base_addr);
    println!("sending index rkey: 0x{:x}", index_rkey);
    println!("sending values rkey: 0x{:x}", values_rkey);
    
    // ---------------------------------------
    //      HANDLE CONN -- COMMUNICATE
    // ---------------------------------------

    // officially accept client connection
    accept(id).unwrap();

    // send the index address (this posts it to the send queue)
    post_send_and_wait(id, &mut index_base_buf, index_mem, send_flags).unwrap();
    // send the index rkey
    post_send_and_wait(id, &mut index_rkey_buf, index_rkey_mem, send_flags).unwrap();
    // send the values remote key
    post_send_and_wait(id, &mut values_rkey_buf, values_rkey_mem, send_flags).unwrap();

    // send the value region remote key
    
    ret

}


/// runs a central server listener, that listens on the listen_id socket and processes requests as they come in
fn run_server_listen(listen_id: *mut rdma_cm_id, init: &mut ibv_qp_init_attr, index_base_addr: u64) -> i32 {

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

        ret = set_up_client_conn(id, init, index_base_addr);
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
    return run_server_listen(listen_id, &mut init, index_base_addr);

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
    let mut index_base_buf = [0u8; 8];
    let index_base_mr = reg_read(id, index_base_buf.as_ptr() as u64, index_base_buf.len()).unwrap();
    // register mr for index rkey
    let mut index_rkey_buf = [0u8; 8];
    let index_rkey_mr = reg_read(id, index_rkey_buf.as_ptr() as u64, index_rkey_buf.len()).unwrap();
    // register mr for values rkey
    let mut values_rkey_buf = [0u8; 8];
    let values_rkey_mr = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();
    
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
    println!("index rkey: 0x{:x}", u64::from_le_bytes(index_rkey_buf));
    println!("values rkey: 0x{:x}", u64::from_le_bytes(values_rkey_buf));
    0
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
    
    let kv = init_kv_store();
    put_addr_in_for_all_keys(&kv, val_addr);
    
    let ret = if args.server {
        run_server(addr, port, kv.index)
    } else {
        run_client(addr, port, kv.index)
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
