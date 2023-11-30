use clap::Parser;
use rdma_sys::*;
use std::ptr::null_mut;
use libc;
use libc::c_void;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    host: bool,
    #[arg(long)]
    cache: bool,
    #[arg(long)]
    client:bool,
    cache_addr: String,
    host_addr: String, 
    #[arg(default_value = "8080")]
    port: String, // just have cache and host use the same port for now
}

struct KVS {
    cached_index_base: u64,
    cached_index_rkey: u32,
    cached_values_rkey: u32,
    host_index_base: u64,
    host_index_rkey: u32,
    host_values_rkey: u32,
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
    Listen,
    GetRequest,
    PostSend,
    PostRecv,
    PostRead,
    WaitForSend,
    WaitForRecv,
    GetAddrInfo,
    CreateEp,
}

const N_KEYS: usize = 256;
const KEY_SIZE: usize = 8;
const INDEX_SIZE: usize = N_KEYS * KEY_SIZE;

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
fn put_addr_in_for_all_keys(kvs: &KVS, addr_to_put: u64, cache: bool) {

    let idx_base = if cache { kvs.cached_index_base } else { kvs.host_index_base };
    
    for key_val in 0..N_KEYS as u64 {

        let offset = key_val * 8;
        let ass_addr = (idx_base + offset) as *mut u64;
        
        let to_put = serialize_kv_addr(KVAddr{
            addr: addr_to_put,
            is_cached: cache,
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

fn listen(id: *mut rdma_cm_id) -> Result<(), Error> {
    let mut ret = unsafe { rdma_listen(id, 0) };
    if ret != 0 {
        unsafe { rdma_destroy_ep(id); }
        return Err(Error::Listen);
    }
    Ok(())
}

fn get_request(listen_id: *mut rdma_cm_id, id: *mut *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_get_request(listen_id, id) };
    if ret != 0 {
        unsafe { rdma_destroy_ep(listen_id); }
        return Err(Error::GetRequest);
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

fn post_read_and_wait(
    id: *mut rdma_cm_id,
    buf: &mut [u8],
    mr: *mut ibv_mr,
    raddr: u64,
    rkey: u32
) -> Result<(), Error> {
    post_read(id, buf, mr, raddr, rkey)?;
    wait_for_send(id)?;
    Ok(())
}

fn post_read(
    id: *mut rdma_cm_id,
    buf: &mut [u8],
    mr: *mut ibv_mr,
    raddr: u64,
    rkey: u32
) -> Result<(), Error> {
    let ret = unsafe {
	rdma_post_read(
	    id,
	    null_mut(),
	    buf.as_mut_ptr().cast(),
	    buf.len(),
	    mr,
	    0,
	    raddr,
	    rkey
	)
    };
    if ret != 0 {
	return Err(Error::PostRead);
    }
    Ok(())
}

/// initialize the key value store by memmaping a region of memory that can hold 256 addresses, returning the base pointer of the mapped region
fn init_kv_store(cache: bool) -> KVS {

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
    
    // client and host will have their own kvs structs
    // they will coordinate at runtime and fill out the rest of the entries
    KVS {
	cached_index_base: if cache { res as u64 }  else {  0 },
	cached_index_rkey: 0,
	cached_values_rkey: 0,
	host_index_base: if !cache { res as u64 } else { 0 },
	host_index_rkey: 0,
	host_values_rkey: 0,
    }
} 


/// processes a single request, whose communication id is in id
fn set_up_client_conn(
    id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: &mut KVS,
) -> i32 {

    let mut ret;

    // a dummy test, this will later place the index (and the initial values?)
    let test_str = "Hello from server!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].clone_from_slice(test_str);

    let mut cached_index_base_buf: [u8; 8] = [0u8; 8];
    
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

    // Client needs index base address, index remote key, cache values rkey, host values rkey
    // register mem containing pointer to index base
    let cached_index_mem = reg_read(id, kvs.cached_index_base, 256*8).unwrap();
    let mut cached_index_rkey_buf = kvs.cached_index_rkey.to_le_bytes();
    let cached_index_rkey_mem = reg_read(id, cached_index_rkey_buf.as_ptr() as u64, cached_index_rkey_buf.len()).unwrap();
    let mut cached_values_rkey_buf = kvs.cached_values_rkey.to_le_bytes();
    let cached_values_rkey_mem = reg_read(id, cached_values_rkey_buf.as_ptr() as u64, cached_values_rkey_buf.len()).unwrap();
    let mut host_values_rkey_buf = kvs.host_values_rkey.to_le_bytes();
    let host_values_rkey_mem = reg_read(id, host_values_rkey_buf.as_ptr() as u64, host_values_rkey_buf.len()).unwrap();
    
    // ---------------------------------------
    //      HANDLE CONN -- COMMUNICATE
    // ---------------------------------------

    // officially accept client connection
    accept(id).unwrap();

    // send the index address (this posts it to the send queue)
    post_send_and_wait(id, &mut cached_index_base_buf, cached_index_mem, send_flags).unwrap();
    // send the index rkey
    post_send_and_wait(id, &mut cached_index_rkey_buf, cached_index_rkey_mem, send_flags).unwrap();
    // send the cached values remote key
    post_send_and_wait(id, &mut cached_values_rkey_buf, cached_values_rkey_mem, send_flags).unwrap();
    // send the host values remote key
    post_send_and_wait(id, &mut host_values_rkey_buf, host_values_rkey_mem, send_flags).unwrap();

    // send the value region remote key
    
    ret

}


/// runs a central server listener, that listens on the listen_id socket and processes requests as they come in
fn run_server_listen(
    listen_id: *mut rdma_cm_id,
    init: &mut ibv_qp_init_attr,
    kvs: &mut KVS,
) -> i32 {

    // listen for incoming conns
    // TODO would this be where the loop starts?
    let mut ret = unsafe { rdma_listen(listen_id, 0) };
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

        ret = set_up_client_conn(id, init, kvs);
        if ret != 0 {
            println!("error processing request");
            return ret;
        }
    }
}

fn run_cache(host_addr: &str, cache_addr: &str, port: &str) -> Result<(), Error> {
    // create the KVS / index
    // the index will only live on the cache
    // - how is the index initially populated?
    // - should the host construct the index, then the cache asks for it and copies it into memory?
    // - this might be easier and could be done once at startup at least in our prototype

    let test_str = "Hello from cache!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].copy_from_slice(test_str);
    let val_addr = send_msg.as_ptr() as u64;

    // inits a cache-local view of the kv store
    let mut kvs = init_kv_store(true);

    let mut host_index_base_buf = [0u8; 8];
    let mut host_index_rkey_buf = [0u8; 4];
    let mut host_values_rkey_buf = [0u8; 4];

    // set up connection to host
    let mut host_id: *mut rdma_cm_id = null_mut();
    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 3;
    init.cap.max_recv_wr = 3;
    init.cap.max_send_sge = 3;
    init.cap.max_recv_sge = 3;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;
    
    get_new_cm_id(host_addr, port, &mut host_id, &mut init)?;


    Ok(())
}

fn run_host(host_addr: &str, cache_addr: &str, port: &str) -> Result<(), Error> {
    // create the KVS / index
    // - for this toy example, the host will populate all entries of the index with
    let test_str = "Hello from host!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].copy_from_slice(test_str);
    let val_addr = send_msg.as_ptr() as u64;
    
    let mut kvs = init_kv_store(false);
    put_addr_in_for_all_keys(&kvs, val_addr, false);

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
    get_new_cm_id(host_addr, port, &mut listen_id, &mut init).unwrap();

    // register the index
    let index_mem = reg_read(listen_id, kvs.host_index_base, INDEX_SIZE).unwrap();
    // register the value(s)
    let values_mem = reg_read(listen_id, val_addr, send_msg.len()).unwrap();

    kvs.host_index_rkey = unsafe { (*index_mem).rkey };
    kvs.host_values_rkey = unsafe { (*values_mem).rkey };

    setup_cache(&mut kvs, listen_id, cache_addr)?;

    return run_host_listen();    
}

fn setup_cache(kvs: &mut KVS, listen_id: *mut rdma_cm_id, cache_addr: &str) -> Result<(), Error> {
    //wait for cache to connect
    //the order is: host runs, cache runs and connects to host, client runs and connects to cache
    //this is fine because the cache won't listen for connections until it connects to the host

    //I imagine the fastest way to do this is to have the client set up a connection to
    //both the cache and the host at startup time, then the cache and host maintain those
    //connections while the client does some READs

    //when the host/cache start, they will need to provide both their own address and the other's
    //address so they can communicate to set things up.

    // here is the flow of the whole thing:
    // host starts, creates its own index and populates it with values, listen for cache connection
    // cache starts, connects to host
    // cache reads index from host, filling keys with some of its own values, and setting the cached bit
    // client starts, connects to cache (needs host and client addresses)
    // client connects to host
    // client gets the base address of the cached index
    // client reads some keys from the cached index and uses the bit to determine whether
    //   the key is on the host or the cache

    listen(listen_id)?;

    let mut cache_id: *mut rdma_cm_id = null_mut();
    get_request(listen_id, &mut cache_id)?;

    // register index for reads
    // post send for host index base
    // post send for host values rkey
    // post send for host index rkey

    let host_index_mem = reg_read(cache_id, kvs.host_index_base, INDEX_SIZE)?;

    let mut host_index_base_buf = kvs.host_index_base.to_le_bytes();
    let host_index_base_mem = reg_read(cache_id, host_index_base_buf.as_ptr() as u64, host_index_base_buf.len()).unwrap();
    
    let mut host_index_rkey_buf = kvs.host_index_rkey.to_le_bytes();
    let host_index_rkey_mem = reg_read(cache_id, host_index_rkey_buf.as_ptr() as u64, host_index_rkey_buf.len()).unwrap();
    
    let mut host_values_rkey_buf = kvs.host_index_rkey.to_le_bytes();
    let host_values_rkey_mem = reg_read(cache_id, host_index_rkey_buf.as_ptr() as u64, host_index_rkey_buf.len()).unwrap();

    // accept connection from cache
    accept(listen_id)?;

    // post sends for all three
    post_send_and_wait(cache_id, &mut host_index_base_buf, host_index_base_mem, 0)?;
    post_send_and_wait(cache_id, &mut host_index_rkey_buf, host_index_rkey_mem, 0)?;
    post_send_and_wait(cache_id, &mut host_values_rkey_buf, host_values_rkey_mem, 0)?;
    
    Ok(())
}

fn run_host_listen() -> Result<(), Error> {
    Ok(())
}

fn get_new_cm_id(addr: &str, port: &str, id: *mut *mut rdma_cm_id, init: &mut ibv_qp_init_attr) -> Result<(), Error> {
    // create addr info
    let mut addr_info: *mut rdma_addrinfo = null_mut();
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    hints.ai_port_space = rdma_port_space::RDMA_PS_TCP as i32;
    let mut ret =
    unsafe { rdma_getaddrinfo(addr.as_ptr().cast(), port.as_ptr().cast(), &hints, &mut addr_info) };
    if ret != 0 {
        return Err(Error::GetAddrInfo);
    }
    ret = unsafe { rdma_create_ep(id, addr_info, null_mut(), init) };
    if ret != 0 {
        unsafe { rdma_freeaddrinfo(addr_info); }
        return Err(Error::CreateEp);
    }
    
    Ok(())
}


/// sets up the server rdma connection, then listens for incoming connections and processes them
fn run_server(kvs: &mut KVS, addr: &str, port: &str) -> i32 {

    let test_str = "Hello from server!".as_bytes();
    let mut send_msg: [u8; 64] = [0u8; 64];
    send_msg[0..test_str.len()].copy_from_slice(test_str);
    let val_addr = send_msg.as_ptr() as u64;
    
    put_addr_in_for_all_keys(&kvs, val_addr, false);

    println!("val addr: 0x{:x}", val_addr);
    
    

    let mut init = unsafe { std::mem::zeroed::<ibv_qp_init_attr>() };
    init.cap.max_send_wr = 2;
    init.cap.max_recv_wr = 2;
    init.cap.max_send_sge = 2;
    init.cap.max_recv_sge = 2;
    init.cap.max_inline_data = 64;
    init.sq_sig_all = 1;

    // create a socket in listen_id
    let mut listen_id: *mut rdma_cm_id = null_mut();
    get_new_cm_id(addr, port, &mut listen_id, &mut init).unwrap();

    // register the index
    // let index_mem = reg_read(listen_id, kvs.index_base, 256 * 8).unwrap();
    // // register the values (which for now will just be the test string)
    // let values_mem = reg_read(listen_id, val_addr, 64).unwrap();

    // kvs.index_rkey = unsafe { (*index_mem).rkey };
    // kvs.values_rkey = unsafe { (*values_mem).rkey };

    // println!("sending index base: 0x{:x}", kvs.index_base);
    // println!("sending index rkey: 0x{:x}", kvs.index_rkey);
    // println!("sending values rkey: 0x{:x}", kvs.values_rkey);
    
    // run server
    return run_server_listen(listen_id, &mut init, kvs);

}


fn run_client(addr: &str, port: &str) -> i32 {

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
    let mut index_rkey_buf = [0u8; 4];
    let index_rkey_mr = reg_read(id, index_rkey_buf.as_ptr() as u64, index_rkey_buf.len()).unwrap();
    // register mr for values rkey
    let mut values_rkey_buf = [0u8; 4];
    let values_rkey_mr = reg_read(id, values_rkey_buf.as_ptr() as u64, values_rkey_buf.len()).unwrap();
    // register mr for value ptr
    let mut val_ptr_buf = [0u8; 8];
    let val_ptr_mr = reg_read(id, val_ptr_buf.as_ptr() as u64, val_ptr_buf.len()).unwrap();
    // register mr for value
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
	post_read_and_wait(
	    id,
	    &mut val_ptr_buf,
	    val_ptr_mr,
	    u64::from_le_bytes(index_base_buf) + (8 * x),
	    u32::from_le_bytes(index_rkey_buf),
	).unwrap();

	//sanity check
	println!("pointer to val: 0x{:x}", u64::from_le_bytes(val_ptr_buf));
	
	post_read_and_wait(
	    id,
	    &mut val_buf,
	    val_mr,
	    u64::from_le_bytes(val_ptr_buf),
	    u32::from_le_bytes(values_rkey_buf),
	).unwrap();
	
	let s = match std::str::from_utf8(&val_buf) {
	    Ok(v) => v,
	    Err(e) => panic!("invalid string: {}", e),
	};
	
	//also sanity check
	println!("value received: {}", s);
    }
    
    ret
}



fn main() {
    // initialize args
    let mut args = Args::parse();

    args.port.push_str("\0");
    let port = args.port.as_str();

    args.host_addr.push_str("\0");
    let host_addr = args.host_addr.as_str();

    args.cache_addr.push_str("\0");
    let cache_addr = args.cache_addr.as_str();
    
    if args.host {
	println!("Starting host KVS");
    } else if args.cache {
	println!("Starting cache KVS");
	// client is straightforward
    } else if args.client {
	println!("Starting client"); 
    }
}
