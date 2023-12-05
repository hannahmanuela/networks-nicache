use std::{ptr::null_mut, time::Duration};
use std::time::Instant;
use rand::{thread_rng, Rng};
use rdma_sys::*;
use crate::{rdma_utils::*, deserialize_kv_addr};
use crate::kv_store::*;

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

#[inline(always)]
fn do_request(
    soc_conn: &Connection,
    host_conn: &Connection,
    addr_buf: &mut [u8; 8],
    val_buf: &mut [u8; 64],
    offset: u64,
) -> Result<(Instant, Instant, bool), Error> {
    post_read_and_wait(
        soc_conn.conn_id,
        addr_buf,
        soc_conn.addr_mr,
        soc_conn.index_base + (offset * 8),
        soc_conn.index_read_key,
    )?;
    // deserialize the address
    let kv_addr = deserialize_kv_addr(u64::from_le_bytes(*addr_buf));
    let mut on_host = false;
    let conn_to_use = if kv_addr.is_cached {
        &soc_conn
    } else {
        on_host = true;
        &host_conn
    };
    let time_after_get_addr_to_read = Instant::now();
    // read value from appropriate source
    post_read_and_wait(
        conn_to_use.conn_id,
        val_buf,
        conn_to_use.val_mr,
        kv_addr.addr,
        conn_to_use.val_read_key,
    )?;
    let time_after_get_val = Instant::now();
    Ok((time_after_get_addr_to_read, time_after_get_val, on_host))
}

fn run_latency(
    soc_conn: &Connection,
    host_conn: &Connection,
    addr_buf: &mut [u8; 8],
    val_buf: &mut [u8; 64],
) -> Result<(), Error> {
    //generate 10000 random indicies to read from
    let mut rng = thread_rng();
    let mut reqs: Vec<u64> = Vec::new();
    let num_iters = 10000;
    for _ in 0..num_iters {
        reqs.push(rng.gen_range(0..N_KEYS as u64));
    }
    let mut sum_get_addr_time: Duration = Duration::from_secs(0);
    let mut sum_get_val_time_soc: Duration = Duration::from_secs(0);
    let mut sum_get_val_time_host: Duration = Duration::from_secs(0);
    let mut num_host_iters = 0;
    let mut num_soc_iters = 0;
    // do 10k requests and measure latency each time
    for offset in reqs {
        let now = Instant::now();
        // get address from index
        let (time_after_addr, time_after_val, on_host) = do_request(soc_conn, host_conn, addr_buf, val_buf, offset)?;
        let time_to_addr = time_after_addr - now;
        sum_get_addr_time = sum_get_addr_time + time_to_addr;
        let time_to_val = time_after_val - time_after_addr;
        if on_host {
            sum_get_val_time_host = sum_get_val_time_host + time_to_val;
            num_host_iters += 1;
        } else {
            sum_get_val_time_soc = sum_get_val_time_soc + time_to_val;
            num_soc_iters += 1;
        }
    }
    let avg_get_addr_time = sum_get_addr_time /  num_iters;
    let avg_get_val_time_soc = sum_get_val_time_soc / num_soc_iters;
    let avg_get_val_time_host = sum_get_val_time_host / num_host_iters;
    println!("avg_get_addr_time in nanos: {}", avg_get_addr_time.as_nanos());
    println!("avg_get_val_time_soc in nanos: {}", avg_get_val_time_soc.as_nanos());
    println!("avg_get_val_time_host in nanos: {}", avg_get_val_time_host.as_nanos());
    Ok(())
}

fn run_throughput(
    soc_conn: &Connection,
    host_conn: &Connection,
    addr_buf: &mut [u8; 8],
    val_buf: &mut [u8; 64],
) -> Result<(), Error> {
    let now = Instant::now();
    let mut req_count = 0;
    // run for 30 seconds
    while now.elapsed().as_secs() < 30 {
        let offset: u64 = rand::thread_rng().gen_range(0..N_KEYS as u64);
        do_request(soc_conn, host_conn, addr_buf, val_buf, offset)?;
        req_count += 1;
    }

    println!(
        "{} requests in 30 seconds = {}reqs/s",
        req_count,
        req_count / 30
    );
    Ok(())
}

fn run_benchmark(
    soc_conn: &Connection,
    host_conn: &Connection,
    addr_buf: &mut [u8; 8],
    val_buf: &mut [u8; 64],
) -> Result<(), Error> {
    run_latency(soc_conn, host_conn, addr_buf, val_buf)?;
    // run_throughput(soc_conn, host_conn, addr_buf, val_buf)?;

    Ok(())
}

pub fn run_client(soc_addr: &str, soc_port: &str, host_addr: &str, host_port: &str) -> Result<(), Error> {

    let mut val_buf = [0u8; 64];
    let mut addr_buf = [0u8; 8];

    println!("setting up soc conn");
    let soc_conn = setup_soc_conn(soc_addr, soc_port, &mut addr_buf, &mut val_buf).unwrap();
    println!("setting up host conn");
    let host_conn = setup_host_conn(host_addr, host_port, &mut val_buf).unwrap();
    
    // ---------------------------------------
    //      GET VALUE - RUN GETS
    // ---------------------------------------

    run_benchmark(&soc_conn, &host_conn, &mut addr_buf, &mut val_buf)?;
    
    Ok(())
}
