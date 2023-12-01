use std::ptr::null_mut;
use std::time::Instant;
use rdma_sys::*;
use libc::c_void;


#[derive(Debug)]
pub enum Error {
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
    QueryQP,
}

pub fn reg_read(id: *mut rdma_cm_id, base: u64, len: usize) -> Result<*mut ibv_mr, Error> {
    let reg_mem = unsafe { rdma_reg_read(id, base as *mut c_void, len) };
    if reg_mem.is_null() {
        unsafe { rdma_dereg_mr(reg_mem); }
        return Err(Error::RegisterMem);
    }
    Ok(reg_mem)
}


pub fn accept(id: *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_accept(id, null_mut()) };
    if ret != 0 {
	return Err(Error::Accept);
    }
    Ok(())
}

pub fn connect(id: *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_connect(id, null_mut()) };
    if ret != 0 {
        unsafe { rdma_disconnect(id) };
	return Err(Error::Connect);
    }
    Ok(())
}

pub fn listen(id: *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_listen(id, 0) };
    if ret != 0 {
        unsafe { rdma_destroy_ep(id); }
        return Err(Error::Listen);
    }
    Ok(())
}

pub fn get_request(listen_id: *mut rdma_cm_id, id: *mut *mut rdma_cm_id) -> Result<(), Error> {
    let ret = unsafe { rdma_get_request(listen_id, id) };
    if ret != 0 {
        unsafe { rdma_destroy_ep(listen_id); }
        return Err(Error::GetRequest);
    }
    Ok(())
}

pub fn post_send_and_wait(id: *mut rdma_cm_id, src_buf: &mut[u8], mr: *mut ibv_mr, flags: u32) -> Result<(), Error> {
    post_send(id, src_buf, mr, flags)?;
    wait_for_send(id)?;
    Ok(())
}

pub fn post_send(id: *mut rdma_cm_id, src_buf: &mut[u8], mr: *mut ibv_mr, flags: u32) -> Result<(), Error> {
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

pub fn wait_for_send(id: *mut rdma_cm_id) -> Result<(), Error> {
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

pub fn post_recv_and_wait(id: *mut rdma_cm_id, buf: &mut [u8], mr: *mut ibv_mr) -> Result<(), Error> {
    post_recv(id, buf, mr)?;
    wait_for_recv(id)?;
    Ok(())
}

pub fn post_recv(id: *mut rdma_cm_id, buf: &mut [u8], mr: *mut ibv_mr) -> Result<(), Error> {
    let ret = unsafe { rdma_post_recv(id, null_mut(), buf.as_mut_ptr().cast(), buf.len(), mr) };
    if ret != 0 {
        return Err(Error::PostRecv);
    }
    Ok(())
}

pub fn wait_for_recv(id: *mut rdma_cm_id) -> Result<(), Error> {
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

pub fn post_read_and_wait(
    id: *mut rdma_cm_id,
    buf: &mut [u8],
    mr: *mut ibv_mr,
    raddr: u64,
    rkey: u32
) -> Result<(), Error> {
    let now = Instant::now();
    post_read(id, buf, mr, raddr, rkey)?;
    // let elapsed = now.elapsed().as_nanos();
    // println!("after post read: {}ns", elapsed);

    // let now1 = Instant::now();
    wait_for_send(id)?;
    let elapsed = now.elapsed().as_nanos();
    println!("after wait for send: {}ns", elapsed);
    Ok(())
}

pub fn post_read(
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

pub fn get_new_cm_id(addr: &str, port: &str, id: *mut *mut rdma_cm_id, init: &mut ibv_qp_init_attr, is_passive: bool) -> Result<(), Error> {
    // create addr info
    let mut addr_info: *mut rdma_addrinfo = null_mut();
    let mut hints = unsafe { std::mem::zeroed::<rdma_addrinfo>() };
    if is_passive {
        hints.ai_flags = RAI_PASSIVE.try_into().unwrap();
    }
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

pub fn query_qp(id: *mut rdma_cm_id, qp_attr: *mut ibv_qp_attr, init: *mut ibv_qp_init_attr) -> Result<(), Error> {
    let ret = unsafe {ibv_query_qp( (*id).qp, qp_attr, ibv_qp_attr_mask::IBV_QP_CAP.0.try_into().unwrap(),init,)};
    if ret != 0 {
        println!("ibv_query_qp");
        unsafe { rdma_destroy_ep(id); }
        return Err(Error::QueryQP);
    }
    return Ok(());
}