use std::ptr::null_mut;

pub const MEM_SIZE: usize = 0x100000; /* 1MB */
pub const VAL_SIZE: usize = 64;
pub const N_KEYS: usize = MEM_SIZE / VAL_SIZE;
pub const KEY_SIZE: usize = 8;
pub const INDEX_SIZE: usize = N_KEYS * KEY_SIZE;
// this is assuming that the first N_KEYS_ON_HOST keys will be on the host
pub const N_KEYS_ON_HOST: usize = N_KEYS;
pub const N_KEYS_ON_SOC: usize = N_KEYS - N_KEYS_ON_HOST;

#[derive(Clone, Copy)]
pub struct KVS {
    pub soc_index_base: u64,
    pub soc_index_rkey: u32,
    pub soc_values_rkey: u32,
    pub host_index_base: u64,
    pub host_index_rkey: u32,
    pub host_values_rkey: u32,
}

pub struct KVAddr {
    pub addr: u64,
    pub is_cached: bool,
    pub num_accesses: u8
}

pub fn serialize_kv_addr(struct_kv: KVAddr) -> u64 {

    let mut to_ret: u64;

    to_ret = struct_kv.addr;
    if !struct_kv.is_cached {
        to_ret = to_ret + 1;
    }
    let accesses: u64 = struct_kv.num_accesses as u64;
    to_ret = to_ret + (accesses << 50);

    return to_ret;
}

pub fn deserialize_kv_addr(addr_val: u64) -> KVAddr {

    let cached_bit = addr_val & 1;
    let num_accesses: u64 = addr_val >> 50;
    let trunc_num_accesses: u8 = num_accesses as u8;

    let addr_no_access = (addr_val << 14) >> 14;
    let addr_no_cached_bit = (addr_no_access >> 1) << 1;

    return KVAddr { addr: addr_no_cached_bit, is_cached: cached_bit == 0, num_accesses: trunc_num_accesses };
}

/// places a serialized kvaddr for all 8-bit keys, starting at base_pointer, that point to addr_to_put
pub fn put_addr_in_index_for_appropriate_keys(kvs: &KVS, addr_to_put: u64, soc: bool) {

    let idx_base = if soc { kvs.soc_index_base } else { kvs.host_index_base };

    let first_key = if soc {
        N_KEYS_ON_HOST as u64
    } else {
        0 as u64
    };

    let last_key = if soc {
        N_KEYS as u64
    } else {
        N_KEYS_ON_HOST as u64
    };

    for key_val in first_key..last_key {
	
        let key_offset = key_val * 8;
	let addr_offset = key_val * VAL_SIZE as u64;
        let ass_addr = (idx_base + key_offset) as *mut u64;
	
	
        let to_put = serialize_kv_addr(KVAddr{
            addr: addr_to_put + addr_offset,
            is_cached: soc,
            num_accesses: 0,});
        
        unsafe { *ass_addr = to_put };
    }
}


pub fn addr_given_key(key: u8, base_pointer: u64) -> u64 {

    let offset = (key * 8) as u64;
    let ass_addr = base_pointer + offset;

    return ass_addr;

}


/// initialize the key value store by memmaping a region of memory that can hold 256 addresses, returning the base pointer of the mapped region
pub fn init_kv_store(soc: bool) -> KVS {

    let res = unsafe {
	libc::mmap(
	    null_mut(),
	    INDEX_SIZE, 
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
	soc_index_base: if soc { res as u64 }  else {  0 },
	soc_index_rkey: 0,
	soc_values_rkey: 0,
	host_index_base: if !soc { res as u64 } else { 0 },
	host_index_rkey: 0,
	host_values_rkey: 0,
    }
} 
