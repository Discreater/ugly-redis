use std::collections::HashMap;
use std::io;
use std::path::Path;

use bytes::{Buf, Bytes};
use tracing::{debug, info};

use crate::db::{Db, ExpireTable, KvTable};

const EOF: u8 = 0xFF;
const SELECTDB: u8 = 0xFE;
const EXPIRETIME: u8 = 0xFD;
const OBJECTIDLETIME: u8 = 0xFC;
const RESIZEDB: u8 = 0xFB;
const AUX: u8 = 0xFA;

enum ParseError {
    UnexpectedEof,
    InvalidData(String),
}

enum SizeError {
    UnexpectedEof,
    Special(u8),
}

impl From<SizeError> for ParseError {
    fn from(err: SizeError) -> ParseError {
        match err {
            SizeError::UnexpectedEof => ParseError::UnexpectedEof,
            SizeError::Special(n) => ParseError::InvalidData(format!(
                "require size encoding but get string encoding of type: {:x}",
                n
            )),
        }
    }
}

impl From<ParseError> for io::Error {
    fn from(err: ParseError) -> io::Error {
        match err {
            ParseError::UnexpectedEof => {
                io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected end of rdb file")
            }
            ParseError::InvalidData(msg) => io::Error::new(io::ErrorKind::InvalidData, msg),
        }
    }
}

impl From<()> for ParseError {
    fn from(_: ()) -> ParseError {
        ParseError::UnexpectedEof
    }
}

impl From<()> for SizeError {
    fn from(_: ()) -> SizeError {
        SizeError::UnexpectedEof
    }
}

type ParseResult<T> = Result<T, ParseError>;

pub fn init(dir: &Option<String>, dbfilename: &Option<String>) -> io::Result<Db> {
    if let Some(dbfilename) = dbfilename {
        let path = if let Some(dir) = dir {
            Path::new(&dir).join(dbfilename)
        } else {
            let dir = std::env::current_dir()?;
            dir.join(dbfilename)
        };
        let rdb = std::fs::read(path);
        if let Ok(rdb) = rdb {
            let (kv, expire) = parse_rdb_file(&mut Bytes::from(rdb))?;
            return Ok(Db { kv, expire });
        } else {
            Ok(Db::default())
        }
    } else {
        Ok(Db::default())
    }
}

fn parse_rdb_file(buf: &mut Bytes) -> ParseResult<(KvTable, ExpireTable)> {
    // 1. Header section
    let mut header = safe_bytes(buf, 9)?;
    let magic = header.split_to(5);
    debug!("rdb header magic: {:?}", String::from_utf8(magic.to_vec()));
    let version = header;
    debug!("rdb version: {:?}", String::from_utf8(version.to_vec()));

    // 2. Metadata section
    loop {
        if buf.is_empty() {
            break;
        }
        let head = buf[0];
        match head {
            AUX => {
                buf.advance(1);
                let key = parse_string(buf)?;
                let val = parse_string(buf)?;
                debug!("aux key: {:?}, value: {:?}", key, val);
                continue;
            }
            SELECTDB | EXPIRETIME | OBJECTIDLETIME | RESIZEDB => {
                break;
            }
            _ => {
                return Err(ParseError::InvalidData(format!(
                    "unexpected metadata section head: {:x}",
                    head
                )));
            }
        }
    }
    // 3. Database section
    require_u8(buf, SELECTDB)?;
    let index = safe_size(buf)?;
    info!("index of the database: {}", index);
    require_u8(buf, RESIZEDB)?;
    let kv_table_len = safe_size(buf)?;
    info!("table length: {}", kv_table_len);
    let expire_table_len = safe_size(buf)?;
    info!("expire table length: {}", expire_table_len);

    let mut kv_table = HashMap::with_capacity(kv_table_len);
    let mut expire_table = HashMap::with_capacity(expire_table_len);

    for _ in 0..kv_table_len {
        let mut value_type = safe_u8(buf)?;
        let expire_time = if value_type == OBJECTIDLETIME {
            let expire_time_mil = safe_u64_le(buf)?;
            value_type = safe_u8(buf)?;
            Some(expire_time_mil)
        } else if value_type == EXPIRETIME {
            let expire_time_sec = safe_u32_le(buf)?;
            value_type = safe_u8(buf)?;
            Some(expire_time_sec as u64 * 1000)
        } else {
            None
        };
        let key = parse_string(buf)?;

        let value = parse_value(buf, value_type)?;
        debug!("key: {:?}, value: {:?}", key, value);

        kv_table.insert(key.clone(), value);
        if let Some(expire_time) = expire_time {
            expire_table.insert(key, expire_time);
        }
    }
    // 4. End of file section
    require_u8(buf, EOF)?;
    let _crc64 = safe_bytes(buf, 8)?;
    Ok((kv_table, expire_table))
}

fn parse_value(buf: &mut Bytes, ty: u8) -> ParseResult<String> {
    match ty {
        0 => parse_string(buf),
        _ => {
            unimplemented!("value type: {:x}", ty);
        }
    }
}

fn parse_string(buf: &mut Bytes) -> ParseResult<String> {
    match safe_size(buf) {
        Ok(len) => {
            let bytes = safe_bytes(buf, len)?;
            let s = String::from_utf8(bytes.to_vec())
                .map_err(|_| ParseError::InvalidData("invalid utf8".to_string()))?;
            Ok(s)
        }
        Err(SizeError::Special(remain)) => match remain {
            0 => {
                let val = safe_u8(buf)? as i8;
                Ok(val.to_string())
            }
            1 => {
                let val = i16::from_le_bytes(safe_bytes_const::<2>(buf)?);
                Ok(val.to_string())
            }
            2 => {
                let val = i32::from_le_bytes(safe_bytes_const::<4>(buf)?);
                Ok(val.to_string())
            }
            3 => {
                unimplemented!("compressed strings")
            }
            n => {
                return Err(ParseError::InvalidData(format!(
                    "invalid string size type: {:x}",
                    n
                )));
            }
        },
        Err(_) => Err(ParseError::UnexpectedEof),
    }
}

fn require_u8(buf: &mut Bytes, expect: u8) -> ParseResult<()> {
    let actual = safe_u8(buf)?;
    if actual != expect {
        return Err(ParseError::InvalidData(format!(
            "expected {:x}, got {:x}",
            expect, actual
        )));
    }
    Ok(())
}

fn safe_size(buf: &mut Bytes) -> Result<usize, SizeError> {
    let first = safe_u8(buf)?;
    let ty = first >> 6;
    let remain = first & 0b0011_1111;
    match ty {
        0b00 => {
            let size = remain;
            Ok(size as usize)
        }
        0b01 => {
            let second = safe_u8(buf)?;
            let size = ((remain as u16) << 8) | second as u16;
            Ok(size as usize)
        }
        0b10 => {
            let size = safe_u32_be(buf)?;
            Ok(size as usize)
        }
        _ => Err(SizeError::Special(remain)),
    }
}

fn safe_u8(buf: &mut Bytes) -> Result<u8, ()> {
    if buf.is_empty() {
        return Err(());
    }
    Ok(buf.get_u8())
}

fn safe_u32_be(buf: &mut Bytes) -> Result<u32, ()> {
    if buf.len() < 4 {
        return Err(());
    }
    Ok(buf.get_u32())
}

fn safe_u32_le(buf: &mut Bytes) -> Result<u32, ()> {
    if buf.len() < 4 {
        return Err(());
    }
    Ok(buf.get_u32_le())
}

fn safe_u64_le(buf: &mut Bytes) -> Result<u64, ()> {
    if buf.len() < 8 {
        return Err(());
    }
    Ok(buf.get_u64_le())
}

fn safe_bytes(buf: &mut Bytes, len: usize) -> Result<Bytes, ()> {
    if buf.len() < len {
        return Err(());
    }
    let bytes = buf.split_to(len);
    Ok(bytes)
}

fn safe_bytes_const<const N: usize>(buf: &mut Bytes) -> Result<[u8; N], ()> {
    if buf.len() < N {
        return Err(());
    }
    let mut bytes = [0; N];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}
