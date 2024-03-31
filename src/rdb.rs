use base64::prelude::*;
pub const BASE64_RDB_EMPTY: &str = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

pub fn get_rdb_bytes() -> Vec<u8> {
    BASE64_STANDARD.decode(BASE64_RDB_EMPTY.as_bytes()).unwrap()
}
