use std::ptr::null_mut;

use arrow_odbc::odbc_api::{sys::AttrConnectionPooling, Environment};

use crate::{try_, ArrowOdbcError};

/// Enable connection pooling in the ODBC Driver manager
#[no_mangle]
pub unsafe extern "C" fn arrow_odbc_enable_connection_pooling() -> *mut ArrowOdbcError {
    try_!(Environment::set_connection_pooling(
        AttrConnectionPooling::DriverAware
    ));
    null_mut() // means Ok(())
}
