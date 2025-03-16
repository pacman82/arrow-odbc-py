use std::ptr::null_mut;

use arrow_odbc::odbc_api::{Environment, sys::AttrConnectionPooling};

use crate::{ArrowOdbcError, try_};

/// Enable connection pooling in the ODBC Driver manager
#[unsafe(no_mangle)]
pub unsafe extern "C" fn arrow_odbc_enable_connection_pooling() -> *mut ArrowOdbcError {
    try_!(unsafe { Environment::set_connection_pooling(AttrConnectionPooling::DriverAware) });
    null_mut() // means Ok(())
}
