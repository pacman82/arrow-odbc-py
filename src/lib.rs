#[no_mangle]
pub extern "C" fn arrow_odbc_connect_with_connection_string() {}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
