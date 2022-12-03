use cozorocks::into_result;

#[test]
fn test_db_open() {
    autocxx::moveit::moveit! {
        let mut options = cozorocks::DbOptionsWrapper::new("./db");
        let status = options.as_mut().load("./db.toml");
    };
    into_result(&status).unwrap();
    options.as_mut().set_create_if_missing(true);
    options.as_mut().set_create_missing_column_families(true);
    options.as_mut().sort_and_complete_missing(4);
    autocxx::moveit::moveit! {
        let mut db = cozorocks::TransactionDBWrapper::new();
        let txn_db_opts = cozorocks::new_transaction_db_options();
        let status = db.as_mut().open(options, &txn_db_opts);
    }
    into_result(&status).unwrap();
}
