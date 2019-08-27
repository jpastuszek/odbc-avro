[![Latest Version]][crates.io] [![Documentation]][docs.rs] ![License]

This Rust crate extends `odbc-iter` crate functionality with ability to query Avro records and write entire `ResultSet` as Avro "Object Container File" data.

Example usage
=============

Write Avro object container file data from query.

```rust
use odbc_iter::Odbc;
use odbc_avro::{AvroConfiguration, AvroResultSet, Codec};

let mut connection = Odbc::connect(&std::env::var("DB_CONNECTION_STRING").expect("no DB_CONNECTION_STRING env set")).expect("connect to database");

// Configure handler with default `AvroConfiguration`.
let mut db = connection.handle_with_configuration(AvroConfiguration::default());

// For example query all table data from database.
let data = db.query(r#"SELECT * FROM sys.tables"#).expect("query failed");

// You can use `File` instead to write Avro object container file or any other `Write` type.
let mut buf = Vec::new();

// Write all rows as uncompressed Avro object container file where rows are represented as record object named "result_set".
data.write_avro(&mut buf, Codec::Null, "result_set").expect("write worked");

// Now `buf` contains all rows from `sys.tables` table serialized Avro object container file.
```

[crates.io]: https://crates.io/crates/odbc-avro
[Latest Version]: https://img.shields.io/crates/v/odbc-avro.svg
[Documentation]: https://docs.rs/odbc-avro/badge.svg
[docs.rs]: https://docs.rs/odbc-avro
[License]: https://img.shields.io/crates/l/odbc-avro.svg
