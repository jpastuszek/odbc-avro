pub use avro_rs::schema::Schema as AvroSchema;
pub use avro_rs::{Writer, Codec};
pub use avro_rs::types::Value as AvroValue;
use std::borrow::Cow;
use lazy_static::lazy_static;
use regex::Regex;
use odbc_iter::ValueRow;
use std::fmt;
use std::error::Error;

lazy_static! { 
    /// Avro Name as defined by standard
    static ref IS_AVRO_NAME: Regex = Regex::new("^[A-Za-z][A-Za-z0-9_]*$").unwrap(); 
    /// Avro Name but only allowing lowercase chars so it plays well with Hive
    static ref IS_AVRO_NAME_STRICT: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap(); 
    // https://play.rust-lang.org/?gist=c47950efc11c64329aab12151e9afcd4&version=stable&mode=debug&edition=2015
    /// Split by non alpha-num and split CamelCase words
    static ref SPLIT_AVRO_NAME: Regex = Regex::new(r"([A-Z]+[0-9]*[a-z]*[0-9]*|[a-z]+|[0-9]+)[^A-Za-z0-9]?").unwrap();
    static ref STARTS_WITH_NUMBER: Regex = Regex::new(r"^[0-9]").unwrap();
}

#[derive(Debug)]
pub enum OdbcAvroError {
    NameNormalizationError { orig: String, attempt: String },
}

impl fmt::Display for OdbcAvroError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OdbcAvroError::NameNormalizationError { orig, attempt } => write!(f, "failed to convert {:?} to strict Avro Name (got as far as {:?})", orig, attempt),
        }
    }
}

impl Error for OdbcAvroError {}

pub trait ToAvroSchema {
    fn to_avro_schema(&self, name: &str) -> Result<AvroSchema, OdbcAvroError>;
}

pub trait ToAvroName<'i> {
    fn to_avro_name_strict(self) -> Result<Cow<'i, str>, OdbcAvroError>;
}

pub trait IsAvroName {
    fn is_avro_name_strict(&self) -> bool;
    fn is_avro_name(&self) -> bool;
}

impl IsAvroName for str {
    fn is_avro_name_strict(&self) -> bool {
        IS_AVRO_NAME_STRICT.is_match(self)
    }

    fn is_avro_name(&self) -> bool {
        IS_AVRO_NAME.is_match(self)
    }
}

impl<'i, T> ToAvroName<'i> for T where T: Into<Cow<'i, str>> {
    fn to_avro_name_strict(self) -> Result<Cow<'i, str>, OdbcAvroError> {
        let orig: Cow<str> = self.into(); 
        if (&orig).is_avro_name_strict() {
            return Ok(orig)
        }

        let name = SPLIT_AVRO_NAME.captures_iter(&orig)
            .flat_map(|m| m.get(1))
            .map(|m| m.as_str().to_string().to_lowercase())
            .skip_while(|m| STARTS_WITH_NUMBER.is_match(m))
            .collect::<Vec<_>>()
            .join("_");

        if !name.as_str().is_avro_name_strict() {
            return Err(OdbcAvroError::NameNormalizationError { orig: orig.into_owned(), attempt: name })
        }

        Ok(Cow::Owned(name))
    }
}

/*
impl TryFromOdbcSchema for AvroSchema {
    fn try_from_odbc_schema(odbc_schema: &OdbcSchema) -> Result<Self, Problem> {
        fn odbc_data_type_to_avro_type(data_type: SqlDataType) -> &'static str {
            use odbc_sys::SqlDataType::*;
            match data_type {
                SQL_EXT_TINYINT |
                SQL_SMALLINT |
                SQL_INTEGER |
                SQL_EXT_BIGINT => "long",
                SQL_FLOAT |
                SQL_REAL |
                SQL_DOUBLE => "double",
                SQL_VARCHAR |
                SQL_CHAR |
                SQL_EXT_WCHAR |
                SQL_EXT_LONGVARCHAR |
                SQL_EXT_WVARCHAR |
                SQL_EXT_WLONGVARCHAR => "string",
                SQL_EXT_BINARY |
                SQL_EXT_VARBINARY |
                SQL_EXT_LONGVARBINARY => "bytes",
                _ => panic!(format!("got unimplemented SQL data type: {:?}", data_type)),
            }
        }

        let fields: Value = 
                odbc_schema.iter().map(|column_descriptor| {
                    let name = &column_descriptor.name.as_str().to_avro_name_strict()?;
                    Ok(if column_descriptor.nullable.unwrap_or(true) {
                        json!({
                        "name": name,
                            "type": ["null", odbc_data_type_to_avro_type(column_descriptor.data_type)]
                        })
                    } else {
                        json!({
                        "name": name,
                            "type": odbc_data_type_to_avro_type(column_descriptor.data_type)
                        })
                    })
                }).collect::<Result<Vec<_>, Problem>>()?.into();

        let json_schema = json!({
            "type": "record",
            "name": "SQLRecord",
            "fields": fields
        });

        AvroSchema::parse(&json_schema).map_problem().problem_while_with(|| format!("converting ODBC schema to Avro schema from: {:?} with JSON: {}", &odbc_schema, &json_schema))
    }
}

// THIS IS NOT FINE - I should be working with AvroSchema here or I will need to convert to_avro_name for every row!
impl TryFromRow for AvroValue {
    type Schema = AvroSchema;
    fn try_from_row(values: Values, schema: &AvroSchema) -> Result<Self, Problem> {
        if let AvroSchema::Record { fields, .. } = schema {
            Ok(AvroValue::Record(
                fields.iter().zip(values).map(|(record_field, value)| {
                    let name = record_field.name.clone();
                    let value = match value {
                        Value::Null => AvroValue::Null,
                        Value::Bool(value) => AvroValue::Boolean(value),
                        Value::Number(value) => {
                            if value.is_i64() {
                                AvroValue::Long(value.as_i64().unwrap())
                            } else if value.is_u64() {
                                AvroValue::Long(value.as_u64().unwrap() as i64) // TODO: this can overflow
                            } else if value.is_f64() {
                                AvroValue::Double(value.as_f64().unwrap())
                            } else {
                                panic!(format!("JSON Value Numbe from ODBC row is not supported: {:?}", value))
                            }
                        },
                        Value::String(value) => AvroValue::String(value),
                        // TODO: perhaps I shoud use custom Value object for ODBC since not all data types are going to be used anyway
                        Value::Array(_) => panic!("got JSON Value Array from ODBC row"),
                        Value::Object(_) => panic!("got JSON Value Object from ODBC row"),
                    };

                    // Nullable are represented as Union type (["null", type])
                    let value = if let AvroSchema::Union(_) = record_field.schema {
                        if let AvroValue::Null = value {
                            AvroValue::Union(None)
                        } else {
                            AvroValue::Union(Some(Box::new(value)))
                        }
                    } else {
                        value
                    };

                    (name, value)
                }).collect()
            ))
        } else {
            panic!("non Record Avro schemas are not supported");
        }
    }
}

pub trait ToAvroRecord {
    fn to_avro_record(self, schema: &AvroSchema) -> Result<AvroValue, Problem>;
}

impl ToAvroRecord for Value {
    fn to_avro_record(self, _schema: &AvroSchema) -> Result<AvroValue, Problem> {
        // TODO: check with schema
        match self {
            Value::Object(items) => {
                Ok(AvroValue::Record(items
                    .into_iter()
                    .map(|(key, value)| (key, value.avro()))
                    .collect::<_>()))
            },
            _ => Err(Problem::cause("JSON Value is not an object"))
        }
    }
}
*/

#[cfg(test)]
mod test {
    use super::*;
    use problem::prelude::*;

    #[test]
    fn test_to_avro_name() {
        assert_eq!("21dOd#Foo.BarBaz-quixISO9823Fro21Do.324".to_avro_name_strict().unwrap(), "d_od_foo_bar_baz_quix_iso9823_fro21_do_324");
        assert_eq!("foobar".to_avro_name_strict().unwrap(), "foobar");
        assert_eq!("123foobar".to_avro_name_strict().unwrap(), "foobar");
        assert_eq!("123.456foobar".to_avro_name_strict().unwrap(), "foobar");
        assert_eq!("cuml.pct".to_avro_name_strict().unwrap(), "cuml_pct");
        // strict
        assert_eq!("FooBar".to_avro_name_strict().unwrap(), "foo_bar");
    }

    #[test]
    #[should_panic(expected = "Failed to convert empty string to Avro Name due to: failed to convert \"\" to strict Avro Name (got as far as \"\")")]
    fn test_to_avro_empty() {
        "".to_avro_name_strict().or_failed_to("convert empty string to Avro Name");
    }

    #[test]
    #[should_panic(expected = "Failed to convert empty string to Avro Name due to: failed to convert \"12.3\" to strict Avro Name (got as far as \"\"")]
    fn test_to_avro_number() {
        "12.3".to_avro_name_strict().or_failed_to("convert empty string to Avro Name");
    }

/*
    mod odbc {
        use super::*;
        use self::AvroValue::*;

        #[test]
        fn test_hive_multiple_rows() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let hive = Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = hive.query::<AvroValue>("SELECT explode(x) AS foo1 FROM (SELECT array(42, 24) AS x) d;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo1");
                    assert_eq!(**value, Long(42));
                });
            });
            assert_matches!(data[1], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo1");
                    assert_eq!(**value, Long(24));
                });
            });
        }

        #[test]
        fn test_hive_multiple_columns() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let hive = Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = hive.query::<AvroValue>("SELECT 42 AS foo1, 24 AS foo2;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo1");
                    assert_eq!(**value, Long(42));
                });
                assert_matches!(fields[1], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo2");
                    assert_eq!(**value, Long(24));
                });
            });
        }

        #[test]
        fn test_hive_types_string() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let hive = Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = hive.query::<AvroValue>("SELECT cast('foo' AS STRING) AS foo1, cast('bar' AS VARCHAR) AS foo2;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo1");
                    assert_eq!(**value, String("foo".into()));
                });
                assert_matches!(fields[1], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo2");
                    assert_eq!(**value, String("bar".into()));
                });
            });
        }

        #[test]
        fn test_hive_types_float() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let hive = Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = hive.query::<AvroValue>("SELECT cast(1.5 AS FLOAT) AS foo1, cast(2.5 AS DOUBLE) AS foo2;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, Double(number) => assert!(number > 1.0 && number < 2.0));
                });
                assert_matches!(fields[1], (ref name, Union(Some(ref value))) => {
                    assert_eq!(name, "foo2");
                    assert_matches!(**value, Double(number) => assert!(number > 2.0 && number < 3.0));
                });
            });
        }

        #[test]
        fn test_hive_types_null() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let hive = Odbc::connect(&odbc, hive_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = hive.query::<AvroValue>("SELECT cast(NULL AS FLOAT) AS foo1, cast(NULL AS DOUBLE) AS foo2;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Union(None)) => assert_eq!(name, "foo1"));
                assert_matches!(fields[1], (ref name, Union(None)) => assert_eq!(name, "foo2"));
            });
        }

        #[test]
        fn test_report_db_not_null() {
            let odbc = Odbc::env().or_failed_to("open ODBC");
            let report_db = Odbc::connect(&odbc, report_db_connection_string().as_str()).or_failed_to("connect to Hive");
            let data = report_db.query::<AvroValue>("SELECT 42 AS foo1, 24 AS foo2;")
                .or_failed_to("failed to run query")
                .collect::<Result<Vec<_>, Problem>>()
                .or_failed_to("fetch data");

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(fields[0], (ref name, Long(value)) => {
                    assert_eq!(name, "foo1");
                    assert_eq!(value, 42);
                });
                assert_matches!(fields[1], (ref name, Long(value)) => {
                    assert_eq!(name, "foo2");
                    assert_eq!(value, 24);
                });
            });
        }

    }
    */
}
