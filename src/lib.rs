pub use avro_rs::schema::Schema as AvroSchema;
pub use avro_rs::types::Value as AvroValue;
pub use avro_rs::{Codec, Writer};
use lazy_static::lazy_static;
use odbc_iter::ResultSet;
use odbc_iter::{
    Column, ColumnType, DataAccessError, DatumAccessError, Row, TryFromColumn, TryFromRow,
};
use regex::Regex;
use serde_json::json;
use chrono::NaiveDate;
use std::borrow::Cow;
use std::error::Error;
use std::fmt;
use std::io::{Write, BufWriter};
use std::ops::Deref;
use std::marker::PhantomData;

lazy_static! {
    /// Avro Name as defined by standard
    static ref IS_AVRO_NAME: Regex = Regex::new("^[A-Za-z][A-Za-z0-9_]*$").unwrap();
    /// Avro Name but only allowing lowercase chars so it plays well with databases
    static ref IS_AVRO_NAME_STRICT: Regex = Regex::new("^[a-z][a-z0-9_]*$").unwrap();
    // https://play.rust-lang.org/?gist=c47950efc11c64329aab12151e9afcd4&version=stable&mode=debug&edition=2015
    /// Split by non alpha-num and split CamelCase words
    static ref SPLIT_AVRO_NAME: Regex = Regex::new(r"([A-Z]+[0-9]*[a-z]*[0-9]*|[a-z]+|[0-9]+)[^A-Za-z0-9]?").unwrap();
    static ref STARTS_WITH_NUMBER: Regex = Regex::new(r"^[0-9]").unwrap();
}

#[derive(Debug)]
pub enum OdbcAvroError {
    NameNormalizationError {
        orig: String,
        attempt: String,
    },
    AvroSchemaError {
        odbc_schema: Vec<ColumnType>,
        avro_schema: serde_json::Value,
        err: String,
    },
    DatumAccessError(DatumAccessError),
    DataAccessError(DataAccessError),
    WriteError(String),
}

impl fmt::Display for OdbcAvroError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OdbcAvroError::NameNormalizationError { orig, attempt } => write!(
                f,
                "failed to convert {:?} to strict Avro Name (got as far as {:?})",
                orig, attempt
            ),
            OdbcAvroError::AvroSchemaError {
                odbc_schema,
                avro_schema,
                err,
            } => write!(
                f,
                "converting ODBC schema to Avro schema from: {:?} with JSON: {}: {}",
                odbc_schema, avro_schema, err
            ),
            OdbcAvroError::WriteError(err) => write!(f, "failed to write Avor data: {}", err),
            OdbcAvroError::DatumAccessError(_) => {
                write!(f, "error getting datum from ODBC row column")
            }
            OdbcAvroError::DataAccessError(_) => write!(f, "error getting data from ODBC row"),
        }
    }
}

impl From<DatumAccessError> for OdbcAvroError {
    fn from(err: DatumAccessError) -> OdbcAvroError {
        OdbcAvroError::DatumAccessError(err)
    }
}

impl From<DataAccessError> for OdbcAvroError {
    fn from(err: DataAccessError) -> OdbcAvroError {
        OdbcAvroError::DataAccessError(err)
    }
}

impl Error for OdbcAvroError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            OdbcAvroError::NameNormalizationError { .. }
            | OdbcAvroError::AvroSchemaError { .. }
            | OdbcAvroError::WriteError { .. } => None,
            OdbcAvroError::DatumAccessError(err) => Some(err),
            OdbcAvroError::DataAccessError(err) => Some(err),
        }
    }
}

/// Represents valid Avro Name as defined by standard
#[derive(Debug)]
pub struct AvroName<'i>(Cow<'i, str>);

impl fmt::Display for AvroName<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl<'i> AvroName<'i> {
    pub fn new_strict(name: impl Into<Cow<'i, str>>) -> Result<AvroName<'i>, OdbcAvroError> {
        let orig: Cow<str> = name.into();
        if Self::is_avro_name_strict(&orig) {
            return Ok(AvroName(orig));
        }

        let name = SPLIT_AVRO_NAME
            .captures_iter(&orig)
            .flat_map(|m| m.get(1))
            .map(|m| m.as_str().to_string().to_lowercase())
            .skip_while(|m| STARTS_WITH_NUMBER.is_match(m))
            .collect::<Vec<_>>()
            .join("_");

        if !Self::is_avro_name_strict(&name) {
            return Err(OdbcAvroError::NameNormalizationError {
                orig: orig.into_owned(),
                attempt: name,
            });
        }

        Ok(AvroName(Cow::Owned(name)))
    }

    pub fn is_avro_name_strict(name: &str) -> bool {
        IS_AVRO_NAME_STRICT.is_match(name)
    }

    pub fn is_avro_name(name: &str) -> bool {
        IS_AVRO_NAME.is_match(name)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'i> Deref for AvroName<'i> {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait ToAvroSchema {
    fn to_avro_schema<'n>(&self, name: &'n str) -> Result<AvroSchema, OdbcAvroError>;
}

impl<'i> ToAvroSchema for &'i [ColumnType] {
    fn to_avro_schema<'n>(&self, name: &'n str) -> Result<AvroSchema, OdbcAvroError> {
        fn column_to_avro_type(column_type: &ColumnType) -> &'static str {
            use odbc_iter::DatumType::*;
            match column_type.datum_type {
                Bit => "boolean",
                Tinyint | Smallint | Integer => "int",
                Bigint => "long",
                Float => "float",
                Double => "double",
                String => "string",
                Json => "string",
                Timestamp | Date | Time => "string",
            }
        }

        let fields: serde_json::Value = self
            .into_iter()
            .map(|column_type| {
                let name = AvroName::new_strict(&column_type.name)?;
                Ok(if column_type.nullable {
                    json!({
                    "name": name.as_str(),
                        "type": ["null", column_to_avro_type(column_type)],
                    })
                } else {
                    json!({
                    "name": name.as_str(),
                        "type": column_to_avro_type(column_type),
                    })
                })
            })
            .collect::<Result<Vec<_>, OdbcAvroError>>()?
            .into();

        let json_schema = json!({
            "type": "record",
            "name": AvroName::new_strict(name)?.as_str(),
            "fields": fields
        });

        AvroSchema::parse(&json_schema).map_err(|err| OdbcAvroError::AvroSchemaError {
            odbc_schema: self.to_vec(),
            avro_schema: json_schema,
            err: err.to_string(),
        })
    }
}

/// How JSON column data is reformatted
#[derive(Debug)]
pub enum JsonReformat {
    /// Strip spaces and new lines
    Compact,
    /// Indented for readability
    Pretty,
}

/// In what format TIMESTAMP columns should be stored in the output
#[derive(Debug)]
pub enum TimestampFormat {
    /// As `String` in format: Y-M-D H:M:S.fff
    DefaultString,
    /// As `i64` number of milliseconds since epoch
    MillisecondsSinceEpoch,
}

pub trait Configuration: Default {
    #[inline(always)]
    fn reformat_json(&self) -> Option<JsonReformat> {
        None
    }

    #[inline(always)]
    fn timestamp_format(&self) -> TimestampFormat {
        TimestampFormat::DefaultString
    }
}

/// Default configuration with no JSON reformatting and storing timestamp as String
#[derive(Debug, Default)]
pub struct DefaultConfiguration;
impl Configuration for DefaultConfiguration {}

/// Table column represented as AvroValue
#[derive(Debug)]
pub struct AvroColumn<C: Configuration = DefaultConfiguration>(pub AvroValue, PhantomData<C>);

impl<C: Configuration> TryFromColumn for AvroColumn<C> {
    type Error = OdbcAvroError;

    fn try_from_column<'i, 's, 'c, S>(column: Column<'i, 's, 'c, S>) -> Result<Self, Self::Error> {
        #[inline(always)]
        fn to_avro<'i, 's, 'c, C: Configuration, S>(
            column: Column<'i, 's, 'c, S>,
            configuration: C,
        ) -> Result<Option<AvroValue>, DatumAccessError> {
            use odbc_iter::DatumType::*;
            Ok(match column.column_type().datum_type {
                Bit => column.into_bool()?.map(AvroValue::Boolean),
                Tinyint => column.into_i8()?.map(|v| AvroValue::Int(v as i32)),
                Smallint => column.into_i16()?.map(|v| AvroValue::Int(v as i32)),
                Integer => column.into_i32()?.map(AvroValue::Int),
                Bigint => column.into_i64()?.map(AvroValue::Long),
                Float => column.into_f32()?.map(AvroValue::Float),
                Double => column.into_f64()?.map(AvroValue::Double),
                String => column.into_string()?.map(AvroValue::String),
                Json => {
                    match configuration.reformat_json() {
                        Some(JsonReformat::Compact) => column.into_json()?.map(|j| AvroValue::String(serde_json::to_string(&j).unwrap())),
                        Some(JsonReformat::Pretty) => column.into_json()?.map(|j| AvroValue::String(serde_json::to_string_pretty(&j).unwrap())),
                        None => column.into_string()?.map(AvroValue::String),
                    }
                }
                Timestamp => column.into_timestamp()?.map(|timestamp| {
                    match configuration.timestamp_format() {
                        TimestampFormat::DefaultString => AvroValue::String(format!(
                            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
                            timestamp.year,
                            timestamp.month,
                            timestamp.day,
                            timestamp.hour,
                            timestamp.minute,
                            timestamp.second,
                            timestamp.fraction / 1_000_000
                        )),
                        TimestampFormat::MillisecondsSinceEpoch => {
                            let date_time = NaiveDate::from_ymd(
                                    i32::from(timestamp.year),
                                    u32::from(timestamp.month),
                                    u32::from(timestamp.day),
                                )
                                .and_hms_nano(
                                    u32::from(timestamp.hour),
                                    u32::from(timestamp.minute),
                                    u32::from(timestamp.second),
                                    timestamp.fraction,
                                );
                            AvroValue::Long(date_time.timestamp_millis())
                        },
                    }
                }),
                Date => column.into_date()?.map(|date| {
                    AvroValue::String(format!(
                        "{:04}-{:02}-{:02}",
                        date.year, date.month, date.day
                    ))
                }),
                Time => column.into_time()?.map(|time| {
                    AvroValue::String(format!(
                        "{:02}:{:02}:{:02}.{:03}",
                        time.hour,
                        time.minute,
                        time.second,
                        time.fraction / 1_000_000
                    ))
                }),
            })
        }
        let configuration = C::default();
        if column.column_type().nullable {
            Ok(AvroColumn(
                AvroValue::Union(Box::new(to_avro(column, configuration)?.unwrap_or(AvroValue::Null))),
                PhantomData,
            ))
        } else {
            Ok(AvroColumn(
                to_avro(column, configuration)?.expect("not-nullable column but got NULL"),
                PhantomData,
            ))
        }
    }
}

/// Table row represented as Avro record
#[derive(Debug)]
pub struct AvroRowRecord<C: Configuration = DefaultConfiguration>(AvroValue, PhantomData<C>);

impl<C: Configuration> TryFromRow for AvroRowRecord<C> {
    type Error = OdbcAvroError;

    fn try_from_row<'r, 's, 'c, S>(mut row: Row<'r, 's, 'c, S>) -> Result<Self, Self::Error> {
        let mut fields = Vec::with_capacity(row.columns() as usize);
        while let Some(column) = row.shift_column() {
            //TODO: cache names in thread local? or make ResultSet to be parametised by Schema
            //type...
            let name = AvroName::new_strict(column.column_type().name.clone())?;
            let value: AvroColumn<C> = AvroColumn::try_from_column(column)?;
            fields.push((name.0.into_owned(), value.0))
        }
        Ok(AvroRowRecord(AvroValue::Record(fields), PhantomData))
    }
}

/// Extension function for `ResultSet`.
pub trait WriteAvro {
    /// Write as `Avro` binary data.
    fn write_avro<'n, W: Write>(
        self,
        writer: &mut W,
        codec: Codec,
        name: &'n str,
    ) -> Result<usize, OdbcAvroError>;
}

impl<'h, 'c: 'h, C: Configuration, S> WriteAvro for ResultSet<'h, 'c, AvroRowRecord<C>, S> {
    fn write_avro<'n, W: Write>(
        mut self,
        writer: &mut W,
        codec: Codec,
        name: &'n str,
    ) -> Result<usize, OdbcAvroError> {
        let stdout = BufWriter::new(writer);
        let avro_schema = self.schema().to_avro_schema(name)?;
        let mut writer = Writer::with_codec(&avro_schema, stdout, codec);

        Ok(self
            .try_fold(0, |bytes, record| {
                writer
                    .append_value_ref(&record?.0)
                    .map(|written| bytes + written)
                    .map_err(|err| OdbcAvroError::WriteError(err.to_string()))
            })
            .and_then(|bytes| {
                writer
                    .flush()
                    .map(|written| bytes + written)
                    .map_err(|err| OdbcAvroError::WriteError(err.to_string()))
            })?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use problem::prelude::*;

    #[test]
    fn test_to_avro_name() {
        assert_eq!(
            AvroName::new_strict("21dOd#Foo.BarBaz-quixISO9823Fro21Do.324")
                .unwrap()
                .as_str(),
            "d_od_foo_bar_baz_quix_iso9823_fro21_do_324"
        );
        assert_eq!(AvroName::new_strict("foobar").unwrap().as_str(), "foobar");
        assert_eq!(
            AvroName::new_strict("123foobar").unwrap().as_str(),
            "foobar"
        );
        assert_eq!(
            AvroName::new_strict("123.456foobar").unwrap().as_str(),
            "foobar"
        );
        assert_eq!(
            AvroName::new_strict("cuml.pct").unwrap().as_str(),
            "cuml_pct"
        );
        // strict
        assert_eq!(AvroName::new_strict("FooBar").unwrap().as_str(), "foo_bar");
    }

    #[test]
    #[should_panic(
        expected = "Failed to convert empty string to Avro Name due to: failed to convert \"\" to strict Avro Name (got as far as \"\")"
    )]
    fn test_to_avro_empty() {
        AvroName::new_strict("").or_failed_to("convert empty string to Avro Name");
    }

    #[test]
    #[should_panic(
        expected = "Failed to convert empty string to Avro Name due to: failed to convert \"12.3\" to strict Avro Name (got as far as \"\""
    )]
    fn test_to_avro_number() {
        AvroName::new_strict("12.3").or_failed_to("convert empty string to Avro Name");
    }

    mod odbc {
        use super::*;
        use assert_matches::assert_matches;
        use odbc_iter::Odbc;

        fn connection_string() -> String {
            std::env::var("DB_CONNECTION_STRING").expect("no DB_CONNECTION_STRING env set")
        }

        #[test]
        fn test_odbc_multiple_rows() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();
            let data = db
                .query::<AvroRowRecord>(
                    "SELECT CAST(42 AS BIGINT) AS foo1 UNION SELECT CAST(24 AS BIGINT) AS foo1;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(&data[0], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 42));
                });
            });
            assert_matches!(&data[1], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 24));
                });
            });
        }

        #[test]
        fn test_odbc_multiple_columns() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();

            let data = db
                .query::<AvroRowRecord>(
                    "SELECT CAST(42 AS BIGINT) AS foo1, CAST(24 AS BIGINT) AS foo2;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(&data[0], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 42));
                });
                assert_matches!(&fields[1], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo2");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 24));
                });
            });
        }

        #[test]
        fn test_odbc_multiple_columns_normalized_names() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();

            let data = db
                .query::<AvroRowRecord>(
                    "SELECT CAST(42 AS BIGINT) AS FooBar1, CAST(24 AS BIGINT) AS fooBarBaz2;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(&data[0], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo_bar1");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 42));
                });
                assert_matches!(&fields[1], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo_bar_baz2");
                    assert_matches!(**value, AvroValue::Long(value) => assert_eq!(value, 24));
                });
            });
        }

        #[test]
        fn test_odbc_types_string() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();

            let data = db
                .query::<AvroRowRecord>(
                    "SELECT cast('foo' AS TEXT) AS foo1, cast('bar' AS VARCHAR) AS foo2;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(&data[0], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(&**value, AvroValue::String(value) => assert_eq!(value, "foo"))
                });
                assert_matches!(&fields[1], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo2");
                    assert_matches!(&**value, AvroValue::String(value) => assert_eq!(value, "bar"))
                });
            });
        }

        #[test]
        fn test_odbc_types_float() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();

            let data = db
                .query::<AvroRowRecord>(
                    "SELECT CAST(1.5 AS FLOAT) AS foo1, CAST(2.5 AS float(53)) AS foo2;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(&data[0], AvroValue::Record(fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, AvroValue::Double(number) => assert!(number > 1.0 && number < 2.0));
                });
                assert_matches!(&fields[1], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo2");
                    assert_matches!(**value, AvroValue::Double(number) => assert!(number > 2.0 && number < 3.0));
                });
            });
        }

        #[test]
        fn test_odbc_types_null() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();

            let data = db
                .query::<AvroRowRecord>(
                    "SELECT CAST(NULL AS FLOAT) AS foo1, CAST(NULL AS float(53)) AS foo2;",
                )
                .or_failed_to("failed to run query")
                .or_failed_to("fetch data")
                .map(|r| r.0)
                .collect::<Vec<AvroValue>>();

            assert_matches!(data[0], AvroValue::Record(ref fields) => {
                assert_matches!(&fields[0], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo1");
                    assert_matches!(**value, AvroValue::Null);
                });
                assert_matches!(&fields[1], (name, AvroValue::Union(value)) => {
                    assert_eq!(name, "foo2");
                    assert_matches!(**value, AvroValue::Null);
                });
            });
        }

        #[test]
        fn test_odbc_avro_write() {
            let mut connection =
                Odbc::connect(&connection_string()).or_failed_to("connect to database");
            let mut db = connection.handle();
            let data = db.query::<AvroRowRecord>("SELECT CAST(42 AS BIGINT) AS FooBar1 UNION SELECT CAST(24 AS BIGINT) AS fooBarBaz2;").expect("query failed");

            let mut buf = Vec::new();

            let bytes = data.write_avro(&mut buf, Codec::Deflate, "result_set").expect("write worked");
            assert!(bytes > 0);
            assert_eq!(bytes, buf.len());
        }

        mod monetdb {
            use super::*;

            fn connection_string() -> String {
                std::env::var("MONETDB_ODBC_CONNECTION").expect("no MONETDB_ODBC_CONNECTION env set")
            }

            #[derive(Debug, Default)]
            pub struct ReformatJsonConfiguration;
            impl Configuration for ReformatJsonConfiguration {
                fn reformat_json(&self) -> Option<JsonReformat> {
                    Some(JsonReformat::Compact)
                }
            }

            #[test]
            fn test_odbc_avro_write_json_default() {
                let mut connection =
                    Odbc::connect(&connection_string()).or_failed_to("connect to database");
                let mut db = connection.handle();
                let data = db.query::<AvroRowRecord>(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#).expect("query failed");

                let mut buf = Vec::new();

                data.write_avro(&mut buf, Codec::Null, "result_set").expect("write worked");

                // find string in binary output
                let json = r#"{ "foo": 42 }"#.as_bytes();
                assert!(buf.windows(json.len()).any(|w| w == json))
            }

            #[test]
            fn test_odbc_avro_write_json_reformat() {
                let mut connection =
                    Odbc::connect(&connection_string()).or_failed_to("connect to database");
                let mut db = connection.handle();
                let data = db.query::<AvroRowRecord<ReformatJsonConfiguration>>(r#"SELECT CAST('{ "foo": 42 }' AS JSON)"#).expect("query failed");

                let mut buf = Vec::new();

                data.write_avro(&mut buf, Codec::Null, "result_set").expect("write worked");

                // find string in binary output
                let json = r#"{"foo":42}"#.as_bytes();
                assert!(buf.windows(json.len()).any(|w| w == json))
            }
        }
    }
}
