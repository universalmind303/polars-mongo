use crate::conversion::*;
use mongodb::bson::Bson;
use num::traits::NumCast;
use polars::export::arrow::types::NativeType;
use polars::prelude::*;

pub(crate) fn init_buffers(
    schema: &polars::prelude::Schema,
    capacity: usize,
) -> PolarsResult<PlIndexMap<String, Buffer>> {
    schema
        .iter()
        .map(|(name, dtype)| {
            let builder = match &dtype {
                DataType::Boolean => Buffer::Boolean(BooleanChunkedBuilder::new(name, capacity)),
                DataType::Int32 => Buffer::Int32(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::Int64 => Buffer::Int64(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::UInt32 => Buffer::UInt32(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::UInt64 => Buffer::UInt64(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::Float32 => Buffer::Float32(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::Float64 => Buffer::Float64(PrimitiveChunkedBuilder::new(name, capacity)),
                DataType::Utf8 => {
                    Buffer::Utf8(Utf8ChunkedBuilder::new(name, capacity, capacity * 5))
                }
                DataType::Datetime(_, _) => {
                    Buffer::Datetime(PrimitiveChunkedBuilder::new(name, capacity))
                }
                DataType::Date => Buffer::Date(PrimitiveChunkedBuilder::new(name, capacity)),
                _ => Buffer::All((Vec::with_capacity(capacity), name)),
            };
            Ok((name.clone(), builder))
        })
        .collect()
}

#[allow(clippy::large_enum_variant)]
pub(crate) enum Buffer<'a> {
    Boolean(BooleanChunkedBuilder),
    Int32(PrimitiveChunkedBuilder<Int32Type>),
    Int64(PrimitiveChunkedBuilder<Int64Type>),
    UInt32(PrimitiveChunkedBuilder<UInt32Type>),
    UInt64(PrimitiveChunkedBuilder<UInt64Type>),
    Float32(PrimitiveChunkedBuilder<Float32Type>),
    Float64(PrimitiveChunkedBuilder<Float64Type>),
    Utf8(Utf8ChunkedBuilder),
    Datetime(PrimitiveChunkedBuilder<Int64Type>),
    Date(PrimitiveChunkedBuilder<Int32Type>),
    All((Vec<AnyValue<'a>>, &'a str)),
}

impl<'a> Buffer<'a> {
    pub(crate) fn into_series(self) -> PolarsResult<Series> {
        let s = match self {
            Buffer::Boolean(v) => v.finish().into_series(),
            Buffer::Int32(v) => v.finish().into_series(),
            Buffer::Int64(v) => v.finish().into_series(),
            Buffer::UInt32(v) => v.finish().into_series(),
            Buffer::UInt64(v) => v.finish().into_series(),
            Buffer::Float32(v) => v.finish().into_series(),
            Buffer::Float64(v) => v.finish().into_series(),
            Buffer::Datetime(v) => v
                .finish()
                .into_series()
                .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
                .unwrap(),
            Buffer::Date(v) => v.finish().into_series().cast(&DataType::Date).unwrap(),
            Buffer::Utf8(v) => v.finish().into_series(),
            Buffer::All((vals, name)) => Series::new(name, vals),
        };
        Ok(s)
    }

    pub(crate) fn add_null(&mut self) {
        match self {
            Buffer::Boolean(v) => v.append_null(),
            Buffer::Int32(v) => v.append_null(),
            Buffer::Int64(v) => v.append_null(),
            Buffer::UInt32(v) => v.append_null(),
            Buffer::UInt64(v) => v.append_null(),
            Buffer::Float32(v) => v.append_null(),
            Buffer::Float64(v) => v.append_null(),
            Buffer::Utf8(v) => v.append_null(),
            Buffer::Datetime(v) => v.append_null(),
            Buffer::Date(v) => v.append_null(),
            Buffer::All((v, _)) => v.push(AnyValue::Null),
        };
    }
    pub(crate) fn add(&mut self, value: &Bson) -> PolarsResult<()> {
        use Buffer::*;
        match self {
            Boolean(buf) => {
                match value {
                    Bson::Boolean(v) => buf.append_value(*v),
                    _ => buf.append_null(),
                }
                Ok(())
            }
            Int32(buf) => {
                let n = deserialize_number::<i32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Int64(buf) => {
                let n = deserialize_number::<i64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            UInt64(buf) => {
                let n = deserialize_number::<u64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            UInt32(buf) => {
                let n = deserialize_number::<u32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Float32(buf) => {
                let n = deserialize_float::<f32>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }
            Float64(buf) => {
                let n = deserialize_float::<f64>(value);
                match n {
                    Some(v) => buf.append_value(v),
                    None => buf.append_null(),
                }
                Ok(())
            }

            Utf8(buf) => {
                match value {
                    Bson::RegularExpression(r) => buf.append_value(r.to_string()),
                    Bson::ObjectId(oid) => buf.append_value(oid.to_hex()),
                    Bson::JavaScriptCode(v) => buf.append_value(v),
                    Bson::String(v) => buf.append_value(v),
                    Bson::Document(doc) => buf.append_value(doc.to_string()),
                    Bson::Array(arr) => buf.append_value(format!("{:#?}", arr)),
                    Bson::Symbol(s) => buf.append_value(s),
                    _ => buf.append_null(),
                }
                Ok(())
            }
            Datetime(buf) => {
                let v = deserialize_date::<i64>(value);
                buf.append_option(v);
                Ok(())
            }
            Date(buf) => {
                let v = deserialize_date::<i32>(value);
                buf.append_option(v);
                Ok(())
            }
            All((buf, _)) => {
                let av: Wrap<AnyValue> = value.into();
                buf.push(av.0);
                Ok(())
            }
        }
    }
}
fn deserialize_float<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        _ => None,
    }
}

fn deserialize_number<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        _ => None,
    }
}

fn deserialize_date<T: NativeType + NumCast>(value: &Bson) -> Option<T> {
    match value {
        Bson::Double(num) => num::traits::cast::<f64, T>(*num),
        Bson::Int32(num) => num::traits::cast::<i32, T>(*num),
        Bson::Int64(num) => num::traits::cast::<i64, T>(*num),
        Bson::Boolean(b) => num::traits::cast::<i32, T>(*b as i32),
        Bson::DateTime(dt) => num::traits::cast::<i64, T>(dt.timestamp_millis()),
        _ => None,
    }
}
