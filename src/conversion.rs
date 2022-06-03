use polars::prelude::*;

use mongodb::bson::{Bson, Document};

#[derive(Debug)]
#[repr(transparent)]
pub struct Wrap<T>(pub T);

impl<T> Clone for Wrap<T>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Wrap(self.0.clone())
    }
}
impl<T> From<T> for Wrap<T> {
    fn from(t: T) -> Self {
        Wrap(t)
    }
}

impl From<&Document> for Wrap<DataType> {
    fn from(doc: &Document) -> Self {
        let fields = doc.iter().map(|(key, value)| {
            let dtype: Wrap<DataType> = value.into();
            Field::new(key, dtype.0)
        });
        DataType::Struct(fields.collect()).into()
    }
}

impl From<&Bson> for Wrap<DataType> {
    fn from(bson: &Bson) -> Self {
        let dt = match bson {
            Bson::Double(_) => DataType::Float64,
            Bson::String(_) => DataType::Utf8,
            Bson::Array(arr) => {
                // todo! add proper inference.
                let dt = arr
                    .get(0)
                    .map(|i| {
                        let dt: Self = i.into();
                        dt.0
                    })
                    .unwrap_or(DataType::Null);

                DataType::List(Box::new(dt))
            }
            Bson::Boolean(_) => DataType::Boolean,
            Bson::Null => DataType::Null,
            Bson::Int32(_) => DataType::Int32,
            Bson::Int64(_) => DataType::Int64,
            Bson::Timestamp(_) => DataType::Utf8,
            Bson::Document(doc) => return doc.into(),
            Bson::DateTime(_) => DataType::Datetime(TimeUnit::Milliseconds, None),
            Bson::ObjectId(_) => DataType::Utf8,
            Bson::Symbol(_) => DataType::Utf8,
            Bson::Undefined => DataType::Unknown,
            _ => DataType::Utf8,
        };
        Wrap(dt)
    }
}
impl<'a> From<&'a Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: &'a Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(*v),
            Bson::String(v) => AnyValue::Utf8(v),
            Bson::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("", vals);
                AnyValue::List(s)
            }
            Bson::Boolean(b) => AnyValue::Boolean(*b),
            Bson::Null | Bson::Undefined => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(*v),
            Bson::Int64(v) => AnyValue::Int64(*v),
            Bson::Timestamp(v) => AnyValue::Utf8Owned(format!("{:#?}", v)),
            Bson::Binary(b) => {
                let s = Series::new("", &b.bytes);
                AnyValue::List(s)
            }
            Bson::ObjectId(oid) => AnyValue::Utf8Owned(oid.to_string()),
            Bson::Symbol(s) => AnyValue::Utf8Owned(s.to_string()),
            v => AnyValue::Utf8Owned(format!("{:#?}", v)),
        };
        Wrap(dt)
    }
}

impl<'a> From<Bson> for Wrap<AnyValue<'a>> {
    fn from(bson: Bson) -> Self {
        let dt = match bson {
            Bson::Double(v) => AnyValue::Float64(v),
            Bson::String(v) => AnyValue::Utf8Owned(v),
            Bson::Array(arr) => {
                let vals: Vec<Wrap<AnyValue>> = arr.iter().map(|v| v.into()).collect();
                // Wrap is transparent, so this is safe
                let vals = unsafe { std::mem::transmute::<_, Vec<AnyValue>>(vals) };
                let s = Series::new("", vals);
                AnyValue::List(s)
            }
            Bson::Boolean(b) => AnyValue::Boolean(b),
            Bson::Null | Bson::Undefined => AnyValue::Null,
            Bson::Int32(v) => AnyValue::Int32(v),
            Bson::Int64(v) => AnyValue::Int64(v),
            Bson::Timestamp(v) => AnyValue::Utf8Owned(format!("{:#?}", v)),
            Bson::Binary(b) => {
                let s = Series::new("", &b.bytes);
                AnyValue::List(s)
            }
            Bson::ObjectId(oid) => AnyValue::Utf8Owned(oid.to_string()),
            Bson::Symbol(s) => AnyValue::Utf8Owned(s.to_string()),
            v => AnyValue::Utf8Owned(format!("{:#?}", v)),
        };
        Wrap(dt)
    }
}