use polars_mongo::prelude::*;

pub fn main() -> Result<()> {
    let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
    let db = std::env::var("POLARS_MONGO_DB").unwrap();
    let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();

    let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
        batch_size: Some(2000),
        connection_str,
        db,
        collection,
        infer_schema_length: Some(100),
        n_rows: Some(20000),
    })?
    .collect()?;

    dbg!(df);
    Ok(())
}
