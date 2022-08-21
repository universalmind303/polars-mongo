use polars::prelude::*;
use polars_mongo::prelude::*;

pub fn main() -> Result<()> {
    let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
    let db = std::env::var("POLARS_MONGO_DB").unwrap();
    let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();

    let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
        batch_size: None,
        connection_str,
        db,
        collection,
        infer_schema_length: Some(1000),
        n_rows: None,
    })?
    .collect()?;

    dbg!(df);
    Ok(())
}
