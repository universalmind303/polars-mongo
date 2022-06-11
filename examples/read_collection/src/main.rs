use polars_mongo::prelude::*;

pub fn main() -> Result<()> {
  let connection_str = std::env::var("POLARS_MONGO_CONNECTION_URI").unwrap();
  let db = std::env::var("POLARS_MONGO_DB").unwrap();
  let collection = std::env::var("POLARS_MONGO_COLLECTION").unwrap();

  let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
      connection_str: &connection_str,
      db: &db,
      collection: &collection,
      infer_schema_length: Some(10),
      n_rows: None,
  })?
  .collect()?;
  
  dbg!(df);
  Ok(())
}
