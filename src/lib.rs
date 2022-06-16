#![deny(clippy::all)]

mod bson;
mod conversion;
pub mod prelude;

use napi::bindgen_prelude::{External, Result as JsResult};

use crate::bson::buffer::*;

use conversion::Wrap;
use polars::export::rayon::prelude::*;
use polars::{frame::row::*, prelude::*};
use polars_core::POOL;

use mongodb::{
    bson::{Bson, Document},
    options::{ClientOptions, FindOptions},
    sync::{Client, Collection, Cursor},
};
use polars_core::utils::accumulate_dataframes_vertical;

#[macro_use]
extern crate napi_derive;

pub struct MongoScan {
    client_options: ClientOptions,
    db: String,
    collection_name: String,
    pub collection: Option<Collection<Document>>,
    pub n_threads: Option<usize>,
    pub batch_size: Option<usize>,
    pub rechunk: bool,
}

impl MongoScan {
    pub fn with_rechunk(mut self, rechunk: bool) -> Self {
        self.rechunk = rechunk;
        self
    }
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn new(connection_str: String, db: String, collection: String) -> Result<Self> {
        let client_options = ClientOptions::parse(connection_str).unwrap();

        Ok(MongoScan {
            client_options,
            db,
            collection_name: collection,
            collection: None,
            n_threads: None,
            rechunk: false,
            batch_size: None,
        })
    }

    fn get_collection(&self) -> Collection<Document> {
        println!("get_collection");
        let client = Client::with_options(self.client_options.clone()).unwrap();
        let database = client.database(&self.db);
        database.collection::<Document>(&self.collection_name)
    }

    fn parse_lines<'a>(
        &self,
        mut cursor: Cursor<Document>,
        buffers: &mut PlIndexMap<String, Buffer<'a>>,
    ) -> mongodb::error::Result<()> {
        while let Some(Ok(doc)) = cursor.next() {
            buffers.iter_mut().for_each(|(s, inner)| match doc.get(s) {
                Some(v) => inner.add(v).expect("inner.add(v)"),
                None => inner.add_null(),
            });
        }
        Ok(())
    }
}

impl AnonymousScan for MongoScan {
    fn scan(&self, scan_opts: AnonymousScanOptions) -> Result<DataFrame> {
        println!("AnonymousScan::scan");

        let collection = &self.get_collection();

        let projection = scan_opts.output_schema.clone().map(|schema| {
            let prj = schema
                .iter_names()
                .map(|name| (name.clone(), Bson::Int64(1)));

            Document::from_iter(prj)
        });

        let mut find_options = FindOptions::default();
        find_options.projection = projection;
        find_options.batch_size = self.batch_size.map(|b| b as u32);

        let schema = scan_opts.output_schema.unwrap_or(scan_opts.schema);

        // if no n_rows we need to get the count from mongo.
        let n_rows = scan_opts
            .n_rows
            .unwrap_or_else(|| collection.estimated_document_count(None).unwrap() as usize);

        let n_threads = self.n_threads.unwrap_or_else(|| POOL.current_num_threads());

        let rows_per_thread = n_rows / n_threads;

        let dfs = POOL.install(|| {
            (0..n_threads)
                .into_par_iter()
                .map(|idx| {
                    let mut find_options = find_options.clone();

                    let start = idx * rows_per_thread;

                    find_options.skip = Some(start as u64);
                    find_options.limit = Some(rows_per_thread as i64);

                    let cursor = collection.find(None, Some(find_options));
                    let mut buffers = init_buffers(schema.as_ref(), rows_per_thread)?;

                    self.parse_lines(cursor.unwrap(), &mut buffers)
                        .map_err(|err| PolarsError::ComputeError(format!("{:#?}", err).into()))?;

                    DataFrame::new(
                        buffers
                            .into_values()
                            .map(|buf| buf.into_series())
                            .collect::<Result<_>>()?,
                    )
                })
                .collect::<Result<Vec<_>>>()
        })?;
        let mut df = accumulate_dataframes_vertical(dfs)?;

        if self.rechunk {
            df.rechunk();
        }
        Ok(df)
    }

    fn schema(&self, infer_schema_length: Option<usize>) -> Result<Schema> {
        let now = std::time::Instant::now();
        println!("infer_schema_length = {:#?}", infer_schema_length);
        println!("AnonymousScan::schema {:#?}", now.elapsed());

        let collection = self.get_collection();
        println!("Done getting collection {:#?}", now.elapsed());

        let infer_options = FindOptions::builder()
            .limit(infer_schema_length.map(|i| i as i64))
            .build();

        println!("start cursor: {:#?}", now.elapsed());

        let res = collection
            .find(None, Some(infer_options))
            .map_err(|err| PolarsError::ComputeError(format!("{:#?}", err).into()))?;

        let iter = res.map(|doc| {
            let val = doc.unwrap();
            let v = val.into_iter().map(|(key, value)| {
                let dtype: Wrap<DataType> = (&value).into();
                (key, dtype.0)
            });
            v.collect()
        });

        println!("end cursor:  {:#?}", now.elapsed());
        println!("start infer_schema: {:#?}", now.elapsed());

        let schema = infer_schema(iter, infer_schema_length.unwrap_or(1000));
        println!("end infer_schema: {:#?}", now.elapsed());
        println!("schema = {:#?}", schema);

        Ok(schema)
    }

    fn allows_predicate_pushdown(&self) -> bool {
        true
    }
    fn allows_projection_pushdown(&self) -> bool {
        true
    }
    fn allows_slice_pushdown(&self) -> bool {
        true
    }
}

#[napi(object)]
pub struct MongoScanOptions {
    pub connection_str: String,
    pub db: String,
    pub collection: String,
    pub infer_schema_length: Option<i64>,
    pub n_rows: Option<i64>,
}

pub trait MongoLazyReader {
    /// Example
    /// scans a mongo collection into a lazyframe.
    fn scan_mongo_collection(options: MongoScanOptions) -> Result<LazyFrame> {
        let f = MongoScan::new(options.connection_str, options.db, options.collection)?;

        let args = ScanArgsAnonymous {
            name: "MONGO SCAN",
            infer_schema_length: options.infer_schema_length.map(|l| l as usize),
            n_rows: options.n_rows.map(|l| l as usize),
            ..ScanArgsAnonymous::default()
        };

        LazyFrame::anonymous_scan(Arc::new(f), args)
    }
}

impl MongoLazyReader for LazyFrame {}

#[napi]
pub fn scan_mongo_collection(options: MongoScanOptions) -> JsResult<External<LazyFrame>> {
    let f = MongoScan::new(options.connection_str, options.db, options.collection)
        .map_err(|e| napi::Error::from_reason(format!("{:#?}", e)))?;

    let args = ScanArgsAnonymous {
        name: "MONGO SCAN",
        infer_schema_length: options.infer_schema_length.map(|i| i as usize),
        ..ScanArgsAnonymous::default()
    };

    let lf = LazyFrame::anonymous_scan(Arc::new(f), args)
        .map_err(|e| napi::Error::from_reason(format!("{:#?}", e)))?;

    Ok(External::new(lf))
}
