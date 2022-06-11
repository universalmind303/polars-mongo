#![deny(clippy::all)]

mod bson;
mod conversion;
pub mod prelude;

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

pub struct MongoScan {
    pub collection: Collection<Document>,
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

    pub fn connect(connection_str: &str, db: &str, collection: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(connection_str).unwrap();

        let client = Client::with_options(client_options).unwrap();
        let database = client.database(db);
        let collection = database.collection::<Document>(collection);
        Ok(MongoScan {
            collection,
            n_threads: None,
            rechunk: false,
            batch_size: None,
        })
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
            .unwrap_or_else(|| self.collection.estimated_document_count(None).unwrap() as usize);

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

                    let cursor = self.collection.find(None, Some(find_options));
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
        let infer_options = FindOptions::builder()
            .limit(infer_schema_length.map(|i| i as i64))
            .build();

        let res = self
            .collection
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

        let schema = infer_schema(iter, infer_schema_length.unwrap_or(1000));
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

pub struct MongoScanOptions<'a> {
    pub connection_str: &'a str,
    pub db: &'a str,
    pub collection: &'a str,
    pub infer_schema_length: Option<usize>,
    pub n_rows: Option<usize>,
}

pub trait MongoLazyReader {
    /// Example
    /// scans a mongo collection into a lazyframe.
    /// Will pushdown slice & predicate operations to mongo.
    /// ## Example
    /// ```rust
    /// let df = LazyFrame::scan_mongo_collection(MongoScanOptions {
    ///     connection_str
    ///     db,
    ///     collection,
    ///     infer_schema_length: None,
    ///     n_rows: None,
    /// })?
    /// .collect()?;
    ///
    /// println!("{df}");
    /// ```
    fn scan_mongo_collection(options: MongoScanOptions) -> Result<LazyFrame> {
        let f = MongoScan::connect(&options.connection_str, &options.db, &options.collection)?;

        let args = ScanArgsAnonymous {
            name: "MONGO SCAN",
            infer_schema_length: options.infer_schema_length,
            ..ScanArgsAnonymous::default()
        };

        LazyFrame::anonymous_scan(Arc::new(f), args)
    }
}

impl MongoLazyReader for LazyFrame {}

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
    println!("{df}");
    todo!()
}
