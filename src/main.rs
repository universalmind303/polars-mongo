#![deny(clippy::all)]

mod bson;
mod conversion;
use bson::buffer::*;

use conversion::Wrap;
use polars::{frame::row::*, prelude::*};

use mongodb::{
    bson::{doc, Bson, Document},
    options::{ClientOptions, FindOptions},
    sync::{Client, Collection, Cursor},
};

struct MongoScan {
    pub n_rows: Option<usize>,
    pub collection: Collection<Document>,
}

impl MongoScan {
    pub fn connect(connection_str: &str, db: &str, collection: &str) -> Result<Self> {
        let client_options = ClientOptions::parse(connection_str).unwrap();

        let client = Client::with_options(client_options).unwrap();
        let database = client.database(db);
        let collection = database.collection::<Document>(collection);
        Ok(MongoScan { collection, n_rows: None })
    }


    fn parse_lines<'a>(
        &self,
        mut cursor: Cursor<Document>,
        buffers: &mut PlHashMap<String, Buffer<'a>>,
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
        
        let mut find_options = FindOptions::builder().limit(self.n_rows.map(|n| n as i64)).build();
        find_options.projection = projection;
        
        println!("scan options = {:#?}", scan_opts);
        let schema = scan_opts.output_schema.unwrap_or(scan_opts.schema);
        let n_rows = self.n_rows.unwrap_or(10000);



        let mut buffers = init_buffers(schema.as_ref(), n_rows).unwrap();

        let cursor = self.collection.find(None, Some(find_options));

        self.parse_lines(cursor.unwrap(), &mut buffers).unwrap();

        DataFrame::new(
            buffers
                .into_values()
                .map(|buf| buf.into_series())
                .collect::<Result<_>>()?,
        )

    }
    fn schema(&self, infer_schema_length: Option<usize>) -> Result<Schema> {
        let infer_options = FindOptions::builder()
            .limit(infer_schema_length.map(|i| i as i64))
            .build();

        let res = self.collection.find(None, Some(infer_options)).unwrap();

        let inferable: Vec<Vec<(_, _)>> = res
            .map(|doc| {
                let val = doc.unwrap();

                let v = val.into_iter().map(|(key, value)| {
                    let dtype: Wrap<DataType> = (&value).into();
                    (key, dtype.0)
                });
                v.collect()
            })
            .collect();

        let schema = infer_schema(inferable.into_iter(), 100);
        println!("{:#?}", schema);
        Ok(schema)
    }
}

fn main() {
    let connection_str = std::env::var("CONNECTION_URI").unwrap();
    let f = MongoScan::connect(
        &connection_str,
        "test",
        "txns",
    )
    .unwrap();
    let args = ScanArgsAnonymous {
        name: "MONGO SCAN",
        infer_schema_length: Some(1000),
        n_rows: Some(1000),
        ..ScanArgsAnonymous::default()
    };

    let df = LazyFrame::anonymous_scan(Arc::new(f), args)
        .unwrap()
        .select([col("block_hash"), col("gas_price").max()])
        .collect()
        .unwrap();

    println!("{df}");
}
