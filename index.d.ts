/**
 * options required to read from a mongo collection
 * @param connectionStr: connection string with the following format:  `mongodb://user:password@url/database`
 * @param db database name to read from
 * @param collection name of the collection in the mongo database
 * @param inferSchemaLength number of rows to read to detect schema
 * - A higher number will greatly reduce performance, but may get a more accurate schema. 
 * Defaults to `100`
 * @param nRows: number of rows to read from mongodb collection. same as calling `.limit(nRows)`
 * @param batchSize: number of records to retrieve in a single call to the mongodb database  
 */
export interface MongoScanOptions {
  connectionStr: string
  db: string
  collection: string
  inferSchemaLength?: number
  nRows?: number
  batchSize?: number
}

export function scanMongo(options: MongoScanOptions): import('nodejs-polars').LazyDataFrame

declare module 'nodejs-polars' {
  export function scanMongo(options: MongoScanOptions): import('nodejs-polars').LazyDataFrame
}
