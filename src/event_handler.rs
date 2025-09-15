use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::sqs::SqsMessage;

use iceberg::spec::DataFileFormat;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::{FileWriterBuilder, ParquetWriter, ParquetWriterBuilder};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use lambda_runtime::{tracing, Error, LambdaEvent};
use serde_json::Value;
use std::collections::HashMap;
use std::env;

pub(crate) async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request
    let payload = event.payload;
    tracing::info!("Payload: {:?}", payload);
    let catalog: RestCatalog = load_catalog();
    let mut event_map: HashMap<(String, String), Vec<&SqsMessage>> = HashMap::new();

    for event in payload.records.iter() {
        let key = get_namespace_and_table(event);
        event_map.entry(key).or_insert(vec![]).push(event)
    }

    for (key, value) in event_map.into_iter() {
        //Create namespace if not exists
        let namespace = NamespaceIdent::new(key.0);
        if !catalog.namespace_exists(&namespace).await? {
            _ = catalog.create_namespace(&namespace, HashMap::new());
        }
        // Load table
        let table: Table = catalog
            .load_table(&TableIdent::new(namespace, key.1))
            .await?;
        // Append to table
    }

    Ok(())
}

fn load_catalog() -> RestCatalog {
    // Create the REST iceberg catalog.
    let warehouse = env::var("WAREHOUSE").unwrap();
    let uri = env::var("URI").unwrap();
    let catalog_config = RestCatalogConfig::builder()
        .warehouse(warehouse)
        .uri(uri)
        .props(HashMap::new())
        .build();
    RestCatalog::new(catalog_config)
}

fn get_namespace_and_table(message: &SqsMessage) -> (String, String) {
    let body: String = message.body.to_owned().unwrap();
    let parsed_body: Value = serde_json::from_str(&body[..]).unwrap();
    let detail_type: &str = parsed_body["detail-type"].as_str().unwrap();
    let parts: Vec<&str> = detail_type.split(".").collect();
    let mut namespace = String::from("dfs_");
    namespace.push_str(parts[0]);
    (namespace, String::from(parts[1]))
}

async fn write_data(event: SqsMessage, table: Table, catalog: RestCatalog) {
    // Create a transaction.
    let tx = Transaction::new(&table);

    // Create a `FastAppendAction` which will not rewrite or append
    // to existing metadata. This will create a new manifest.
    let action = tx.fast_append().add_data_files(my_data_files);

    // Apply the fast-append action to the given transaction, returning
    // the newly updated `Transaction`.
    let tx = action.apply(tx).unwrap();

    // End the transaction by committing to an `iceberg::Catalog`
    // implementation. This will cause a table update to occur.
    let table = tx.commit(&catalog).await.unwrap();
}

fn get_writer(table: Table) -> ParquetWriter {
    ParquetWriterBuilder::new(
        props,
        schema,
        file_io,
        DefaultLocationGenerator::new(table.metadata().to_owned()).unwrap(),
        DefaultFileNameGenerator::new(String::from(""), None, DataFileFormat::Parquet),
    )
}
// 1. Load Catalog
// 2. Iterate events
//      a. Calculate Namespace and table
//      b. Pull S3 data
//      c. Add to namespace+table list
// 3. Iterate namespace+table
//      a. Append data to table
