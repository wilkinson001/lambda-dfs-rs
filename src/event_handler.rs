use aws_config::SdkConfig;
use aws_lambda_events::event::sqs::SqsEvent;
use aws_lambda_events::sqs::SqsMessage;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::Error;
use aws_sdk_s3::Client;
use lambda_runtime::{tracing, LambdaEvent};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::env;

pub(crate) async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request
    let payload = event.payload;
    tracing::info!("Payload: {:?}", payload);
    let mut event_map: HashMap<(String, String), Vec<Value>> = HashMap::new();
    let bucket_name: String = env::var("EXTENDED_DATA_BUCKET_NAME").unwrap();

    let region_provider: RegionProviderChain =
        RegionProviderChain::default_provider().or_else("us-east-1");
    let config: SdkConfig = aws_config::from_env().region(region_provider).load().await;
    let client: Client = Client::new(&config);

    for event in payload.records.iter() {
        let body: String = event.body.to_owned().unwrap();
        let parsed_event: Value = serde_json::from_str(&body[..]).unwrap();
        let key: (String, String) = get_namespace_and_table(event);
        let enriched_event: Value = maybe_pull_s3_data(parsed_event, &client, &bucket_name).await;
        event_map.entry(key).or_insert(vec![]).push(enriched_event)
    }

    for (key, value) in event_map.iter() {
        let s3_path: String = construct_s3_path(key);
        write_to_s3(value, s3_path)
    }

    Ok(())
}

fn get_namespace_and_table(message: &SqsMessage) -> (String, String) {
    let body: String = message.body.to_owned().unwrap();
    let parsed_body: Value = serde_json::from_str(&body[..]).unwrap();
    let detail_type: &str = parsed_body["detail-type"].as_str().unwrap();
    let parts: Vec<&str> = detail_type.split(".").collect();
    let mut namespace: String = String::from("dfs_");
    namespace.push_str(parts[0]);
    (namespace, String::from(parts[1]))
}

async fn maybe_pull_s3_data(event: Value, client: &Client, bucket_name: &String) -> Value {
    let s3_path: Value = event["data"]["detail"]["extended"]["s3"].to_owned();
    if s3_path != json!(null) {
        let key = s3_path.as_str().unwrap();
        let s3_data = download_object(client, bucket_name, key).await;
    }
    //Merge S3 data with event
    return event;
}

async fn download_object(
    client: &aws_sdk_s3::Client,
    bucket_name: &str,
    key: &str,
) -> GetObjectOutput {
    client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap()
}
fn construct_s3_path(table_key: &(String, String)) -> String {}

fn write_to_s3(data: &Vec<Value>, path: String) {}
// 2. Iterate events
//      a. Calculate Namespace and table
//      b. Pull S3 data
//      c. Add to namespace+table list
// 3. Iterate namespace+table
//      a. Write to Parquet S3 file
