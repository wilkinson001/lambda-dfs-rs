use crate::s3::{construct_s3_path, maybe_pull_s3_data, write_to_s3};
use arrow_schema::{DataType, Field, Schema};
use aws_config::SdkConfig;
use aws_lambda_events::event::sqs::SqsEvent;
use lambda_runtime::Error;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use chrono::{DateTime, Utc};
use lambda_runtime::{tracing, LambdaEvent};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use std::sync::Arc;

pub(crate) async fn function_handler(event: LambdaEvent<SqsEvent>) -> Result<(), Error> {
    // Extract some useful information from the request
    let payload = event.payload;
    tracing::info!("Payload: {:?}", payload);
    let mut event_map: HashMap<String, HashMap<(String, String), Vec<Value>>> = HashMap::new();
    let extended_bucket_name: String = env::var("EXTENDED_DATA_BUCKET_NAME").unwrap();
    let environment: String = env::var("ENVIRONMENT").unwrap();

    let region_provider: RegionProviderChain =
        RegionProviderChain::default_provider().or_else("us-east-1");
    let config: SdkConfig = aws_config::from_env().region(region_provider).load().await;
    let client: Client = Client::new(&config);
    // Define schema: "data" as Utf8, "insert_timestamp" as Timestamp(Nanosecond, Some("UTC"))
    let schema = Arc::new(Schema::new(vec![
        Field::new("data", DataType::Utf8, false),
        Field::new(
            "insert_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]));

    for event in payload.records.iter() {
        let body: String = event.body.to_owned().unwrap();
        let enriched_event: Value = maybe_pull_s3_data(
            serde_json::from_str(&body[..]).unwrap(),
            &client,
            extended_bucket_name.to_owned(),
        )
        .await;
        event_map
            .entry(get_event_routing_region(&enriched_event))
            .or_default()
            .entry(get_namespace_and_table(&enriched_event))
            .or_default()
            .push(enriched_event)
    }

    for (region, event) in event_map.iter() {
        for (key, value) in event.iter() {
            let insert_timestamp: DateTime<Utc> = Utc::now();
            let s3_path: String = construct_s3_path(key);
            let bucket_name: String = format!("kdp-{environment}-{region}-data");
            write_to_s3(
                value.to_owned(),
                s3_path,
                &client,
                &bucket_name,
                insert_timestamp.timestamp_nanos_opt().unwrap(),
                &schema,
            )
            .await
        }
    }

    Ok(())
}

fn get_namespace_and_table(event: &Value) -> (String, String) {
    let detail_type: &str = event["detail-type"].as_str().unwrap();
    let parts: Vec<&str> = detail_type.split(".").collect();
    let mut namespace: String = String::from("dfs_");
    namespace.push_str(parts[0]);
    (namespace, String::from(parts[1]))
}

fn get_event_routing_region(event: &Value) -> String {
    let kb4_principal: &str = event["metadata"]["kb4_principal"].as_str().unwrap();
    let parts: Vec<&str> = kb4_principal.split(":").collect();
    String::from(parts[2])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_namespace_and_table() {
        let test_event: Value = serde_json::from_str(
            String::from(r#"{"detail-type": "test_namespace.test_table.foo_bar"}"#).as_str(),
        )
        .unwrap();
        assert_eq!(
            get_namespace_and_table(&test_event),
            (
                String::from("dfs_test_namespace"),
                String::from("test_table")
            )
        )
    }

    #[test]
    fn test_get_event_routing_region() {
        let test_event: Value = serde_json::from_str(
            String::from(
                r#"{"metadata": {"kb4_principal": "krn:resource:us-east-1:identity/foo/bar"}}"#,
            )
            .as_str(),
        )
        .unwrap();
        assert_eq!(
            get_event_routing_region(&test_event),
            String::from("us-east-1")
        )
    }
}
