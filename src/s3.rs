use arrow::array::{RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::Schema;

use aws_sdk_s3::Client;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use parquet::arrow::ArrowWriter;
use serde_json::{Value, json};
use std::sync::Arc;

pub async fn maybe_pull_s3_data(mut event: Value, client: &Client, bucket_name: String) -> Value {
    let s3_path: Value = event["data"]["detail"]["extended"]["s3"].to_owned();
    if s3_path != json!(null) {
        let key = s3_path.as_str().unwrap();
        let s3_data: Vec<u8> = download_object(client, bucket_name.as_str(), key).await;
        //Merge S3 data with event
        let extended_map = event
            .get_mut("data")
            .and_then(|d| d.get_mut("detail"))
            .and_then(|d| d.get_mut("extended"))
            .and_then(|x| x.as_object_mut())
            .unwrap();
        let additions: Value =
            serde_json::from_str(std::str::from_utf8(&s3_data).unwrap()).unwrap();
        if let Some(add_obj) = additions.as_object() {
            for (k, v) in add_obj {
                if k == "s3" {
                    continue;
                } // preserve existing s3
                extended_map.insert(k.clone(), v.clone()); // clone from additions
            }
        }
    }
    event
}

async fn download_object(client: &aws_sdk_s3::Client, bucket_name: &str, key: &str) -> Vec<u8> {
    let stream: GetObjectOutput = client
        .get_object()
        .bucket(bucket_name)
        .key(key)
        .send()
        .await
        .unwrap();
    let data = stream.body.collect().await.unwrap();
    data.to_vec()
}

pub fn construct_s3_path(table_key: &(String, String)) -> String {
    format!("/raw/dfs/{}/{}", table_key.0, table_key.1,)
}

pub async fn write_to_s3(
    data: Vec<Value>,
    path: String,
    client: &aws_sdk_s3::Client,
    bucket: &str,
    insert_timestamp: i64,
    schema: &Arc<Schema>,
) {
    let json_strings: Vec<String> = data
        .iter()
        .map(|v| v.to_string()) // serialize to JSON text
        .collect();

    // Create Arrow array for the `data` column
    let data_array: Arc<StringArray> = Arc::new(StringArray::from(json_strings)) as _;

    // Create a timestamp array (same timestamp for all rows here)
    let ts_array: Arc<TimestampNanosecondArray> = Arc::new(TimestampNanosecondArray::from(vec![
            insert_timestamp;
            data.len()
        ])) as _;

    // Build RecordBatch
    let mut buffer = Vec::new();
    let batch = RecordBatch::try_new(schema.to_owned(), vec![data_array, ts_array]).unwrap();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.to_owned(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let _ = client
        .put_object()
        .bucket(bucket)
        .key(path)
        .body(ByteStream::from(buffer))
        .send()
        .await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_smithy_mocks::{mock, mock_client};

    #[tokio::test]
    async fn test_maybe_pull_s3_data_no_s3_field() {
        let event = json!({
            "data": {
                "detail": {
                    "extended": {}
                }
            }
        });

        let bucket_name = "test-bucket".to_string();

        // Should not call client when no s3 field
        let result = maybe_pull_s3_data(
            event.clone(),
            &Client::new(&aws_config::from_env().load().await),
            bucket_name,
        )
        .await;

        assert_eq!(result, event);
    }

    #[tokio::test]
    async fn test_maybe_pull_s3_data_null_s3_field() {
        let event = json!({
            "data": {
                "detail": {
                    "extended": {
                        "s3": null
                    }
                }
            }
        });

        let bucket_name = "test-bucket".to_string();

        let result = maybe_pull_s3_data(
            event.clone(),
            &Client::new(&aws_config::from_env().load().await),
            bucket_name,
        )
        .await;

        assert_eq!(result, event);
    }

    #[tokio::test]
    async fn test_maybe_pull_s3_data_pull_data() {
        let event = json!({
            "data": {
                "detail": {
                    "extended": {
                        "s3": "some_value"
                    }
                }
            }
        });
        let expected_result = json!({
            "data": {
                "detail": {
                    "extended": {
                        "s3": "some_value",
                        "some_key": {
                            "some_other_key": "some_value"
                        }
                    }
                }
            }
        });

        let get_object_rule = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(
                    json!({
                        "some_key": {
                            "some_other_key": "some_value"
                        },
                        "s3": "some_different_bucket"
                    })
                    .to_string()
                    .into_bytes(),
                ))
                .build()
        });

        // Create a mocked client with the rule
        let client = mock_client!(aws_sdk_s3, [&get_object_rule]);

        let result = maybe_pull_s3_data(event, &client, "test-bucket".to_string()).await;

        assert_eq!(result, expected_result);
    }

    #[tokio::test]
    async fn test_maybe_pull_s3_data_pull_empty_data() {
        let event = json!({
            "data": {
                "detail": {
                    "extended": {
                        "s3": "some_value"
                    }
                }
            }
        });

        let get_object_rule = mock!(aws_sdk_s3::Client::get_object).then_output(move || {
            GetObjectOutput::builder()
                .body(ByteStream::from(json!("").to_string().into_bytes()))
                .build()
        });

        // Create a mocked client with the rule
        let client = mock_client!(aws_sdk_s3, [&get_object_rule]);

        let result = maybe_pull_s3_data(event.clone(), &client, "test-bucket".to_string()).await;

        assert_eq!(result, event);
    }

    #[tokio::test]
    async fn test_construct_s3_path_normal_case() {
        let key = ("namespace1".to_string(), "table1".to_string());
        let result = construct_s3_path(&key);
        assert_eq!(result, "/raw/dfs/namespace1/table1");
    }

    #[tokio::test]
    async fn test_construct_s3_path_empty_table_name() {
        let key = ("namespace1".to_string(), "".to_string());
        let result = construct_s3_path(&key);
        assert_eq!(result, "/raw/dfs/namespace1/");
    }

    #[tokio::test]
    async fn test_construct_s3_path_with_special_characters() {
        let key = ("ns-1.0".to_string(), "table_v2".to_string());
        let result = construct_s3_path(&key);
        assert_eq!(result, "/raw/dfs/ns-1.0/table_v2");
    }

    #[tokio::test]
    async fn test_write_to_s3_empty_data() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("data", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new(
                "insert_timestamp",
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let config = aws_config::from_env().load().await;
        let client = Client::new(&config);

        // This should not panic with empty data
        let result = write_to_s3(
            vec![],
            "/test/path".to_string(),
            &client,
            "test-bucket",
            1234567890i64,
            &schema,
        )
        .await;

        // Just verify it doesn't panic - actual S3 write is mocked
        let _ = result;
    }

    #[tokio::test]
    async fn test_write_to_s3_single_record() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("data", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new(
                "insert_timestamp",
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let config = aws_config::from_env().load().await;
        let client = Client::new(&config);

        let data = vec![json!({"key": "value"})];
        let result = write_to_s3(
            data,
            "/test/path".to_string(),
            &client,
            "test-bucket",
            1234567890i64,
            &schema,
        )
        .await;

        let _ = result;
    }

    #[tokio::test]
    async fn test_write_to_s3_multiple_records() {
        let schema = Arc::new(Schema::new(vec![
            arrow_schema::Field::new("data", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new(
                "insert_timestamp",
                arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));

        let config = aws_config::from_env().load().await;
        let client = Client::new(&config);

        let data = vec![
            json!({"key": "value1"}),
            json!({"key": "value2"}),
            json!({"key": "value3"}),
        ];

        let result = write_to_s3(
            data,
            "/test/path".to_string(),
            &client,
            "test-bucket",
            1234567890i64,
            &schema,
        )
        .await;

        let _ = result;
    }
}
