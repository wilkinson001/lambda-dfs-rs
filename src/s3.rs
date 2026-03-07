use arrow::array::{RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_schema::Schema;

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use parquet::arrow::ArrowWriter;
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn maybe_pull_s3_data(mut event: Value, client: &Client, bucket_name: String) -> Value {
    let s3_path: Value = event["data"]["detail"]["extended"]["s3"].to_owned();
    if s3_path != json!(null) {
        let key = s3_path.as_str().unwrap();
        let s3_data: Vec<u8> = download_object(client, bucket_name.as_str(), key).await;
        //Merge S3 data with event
        if let Some(extended_map) = event
            .get_mut("data")
            .and_then(|d| d.get_mut("detail"))
            .and_then(|d| d.get_mut("extended"))
            .and_then(|x| x.as_object_mut())
        {
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
