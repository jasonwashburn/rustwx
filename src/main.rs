use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, output::GetObjectOutput, types::SdkError, error::GetObjectError};
use tokio;
use tokio::io::{BufReader, AsyncBufReadExt};
use std::collections::HashMap;

#[derive(Debug, Default)]
struct GribIdxRecord {
    record_num: u32,
    start_byte: u32,
    stop_byte: u32,
    timestamp: u32,
    parameter: String,
    level: String,
    forecast: String,
}

impl GribIdxRecord {
    fn from_line(line: String) -> GribIdxRecord {
        let mut split = line.split(":");
        let record_num = split.next().unwrap().parse().unwrap();
        let start_byte = split.next().unwrap().parse().unwrap();
        let timestamp = split.next().unwrap().strip_prefix("d=").unwrap().parse().unwrap();
        let parameter = split.next().unwrap().to_string();
        let level = split.next().unwrap().to_string();
        let forecast = split.next().unwrap().to_string();
        GribIdxRecord {
            record_num,
            start_byte,
            timestamp,
            parameter,
            level,
            forecast,
            ..Default::default()
        }
    }
}



#[tokio::main]
async fn main() {
    let client = get_client().await;
    match download_object(&client).await {
        Ok(object) => read_body(object).await,
        Err(_) => println!("Error downloading object!"),
    }
}

async fn get_client() -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;

    Client::new(&config)
}

async fn download_object(client: &Client) -> Result<GetObjectOutput, SdkError<GetObjectError>>  {
    let resp = client
        .get_object()
        .bucket("noaa-gfs-bdp-pds")
        .key("gfs.20221215/18/atmos/gfs.t18z.pgrb2.0p25.f001.idx")
        .send()
        .await;
    resp
}

async fn read_body(object: GetObjectOutput) {
    let mut grib_index = HashMap::new();
    let mut grib_idx_records: Vec<GribIdxRecord> = Vec::new();
    let mut lines = BufReader::new(object.body.into_async_read()).lines();
    while let Some(line) = lines.next_line().await.expect("IO Error") {
        println!("{}", line);
        grib_idx_records.push(GribIdxRecord::from_line(line));
    }

    let mut stop_byte: u32;
    let last_record_index = grib_idx_records.len() - 1;
    for (index, current_record) in grib_idx_records.iter_mut().rev().enumerate() {
        let level_map = grib_index.entry(&current_record.parameter).or_insert(HashMap::new());
        level_map.entry(&current_record.level).or_insert((&current_record.start_byte, &current_record.stop_byte));

    }
    dbg!(grib_index);
}
