use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{error::GetObjectError, output::GetObjectOutput, types::SdkError, Client};
use chrono::{DateTime, Datelike, Timelike};
use std::collections::HashMap;
use tokio;
use tokio::io::{AsyncBufReadExt, BufReader};

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
        let timestamp = split
            .next()
            .unwrap()
            .strip_prefix("d=")
            .unwrap()
            .parse()
            .unwrap();
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
    let s3_client = get_s3_client().await;
    match get_idx_object(&s3_client).await {
        Ok(object) => read_idx_object(object).await,
        Err(_) => println!("Error downloading object!"),
    }
}

async fn get_s3_client() -> Client {
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;

    Client::new(&config)
}

async fn get_idx_object(s3_client: &Client) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
    let model_run = DateTime::parse_from_rfc3339("2022-12-09T18:00:00-00:00")
        .expect("Unable to parse datetime");
    dbg!(model_run);
    let year = model_run.year();
    let month = model_run.month();
    let day = model_run.day();
    let hour = model_run.time().hour();
    let forecast = 1;
    dbg!(hour);
    let key =
        format!("gfs.{year}{month:02}{day:02}/{hour:02}/atmos/gfs.t{hour:02}z.pgrb2.0p25.f{forecast:03}.idx");
    println!("{:?}", key);
    let resp = s3_client
        .get_object()
        .bucket("noaa-gfs-bdp-pds")
        .key(key.to_owned())
        .send()
        .await;
    resp
}

async fn read_idx_object(object: GetObjectOutput) {
    let mut grib_index = HashMap::new();
    let mut grib_idx_records: Vec<GribIdxRecord> = Vec::new();
    let mut lines = BufReader::new(object.body.into_async_read()).lines();
    while let Some(line) = lines.next_line().await.expect("IO Error") {
        grib_idx_records.push(GribIdxRecord::from_line(line));
    }

    let mut prev_start_byte: u32 = 0;
    for current_record in grib_idx_records.iter_mut().rev() {
        current_record.stop_byte = prev_start_byte;
        let level_map = grib_index
            .entry(&current_record.parameter)
            .or_insert(HashMap::new());

        level_map
            .entry(&current_record.level)
            .or_insert((&current_record.start_byte, &current_record.stop_byte));
        prev_start_byte = current_record.start_byte;
    }
}
