use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, output::GetObjectOutput, types::SdkError, error::GetObjectError};
use tokio;
use tokio::io::{BufReader, AsyncBufReadExt};

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

    fn set_stop_byte(mut self, stop_byte: u32) -> Self {
        self.stop_byte = stop_byte;
        return self;
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
    let mut grib_idx_records: Vec<GribIdxRecord> = Vec::new();
    let mut lines = BufReader::new(object.body.into_async_read()).lines();
    while let Some(line) = lines.next_line().await.expect("IO Error") {
        println!("{}", line);
        grib_idx_records.push(GribIdxRecord::from_line(line));
    }

    let mut stop_byte: u32;
    for index in 0..grib_idx_records.len() {
        if index == grib_idx_records.len() - 1{
            stop_byte = 0;
        } else {
            stop_byte = grib_idx_records.get(index + 1).unwrap().start_byte;
        }

        if index < grib_idx_records.len() - 1 {
            grib_idx_records.get_mut(index).unwrap().stop_byte = stop_byte;
        }
        dbg!(grib_idx_records.get(index).unwrap());
    }
}
