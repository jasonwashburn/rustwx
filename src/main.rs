use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, output::GetObjectOutput, types::SdkError, error::GetObjectError};
use tokio;

#[tokio::main]
async fn main() {
    let client = get_client().await;
    match download_object(&client).await {
        Ok(object) => println!("{}", object.content_length()),
        Err(_) => println!("Error downloading object!"),
    };
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

