use crowsong::ViewsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv()?;

    let endpoint = std::env::var("ENDPOINT")?;
    let api_key = std::env::var("API_KEY")?;
    let user_id = std::env::var("USER_ID")?;

    println!("Connecting to Views service at {endpoint}...");

    let mut client = ViewsClient::connect(
        &endpoint,
        &api_key,
        "crowsong-test",
        &user_id,
    )
    .await?;

    println!("Connected! CCI = {}", client.cci());

    println!("Testing gRPC connection...");
    client.test().await?;
    println!("Test passed.");

    println!("Getting service version...");
    let version = client.get_version().await?;
    println!("Service version: {:?}", version.version);

    println!("Getting views...");
    let views = client.get_views().await?;
    println!("Views: {:?}", views.views);

    let view = match views.views.first() {
        Some(view) => view.clone(),
        None => {
            println!("No views found; skipping tag list.");
            println!("Disconnecting...");
            client.disconnect().await?;
            println!("Done.");
            return Ok(());
        }
    };

    println!("Getting datasets for view {view}...");
    let datasets = client.get_dataset_list(view.clone(), false).await?;
    println!("Datasets: {:?}", datasets.datasets);

    let dataset_name = match datasets.datasets.first() {
        Some(name) => name.clone(),
        None => {
            println!("No datasets found in view {view}; skipping tag list.");
            println!("Disconnecting...");
            client.disconnect().await?;
            println!("Done.");
            return Ok(());
        }
    };

    println!("Getting tags for {view} / {dataset_name}...");
    let tags = client
        .get_tag_list(view, dataset_name, 0, 100)
        .await?;
    println!("Tags: {:?}", tags.tag_names);

    println!("Disconnecting...");
    client.disconnect().await?;
    println!("Done.");

    Ok(())
}
