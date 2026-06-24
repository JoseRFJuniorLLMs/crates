use heraclitus_core::HeraclitusConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let config_path = std::env::args().nth(1).map(std::path::PathBuf::from);
    let config = HeraclitusConfig::load(config_path.as_deref())?;
    heraclitus_server::serve(config, async {
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    Ok(())
}
