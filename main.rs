#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = hub_api::config::Config::from_env();
    let app = hub_api::build_app(&cfg).await?;

    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr).await?;
    tracing::info!(addr = %cfg.bind_addr, "hub-api listening");
    axum::serve(listener, app).await?;

    Ok(())
}
