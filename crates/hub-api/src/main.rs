#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let cfg = hub_api::config::Config::from_env();
    hub_api::run(&cfg).await
}
