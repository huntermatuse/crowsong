fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "proto/StoreAndForward/canary_store_and_forward_api_service.proto",
                "proto/Views/canary_views_api_service.proto",
            ],
            &["proto/"],
        )?;
    Ok(())
}
