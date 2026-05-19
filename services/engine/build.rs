/// build.rs — compiles engine.proto and common.proto into Rust code via tonic_build.
///
/// The generated code is placed in $OUT_DIR and included with `tonic::include_proto!`
/// at the call site. See src/grpc/proto.rs.
///
/// `protoc-bin-vendored` supplies a pre-compiled protoc binary for the current
/// platform so no system-wide installation of protobuf-compiler is required on
/// Windows, macOS, or Linux.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Point prost-build at the bundled protoc so `cargo build` works without
    // any system-wide `protoc` installation.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    // Suppress rebuild unless proto files change
    println!("cargo:rerun-if-changed=../../proto/engine.proto");
    println!("cargo:rerun-if-changed=../../proto/common.proto");

    tonic_build::configure()
        // We only need the server-side stubs; no client needed inside this service.
        .build_server(true)
        .build_client(false)
        // Generate descriptor for gRPC reflection (optional — useful for grpcurl)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("engine_descriptor.bin"),
        )
        .compile_protos(
            &["../../proto/engine.proto", "../../proto/common.proto"],
            // Include path — lets protoc resolve `import "common.proto"`
            &["../../proto"],
        )?;

    Ok(())
}
