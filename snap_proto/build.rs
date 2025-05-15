fn main() {
    tonic_build::configure()
        .type_attribute("manager.GwConfig", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(&["proto/manager.proto"], &["proto"])
        .unwrap();
}


