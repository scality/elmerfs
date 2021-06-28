fn main() -> Result<(), std::io::Error> {
    let mut config = prost_build::Config::new();
    config.bytes(&[".antidote"]);
    config.compile_protos(&["src/protos/antidote.proto"], &["src/protos"])?;
    Ok(())
}
