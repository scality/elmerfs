use protobuf_codegen_pure as protobuf;

fn main() {
    protobuf::Codegen::new()
        .out_dir("src/protos")
        .inputs(&["3rdparty/antidotepb/proto/antidote.proto"])
        .include("3rdparty/antidotepb/proto")
        .run()
        .expect("protobuf codegen failed");
}