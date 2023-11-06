use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        .pure()
        .cargo_out_dir("generated_protos")
        .include("src/protos")
        .input("src/protos/CompressedVideo.proto")
        .run_from_script();
}
