use std::fs::DirEntry;
use std::io::Result;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::{fs, io};

fn main() -> Result<()> {
    build_protos()?;

    Ok(())
}

fn build_protos() -> Result<()> {
    const PROTO_PATH: &str = "./proto";
    const PROTO_MAIN_PATH: &str = "./proto/confluent";
    const PROTO_TEST_PATH: &str = "./proto/test";

    let path = PathBuf::from("src/codegen/file_descriptor_set.bin");
    fs::create_dir_all(path.parent().unwrap())?;

    let prost_config = {
        let mut config = prost_build::Config::new();
        config
            .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
            .out_dir(path.parent().unwrap());
        config
    };

    let proto_files: Vec<PathBuf> = {
        let proto_files = Arc::new(Mutex::new(Vec::new()));
        visit_dirs(Path::new(PROTO_MAIN_PATH), &|entry| {
            if entry.path().extension().is_some_and(|ext| ext == "proto") {
                proto_files.lock().unwrap().push(entry.path());
            }
        })?;
        let proto_files = proto_files.lock().unwrap().iter().cloned().collect();
        proto_files
    };

    prost_reflect_build::Builder::new()
        .descriptor_pool("crate::DESCRIPTOR_POOL")
        .file_descriptor_set_path(path)
        .compile_protos_with_config(prost_config, &proto_files, &[PathBuf::from(PROTO_PATH)])?;

    let path = PathBuf::from("src/codegen/test/file_descriptor_set.bin");
    fs::create_dir_all(path.parent().unwrap())?;

    let mut prost_test_config = {
        let mut config = prost_build::Config::new();
        config
            .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
            .out_dir(path.parent().unwrap());
        config
    };

    let proto_test_files: Vec<PathBuf> = {
        let proto_files = Arc::new(Mutex::new(Vec::new()));
        visit_dirs(Path::new(PROTO_TEST_PATH), &|entry| {
            if entry.path().extension().is_some_and(|ext| ext == "proto") {
                proto_files.lock().unwrap().push(entry.path());
            }
        })?;
        let proto_files = proto_files.lock().unwrap().iter().cloned().collect();
        proto_files
    };

    prost_reflect_build::Builder::new()
        .descriptor_pool("crate::TEST_DESCRIPTOR_POOL")
        .file_descriptor_set_path(path)
        .compile_protos_with_config(
            prost_test_config,
            &proto_test_files,
            &[PathBuf::from(PROTO_PATH)],
        )?;

    Ok(())
}

fn visit_dirs(dir: &Path, cb: &dyn Fn(&DirEntry)) -> io::Result<()> {
    if dir.is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                visit_dirs(&path, cb)?;
            } else {
                cb(&entry);
            }
        }
    }
    Ok(())
}
