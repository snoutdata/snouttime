//! The pure decoders, compiled in by path so the fuzz targets link no Postgres.
#![allow(dead_code)]

#[path = "../../src/codec/mod.rs"]
pub mod codec;

#[path = "../../src/columnar/format.rs"]
pub mod format;
