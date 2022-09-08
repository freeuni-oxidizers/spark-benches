use std::env::args;
use std::fs::File;
use std::io::{prelude::*, BufWriter};

use rand::Rng;

const NAMES: &[&[u8]] = &[b"lekva", b"shota", b"kencho", b"tamta", b"zviki", b"mushu"];

fn main() -> std::io::Result<()> {
    let mut rng = rand::thread_rng();
    let args = args().into_iter().collect::<Vec<_>>();
    let partitions: usize = args[1].parse().unwrap();
    let lines: usize = args[2].parse().unwrap();
    let tokens: usize = args[3].parse().unwrap();
    for partition in 0..partitions {
        let file = File::create(format!("./in/{partition}"))?;
        let mut writer = BufWriter::new(file);

        for _ in 0..lines {
            for _ in 0..tokens {
                writer.write_all(NAMES[rng.gen::<usize>() % NAMES.len()])?;
                writer.write_all(b" ")?;
            }
            writer.write_all(b"\n")?;
        }
    }
    Ok(())
}
