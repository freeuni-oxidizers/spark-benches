use std::fs::File;
use std::io::{prelude::*, BufWriter};
use clap::Parser;
use rand::Rng;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
   /// Name of the person to greet
   #[clap(short, long, value_parser)]
   partitions: usize,

   /// Number of times to greet
   #[clap(short, long, value_parser)]
   lines: usize,
}


fn main() {
    let mut rng = rand::thread_rng();
    let args = Args::parse();
    for partition in 0..args.partitions {
        let file = File::create(format!("./in/{partition}")).unwrap();
        let mut writer = BufWriter::new(file);
        for _ in 0..args.lines {
            writer.write_all(format!("{}\n", rng.gen::<usize>()).as_bytes()).unwrap();
        }
    }
}
