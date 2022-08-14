use arrow::csv;
use clap::Parser;
use std::error::Error;
use std::fmt::Display;
use std::fs::File;
use std::path::Path;
use std::process;
use tabled::{Header, Style, Table, Tabled};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    path: String,
    #[clap(short, long)]
    query: Option<String>,
}

#[derive(Tabled, Debug)]
struct ColumnInfo {
    column_name: String,
    infered_type: String,
    null_count: usize,
}
impl Display for ColumnInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "{}<{}>", self.column_name, self.infered_type)
    }
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    if !Path::new(&args.path).exists() {
        println!("不正なパスです。");
        process::exit(1);
    }
    let file = File::open(&args.path)?;
    let builder = csv::ReaderBuilder::new()
        .infer_schema(Some(500))
        .has_header(true)
        .with_delimiter(b',');
    let mut reader = builder.build(file)?;
    let batch = reader.next().unwrap()?;

    let mut cols: Vec<ColumnInfo> = Vec::new();
    for (arr, field) in batch.columns().iter().zip(batch.schema().fields()) {
        let e = ColumnInfo {
            column_name: field.name().to_string(),
            infered_type: field.data_type().to_string(),
            null_count: arr.null_count(),
        };
        cols.push(e);
    }
    let column_info_table = Table::new(&cols)
        .with(Style::rounded())
        .with(Header(format!(
            "File Path: {path}\nRecord Count: {row_count}",
            path = args.path,
            row_count = batch.num_rows()
        )));
    println!("{}", column_info_table);
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    run()?;
    Ok(())
}
