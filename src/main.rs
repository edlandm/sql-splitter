/*
 * sql-splitter - split a blob of SSMS-generated SQL objects into separate files
 * usage: sql-splitter [-n] [-d <output-dir>] <file>
 * Currently only supports stored-procedures, but the goal is to support all
 * types of database objects
 */
#![feature(buf_read_has_data_left)]

extern crate encoding_rs;
extern crate encoding_rs_io;

use clap::Parser;
use regex::Regex;
use std::fs::{ File, create_dir_all };
use std::io::{ BufRead, BufReader, BufWriter, Write };
use std::path::Path;
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[arg(short = 'd', long = "out-dir", required = false, default_value_t = String::from("."), help = "Output directory to create files")]
    out_dir: String,
    #[arg(short = 'n', long = "only_names", required = false, default_value_t = false, help = "Exclude schema-name from filenames")]
    only_object_names: bool,
    #[arg(short = 'v', long = "verbose", required = false, default_value_t = false, help = "Verbose output")]
    verbose: bool,
    #[arg(short = 'w', long = "windows-1252", required = false, default_value_t = false, help = "specify that input files are using windows-1252 encoding instead of UTF-8")]
    windows_1252: bool,
    // remaining arguments are file-paths
    #[arg(required = false, help = "File(s) to process")]
    in_file: Option<String>,
}

#[derive(Debug)]
enum ObjectType {
    Database,
    DatabaseRole,
    DdlTrigger,
    Index,
    Schema,
    Sequence,
    StoredProcedure,
    Synonym,
    Table,
    Trigger,
    User,
    UserDefinedDataType,
    UserDefinedFunction,
    View,
}

impl std::fmt::Display for ObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectType::Database            => write!(f, "Database"),
            ObjectType::DatabaseRole        => write!(f, "DatabaseRole"),
            ObjectType::DdlTrigger          => write!(f, "DdlTrigger"),
            ObjectType::Index               => write!(f, "Index"),
            ObjectType::Schema              => write!(f, "Schema"),
            ObjectType::Sequence            => write!(f, "Sequence"),
            ObjectType::StoredProcedure     => write!(f, "StoredProcedure"),
            ObjectType::Synonym             => write!(f, "Synonym"),
            ObjectType::Table               => write!(f, "Table"),
            ObjectType::Trigger             => write!(f, "Trigger"),
            ObjectType::User                => write!(f, "User"),
            ObjectType::UserDefinedDataType => write!(f, "UserDefinedDataType"),
            ObjectType::UserDefinedFunction => write!(f, "UserDefinedFunction"),
            ObjectType::View                => write!(f, "View"),
        }
    }
}

struct DatabaseObject {
    object_type: ObjectType,
    schema:      String,
    name:        String,
}

impl TryFrom<&str> for DatabaseObject {
    type Error = ();
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let pattern = Regex::new(r"^/\*+\s+Object:\s+(\w+)\s+\[(\S+)\]\.\[(\S+)\]").unwrap();
        if let Some(caps) = pattern.captures(s) {
            let object_type = match caps.get(1).unwrap().as_str() {
                "Database"            => Some(ObjectType::Database),
                "DatabaseRole"        => Some(ObjectType::DatabaseRole),
                "DdlTrigger"          => Some(ObjectType::DdlTrigger),
                "Index"               => Some(ObjectType::Index),
                "Schema"              => Some(ObjectType::Schema),
                "Sequence"            => Some(ObjectType::Sequence),
                "StoredProcedure"     => Some(ObjectType::StoredProcedure),
                "Synonym"             => Some(ObjectType::Synonym),
                "Table"               => Some(ObjectType::Table),
                "Trigger"             => Some(ObjectType::Trigger),
                "User"                => Some(ObjectType::User),
                "UserDefinedDataType" => Some(ObjectType::UserDefinedDataType),
                "UserDefinedFunction" => Some(ObjectType::UserDefinedFunction),
                "View"                => Some(ObjectType::View),
                _                     => None,
            };
            if let None = object_type {
                return Err(());
            }
            return Ok(DatabaseObject {
                object_type: object_type.unwrap(),
                schema:      caps.get(2).unwrap().as_str().to_string(),
                name:        caps.get(3).unwrap().as_str().to_string(),
            });
        }
        Err(())
    }
}

fn main() {
    let cli = Cli::parse();

    let mut out_dir: String  = cli.out_dir.to_owned();
    if out_dir.len() > 0 {
        // if out_dir was given and ends in a slash, remove the slash
        match out_dir.chars().last().expect("out_dir was empty") {
            '/'  => { out_dir.truncate(out_dir.len() - 1) },
            '\\' => { out_dir.truncate(out_dir.len() - 1) },
            _    => (),
        };
    }
    let only_object_names = &cli.only_object_names;
    let windows_1252      = &cli.windows_1252;
    let verbose           = &cli.verbose;

    let mut reader: Box<dyn BufRead> = if let Some(in_file) = cli.in_file {
        // check if file exists
        if !Path::new(&in_file).exists() {
            eprintln!("File does not exist: {}", in_file);
            std::process::exit(1);
        }
        let file = File::open(in_file).unwrap();
        if *windows_1252 {
            Box::new(BufReader::new(DecodeReaderBytesBuilder::new()
                .encoding(Some(WINDOWS_1252))
                .build(file)))
        } else {
            Box::new(BufReader::new(file))
        }
    } else {
        let stdin = std::io::stdin();
        let handle = stdin.lock();
        if *windows_1252 {
            Box::new(BufReader::new(DecodeReaderBytesBuilder::new()
                .encoding(Some(WINDOWS_1252))
                .build(handle)))
        } else {
            Box::new(BufReader::new(handle))
        }
    };

    // ensure that out_dir exists
    create_dir_all(out_dir.to_owned()).unwrap();

    let mut line = String::new();
    let mut db_use_statement = String::new();
    let mut writer: Option<BufWriter<File>> = None;

    let make_path = |dir: String, obj: DatabaseObject| -> String {
        if *only_object_names || obj.schema.is_empty() {
            format!("{}/{}.sql", dir, obj.name)
        } else {
            format!("{}/{}.{}.sql", dir, obj.schema, obj.name)
        }
    };

    loop {
        // ensure file is (still) readable
        match reader.has_data_left() {
            Ok(false) => {
                return;
            },
            Err(e) => {
                eprintln!("{:?}", e);
                std::process::exit(1);
            },
            _ => {}
        }

        // read a line
        if let Err(e) = reader.read_line(&mut line) {
            eprintln!("{:?}", e);
            std::process::exit(1);
        }

        // keep track of which database the following objects belong to
        if line.starts_with("USE ") {
            // get line containing USE, and the following line with 'GO'
            db_use_statement.clear();
            reader.read_line(&mut line).unwrap();
            db_use_statement.push_str(line.as_str());
        } else if line.starts_with("/****** Object:") {
            if let Ok(obj) = DatabaseObject::try_from(line.as_str()) {
                let dir = [
                    out_dir.as_str(),
                    obj.object_type.to_string().as_str(),
                    ].join("/");
                create_dir_all(dir.to_owned()).unwrap();

                if let Some(w) = writer.as_mut() {
                    w.flush().unwrap();
                }

                let path = make_path(dir.to_owned(), obj);
                if *verbose {
                    println!("creating {:?}", path);
                }

                let file = File::create(path).unwrap();
                let mut _writer = BufWriter::new(file);
                _writer.write(db_use_statement.as_bytes()).unwrap();
                _writer.write(line.as_bytes()).unwrap();
                writer = Some(_writer);
            }
        } else {
            if let Some(w) = writer.as_mut() {
                w.write(line.as_bytes()).unwrap();
            }
        }
        line.clear();
    }
}
