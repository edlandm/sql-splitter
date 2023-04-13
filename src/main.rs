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
use std::path::{ Path, PathBuf };
use encoding_rs::WINDOWS_1252;
use encoding_rs_io::DecodeReaderBytesBuilder;
use zip::ZipWriter;

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
    #[arg(short = 'z', long = "zip", required = false, help = "path to zip file to create and place results")]
    zip: Option<String>,
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
        let pattern = Regex::new(r"^/\*+\s+Object:\s+(\w+)\s+\[(\S+)\]\.\[(\S+)\]")
            .expect("error compiling DatabaseObject regular expression");
        if let Some(caps) = pattern.captures(s) {
            let cap = caps.get(1).expect("Error retrieving capture group");
            let object_type = match cap.as_str() {
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

    let mut zip_path: Option<PathBuf> = None;
    if let Some(zp) = cli.zip {
        // ensure that zp does not exist
        if Path::new(&zp).exists() {
            eprintln!("File already exists: {}", &zp);
            std::process::exit(1);
        }
        zip_path = if !zp.ends_with(".zip") {
            Some(Path::new(&zp).with_extension("zip"))
        } else {
            Some(Path::new(&zp).to_path_buf())
        }
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
        let file = File::open(in_file).expect("Failed to open in_file");
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
    create_dir_all(out_dir.to_owned()).expect("Failed to create out_dir");

    // create zip_file and writer
    let zip_writer: Option<ZipWriter<File>> = if let Some(zp) = zip_path.as_ref() {
        let zipfile = File::create(zp).expect("Failed to create zip file");
        Some(ZipWriter::new(zipfile))
    } else {
        None
    };

    let mut line = String::new();
    let mut db_use_statement = String::new();

    let make_path = |dir: String, obj: DatabaseObject| -> String {
        if *only_object_names || obj.schema.is_empty() {
            format!("{}/{}.sql", dir, obj.name)
        } else {
            format!("{}/{}.{}.sql", dir, obj.schema, obj.name)
        }
    };

    // read lines in in_file and split into separate files
    // these two branches are very similar, but one of them writes the files
    // directly into a zip file
    if let Some(mut zip_writer) = zip_writer {
        // write to zip file
        let zip_parent_dir: String = zip_path.expect("zip_path was None")
            .as_path()
            .file_stem().expect("file should have stem")
            .to_os_string()
            .into_string().expect("failed to convert os string to string");
        zip_writer.add_directory(
            &zip_parent_dir,
            zip::write::FileOptions::default())
            .expect("failed to add parent directory to zip file");
        let mut writer = BufWriter::new(zip_writer);
        loop {
            // ensure file is (still) readable
            // exit if nothing left to read or if there was an error
            match reader.has_data_left() {
                Ok(false) => {
                    writer.flush().expect("Error writing to zip file");
                    let zw = writer.get_mut();
                    zw.finish().expect("Error finishing zip file");
                    break;
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
                reader.read_line(&mut line).expect("Error reading line");
                db_use_statement.push_str(line.as_str());
            } else if line.starts_with("/****** Object:") {
                if let Ok(obj) = DatabaseObject::try_from(line.as_str()) {
                    let dir: String = [
                        &zip_parent_dir,
                        obj.object_type.to_string().as_str(),
                        ].join("/");

                    let path = make_path(dir.to_owned(), obj);
                    if *verbose {
                        println!("creating {:?}", path);
                    }

                    let zw = writer.get_mut();
                    zw.start_file(path.as_str(), Default::default())
                        .expect("Error adding file to zip file");

                    writer.write(db_use_statement.as_bytes())
                        .expect("Error writing db_use_statement to zip file");
                    writer.write(line.as_bytes())
                        .expect("Error writing line to zip file");
                }
            } else {
                writer.write(line.as_bytes())
                    .expect("Error writing line to zip file");
            }
            line.clear();
        }
    } else {
        // write to individual files
        let mut writer: Option<BufWriter<File>> = None;
        loop {
            // ensure file is (still) readable
            // exit if nothing left to read or if there was an error
            match reader.has_data_left() {
                Ok(false) => {
                    if let Some(mut w) = writer {
                        w.flush().expect("failed to flush writer");
                    }
                    break;
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
                reader.read_line(&mut line).expect("Error reading line");
                db_use_statement.push_str(line.as_str());
            } else if line.starts_with("/****** Object:") {
                if let Ok(obj) = DatabaseObject::try_from(line.as_str()) {
                    let dir = [
                        out_dir.as_str(),
                        obj.object_type.to_string().as_str(),
                        ].join("/");

                    // ensure that dir exists
                    create_dir_all(dir.to_owned())
                        .expect("failed to create dir");

                    if let Some(w) = writer.as_mut() {
                        w.flush().expect("failed to flush writer");
                    }

                    let path = make_path(dir.to_owned(), obj);
                    if *verbose {
                        println!("creating {:?}", path);
                    }

                    let file = File::create(path)
                        .expect("failed to create file");
                    let mut _writer: BufWriter<File> = BufWriter::new(file);
                    _writer.write(db_use_statement.as_bytes())
                        .expect("Error writing db_use_statement to file");
                    _writer.write(line.as_bytes())
                        .expect("Error writing line to file");
                    writer = Some(_writer);
                }
            } else {
                if let Some(w) = writer.as_mut() {
                    w.write(line.as_bytes())
                        .expect("Error writing line to file");
                }
            }
            line.clear();
        }
    }
}
