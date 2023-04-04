# SQL Splitter

Split the output of SSMS-scripted objects (even the whole database) into
individual files.

## Usage:
```
Usage: sql-splitter [OPTIONS] [IN_FILE]

Arguments:
  [IN_FILE]  File(s) to process

Options:
  -d, --out-dir <OUT_DIR>  Output directory to create files [default: .]
  -n, --only_names         Exclude schema-name from filenames
  -v, --verbose            Verbose output
  -w, --windows-1252       specify that input files are using windows-1252 encoding instead of UTF-8
  -h, --help               Print help
  -V, --version            Print version
```

If `<IN_FILE>` is not specified, it will be read from STDIN (useful if you told
SSMS to copy its output to the clipboard).

If `<OUT_DIR>` is not specified, it the files will be created in the current
directory.

The output files will be named according to the following rules:
  `<ObjectType>/[<Schema>.]<ObjectName>.sql`
  - `<Schema>` is only populated for objects where database-schema is relevant.
  - Supplying the `--only_names` option will exclude schema-name from filenames.

If running this script gives an error relating to files not being UTF-8
encoded, you can run it with the `--windows-1252` option.
