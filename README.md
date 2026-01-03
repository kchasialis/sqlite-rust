A minimal SQLite implementation in Rust that parses the SQLite file format and executes basic SQL queries.

Built to learn Rust and understand database internals.

## Features

- **File Format Parsing**: Reads SQLite database headers, page structures, and B-tree data
- **Query Support**:
  - `SELECT COUNT(*) FROM <table>`
  - `SELECT <columns> FROM <table>`
  - `SELECT <columns> FROM <table> WHERE <column> = '<value>'`
- **Index Optimization**: Automatically uses B-tree indexes for `WHERE` clause filtering when available
- **Data Types**: Handles INTEGER, TEXT, REAL, and BLOB columns

## Running Locally

Requires `cargo (1.91)` or later.
```sh
./your_program.sh sample.db "SELECT id, name FROM apples"
```

The first run compiles the project and may be slow. Subsequent runs are fast.

## Sample Databases

**Included:**
- `sample.db` - Small database with `apples` and `oranges` tables (~few KB)

**Download separately:**
```sh
./download_sample_databases.sh
```

- `superheroes.db` - 1MB database for testing table scans
- `companies.db` - 7MB database with an index for testing index scans

Explore databases with:
```sh
sqlite3 sample.db "SELECT id, name FROM apples"
```

## Implementation Notes

Topics explored:
- [SQLite's file format](https://www.sqlite.org/fileformat.html)
- [B-tree storage structures](https://jvns.ca/blog/2014/10/02/how-does-sqlite-work-part-2-btrees/)
- Query optimization with indexes
- Rust systems programming
