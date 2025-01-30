# duckdb-parquet-tools

[Apache Parquet format](https://parquet.apache.org/) tools for [DuckDB](https://duckdb.org/).

### Hacking duckdb-parquet-tools

Install

 * JDK 1.8 or later, https://openjdk.java.net
 * Apache Maven 3.6.3 or later, https://maven.apache.org

To build
```
$ mvn package

$ export PATH=$PATH:`pwd`/target/appassembler/bin

$ duckdb-parquet-tools --help
USAGE
  duckdb-parquet-tools [-hV] [COMMAND]

OPTIONS
  -h, --help      Show this help message and exit.
  -V, --version   Print version information and exit.

COMMANDS
  convert              Convert input Parquet file to DuckDB as Parquet file.
  create               Create DuckDB table and write as Parquet file.
  describe             Describe Parquet file columns as loaded by DuckDB.
  head                 Write the first n records from a Parquet file in JSON format.
  meta                 Query the metadata of a Parquet file as loaded by DuckDB.
  ratios               Show data compression ratios from a Parquet file as loaded by DuckDB.
  schema               Query the internal schema of a Parquet file as loaded by DuckDB.
  help                 Display help information about the specified command.
  generate-completion  Generate bash/zsh completion script for duckdb-parquet-tools.
```
