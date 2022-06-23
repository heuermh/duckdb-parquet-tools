/*
 * The authors of this file license it to you under the
 * Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You
 * may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.github.heuermh.duckdb.parquet.tools;

import java.io.File;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Convert input Parquet file to DuckDB as Parquet file.
 */
@Command(name = "convert")
public final class Convert implements Callable<Integer> {

    @Option(names = { "-i", "--input-parquet-file" }, required = true)
    private File inputParquetFile = null;

    @Option(names = { "-o", "--output-parquet-file" }, required = true)
    private File outputParquetFile = null;

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    @Option(names = { "-c", "--codec" })
    private String parquetCodec = "ZSTD";

    private static final String CREATE_SQL = "CREATE TABLE records AS SELECT * from read_parquet('%s')";
    private static final String COPY_SQL = "COPY records TO '%s' (FORMAT 'PARQUET', CODEC '%s')";

    @Override
    public Integer call() throws Exception {

        // connect to DuckDB
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection(url)) {

            // create in-memory DuckDB table from Parquet file
            try (Statement create = connection.createStatement()) {
                String sql = String.format(CREATE_SQL, inputParquetFile.toString());
                create.execute(sql);
            }

            // copy records from DuckDB table to disk as Parquet file
            try (Statement copy = connection.createStatement()) {
                String sql = String.format(COPY_SQL, outputParquetFile.toString(), parquetCodec);
                copy.execute(sql);
            }
        }
        return 0;
    }

    /**
     * Main.
     *
     * @param args command line args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(new Convert()).execute(args));
    }
}
