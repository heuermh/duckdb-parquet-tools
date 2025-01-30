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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Show data compression ratios from a Parquet file as loaded by DuckDB.
 */
@Command(name = "ratios")
public final class Ratios implements Callable<Integer> {

    @Option(names = { "-i", "--input-parquet-file" }, required = true)
    private File inputParquetFile = null;

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    private static final String META_SQL = "CREATE TABLE m AS SELECT * from parquet_metadata('%s')";
    private static final String RATIOS_SQL = "SELECT column_id, path_in_schema, type, sum(num_values) AS n, sum(total_uncompressed_size) AS uncompressed, sum(total_compressed_size) AS compressed, (uncompressed/compressed) AS ratio, ((1 - (compressed/uncompressed)) * 100.0) AS savings FROM m GROUP BY column_id, path_in_schema, type ORDER BY column_id ASC";

    @Override
    public Integer call() throws Exception {

        // connect to DuckDB
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection(url)) {

            try (Statement create = connection.createStatement()) {

                // create parquet metadata table
                create.execute(String.format(META_SQL, inputParquetFile.toString()));

                // query ratios grouping by to sum over row groups
                try (ResultSet resultSet = create.executeQuery(RATIOS_SQL)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();
                    int columns = metaData.getColumnCount() + 1;

                    // print header
                    for (int i = 1; i < columns; i++) {
                        System.out.print(metaData.getColumnLabel(i) + "\t");
                    }
                    System.out.print("\n");

                    // print rows
                    while (resultSet.next()) {
                        for (int i = 1; i < columns; i++) {
                            Object value = resultSet.getObject(i);
                            System.out.print(value == null ? "" : value.toString() + "\t");
                        }
                        System.out.print("\n");
                    }
                }
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
        System.exit(new CommandLine(new Ratios()).execute(args));
    }
}
