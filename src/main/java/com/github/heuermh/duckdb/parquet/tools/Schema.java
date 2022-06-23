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
 * Query the internal schema of a Parquet file as loaded by DuckDB.
 */
@Command(name = "schema")
public final class Schema implements Callable<Integer> {

    @Option(names = { "-i", "--input-parquet-file" }, required = true)
    private File inputParquetFile = null;

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    private static final String SCHEMA_SQL = "SELECT * from parquet_schema('%s')";

    @Override
    public Integer call() throws Exception {

        // connect to DuckDB
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection(url)) {

            // schema Parquet file columns as loaded by DuckDB
            try (Statement create = connection.createStatement()) {
                String sql = String.format(SCHEMA_SQL, inputParquetFile.toString());
                try (ResultSet resultSet = create.executeQuery(sql)) {
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
        System.exit(new CommandLine(new Schema()).execute(args));
    }
}
