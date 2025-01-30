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
 * Describe Parquet file columns as loaded by DuckDB.
 */
@Command(name = "describe")
public final class Describe implements Callable<Integer> {

    @Option(names = { "-i", "--input-parquet-file" }, required = true)
    private File inputParquetFile = null;

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    @Option(names = { "--skip-header" })
    private boolean skipHeader;

    /** Describe SQL query. */
    private static final String DESCRIBE_SQL = "SELECT * from read_parquet('%s') WHERE 1=0";

    /** Headers to write. */
    private static final String HEADERS = "column\tname\ttype";

    @Override
    public Integer call() throws Exception {

        // connect to DuckDB
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection(url)) {

            // describe Parquet file columns as loaded by DuckDB
            try (Statement create = connection.createStatement()) {
                String sql = String.format(DESCRIBE_SQL, inputParquetFile.toString());
                try (ResultSet resultSet = create.executeQuery(sql)) {
                    ResultSetMetaData metaData = resultSet.getMetaData();

                    // print header
                    if (!skipHeader && (metaData.getColumnCount() > 0)) {
                        System.out.println(HEADERS);
                    }

                    // print rows
                    for (int i = 1; i < (metaData.getColumnCount() + 1); i++) {
                        System.out.println(i + "\t" + metaData.getColumnName(i) + "\t" + metaData.getColumnTypeName(i));
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
        System.exit(new CommandLine(new Describe()).execute(args));
    }
}
