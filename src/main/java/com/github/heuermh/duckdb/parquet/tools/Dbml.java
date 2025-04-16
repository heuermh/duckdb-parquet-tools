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

import com.google.common.io.Files;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Describe Parquet file as loaded by DuckDB in DBML format.
 */
@Command(name = "dbml")
public final class Dbml implements Callable<Integer> {

    @Option(names = { "-i", "--input-parquet-file" }, required = true)
    private File inputParquetFile = null;

    @Option(names = { "-t", "--table-name" }, required = false)
    private String tableName;

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    /** Describe SQL query. */
    private static final String DESCRIBE_SQL = "SELECT * from read_parquet('%s') WHERE 1=0";

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

                    if (metaData.getColumnCount() > 0) {
                        System.out.print("Table ");
                        System.out.print(tableName == null ? baseName(inputParquetFile) : tableName);
                        System.out.println(" {");
                    }

                    // print rows
                    for (int i = 1; i < (metaData.getColumnCount() + 1); i++) {
                        System.out.println("  " + metaData.getColumnName(i) + " " + metaData.getColumnTypeName(i).toLowerCase());
                    }

                    if (metaData.getColumnCount() > 0) {
                        System.out.println("}");
                    }
                }
            }
        }
        return 0;
    }

    static String baseName(final File file) {
        return Files.getNameWithoutExtension(file.toString());
    }

    /**
     * Main.
     *
     * @param args command line args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(new Dbml()).execute(args));
    }
}
