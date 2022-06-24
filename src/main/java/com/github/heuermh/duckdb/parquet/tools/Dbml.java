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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;

import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.concurrent.Callable;

import com.google.common.base.Joiner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Describe a DuckDB database in DBML (https://dbml.org/) format.
 */
@Command(name = "dbml")
public final class Dbml implements Callable<Integer> {

    @Option(names = { "-u", "--url" })
    private String url = "jdbc:duckdb:";

    @Override
    public Integer call() throws Exception {

        // connect to DuckDB
        Class.forName("org.duckdb.DuckDBDriver");
        try (Connection connection = DriverManager.getConnection(url)) {

            DatabaseMetaData databaseMetaData = connection.getMetaData();

            System.out.println("url " + url);
            System.out.println("databaseMetaData " + databaseMetaData);
            System.out.println("  userName " + databaseMetaData.getUserName());
            System.out.println("  productName " + databaseMetaData.getDatabaseProductName());
            System.out.println("  productVersion " + databaseMetaData.getDatabaseProductVersion());
            System.out.println("  driverName " + databaseMetaData.getDriverName());
            System.out.println("  driverVersion " + databaseMetaData.getDriverVersion());

            System.out.println("tables");
            // list all tables
            List<String> tables = new ArrayList<String>();
            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{ "TABLE" })) {
                System.out.println("  resultSet " + resultSet);
                while (resultSet.next()) { 
                    String tableName = resultSet.getString("TABLE_NAME");
                    System.out.println("    tableName " + tableName);
                    tables.add(tableName);
                }
            }

            // gather table metadata
            for (String table : tables) {
                Map<String, Column> columns = new LinkedHashMap<String, Column>();
                ListMultimap<String, String> primaryKeys = ArrayListMultimap.create();

                try (ResultSet resultSet = databaseMetaData.getColumns(null, null, table, null)) {
                    while (resultSet.next()) {
                        String columnName = resultSet.getString("COLUMN_NAME");
                        String columnSize = resultSet.getString("COLUMN_SIZE");
                        String dataType = resultSet.getString("DATA_TYPE");
                        boolean isNullable = "TRUE".equals(resultSet.getString("IS_NULLABLE"));
                        boolean isAutoIncrement = "TRUE".equals(resultSet.getString("IS_AUTOINCREMENT"));
                        columns.put(columnName, new Column(columnName, columnSize, dataType, isNullable, isAutoIncrement));
                    }
                }
                try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, table)) { 
                    while (resultSet.next()) { 
                        String primaryKeyColumnName = resultSet.getString("COLUMN_NAME"); 
                        String primaryKeyName = resultSet.getString("PK_NAME");
                        primaryKeys.put(primaryKeyName, primaryKeyColumnName);
                    }
                }
                /*
                try (ResultSet resultSet = databaseMetaData.getImportedKeys(null, null, table)) {
                    while (resultSet.next()) {
                        String pkTableName = resultSet.getString("PKTABLE_NAME");
                        String fkTableName = resultSet.getString("FKTABLE_NAME");
                        String pkColumnName = resultSet.getString("PKCOLUMN_NAME");
                        String fkColumnName = resultSet.getString("FKCOLUMN_NAME");
                    }
                }
                */

                System.out.println("Table \"" + table + "\" {");
                for (Column column : columns.values()) {
                    System.out.println(describe(column));
                }
                if (!primaryKeys.isEmpty()) {
                    System.out.println("  indexes {");
                    for (String primaryKeyName : primaryKeys.keySet()) {
                        List<String> columnNames = primaryKeys.get(primaryKeyName);
                        if (columnNames.size() == 1) {
                            System.out.println("    \"" + columnNames.get(0) + "\" [pk]");
                        }
                        else if (columnNames.size() > 1) {
                            System.out.println("    (\"" + Joiner.on("\",\"").join(columnNames) + "\") [pk]");
                        }
                    }
                    System.out.println("  }");
                }
                System.out.println("}");
            }

            /*
            // list all views
            List<String> views = new ArrayList<String>();
            try (ResultSet resultSet = databaseMetaData.getTables(null, null, null, new String[]{ "VIEW" })) { 
                while (resultSet.next()) { 
                    String tableName = resultSet.getString("TABLE_NAME");
                    views.add(tableName);
                }
            }
            */
        }
        return 0;
    }

    static String describe(final Column column) {
        StringBuilder sb = new StringBuilder();
        sb.append("  \"");
        sb.append(column.getColumnName());
        sb.append("\"  \"");
        sb.append(column.getDataType());
        if (column.getColumnSize() != null) {
            sb.append("(");
            sb.append(column.getColumnSize());
            sb.append(")");
        }
        sb.append("\"");
        return sb.toString();
    }

    /**
     * Column metadata data class.
     */
    static final class Column {
        final String columnName;
        final String columnSize;
        final String dataType;
        final boolean isNullable;
        final boolean isAutoIncrement;

        Column(final String columnName,
               final String columnSize,
               final String dataType,
               final boolean isNullable,
               final boolean isAutoIncrement) {

            this.columnName = columnName;
            this.columnSize = columnSize;
            this.dataType = dataType;
            this.isNullable = isNullable;
            this.isAutoIncrement = isAutoIncrement;
        }

        String getColumnName() {
            return columnName;
        }

        String getColumnSize() {
            return columnSize;
        }

        String getDataType() {
            return dataType;
        }

        boolean isNullable() {
            return isNullable;
        }

        boolean isAutoIncrement() {
            return isAutoIncrement;
        }
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
