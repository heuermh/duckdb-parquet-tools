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

import java.util.List;

import picocli.AutoComplete.GenerateCompletion;

import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;

/**
 * Apache Parquet format tools for DuckDB.
 */
@Command(
  name = "duckdb-parquet-tools",
  scope = ScopeType.INHERIT,
  subcommands = {
      Convert.class,
      Create.class,
      Describe.class,
      Head.class,
      Meta.class,
      Ratios.class,
      Schema.class,
      HelpCommand.class,
      GenerateCompletion.class
  },
  mixinStandardHelpOptions = true,
  sortOptions = false,
  usageHelpAutoWidth = true,
  resourceBundle = "com.github.heuermh.duckdb.parquet.tools.Messages",
  versionProvider = com.github.heuermh.duckdb.parquet.tools.About.class
)
public final class Tools {

    @Parameters(hidden = true)
    private List<String> ignored;

    /**
     * Main.
     *
     * @param args command line args
     */
    public static void main(final String[] args) {
        System.exit(new CommandLine(new Tools()).execute(args));
    }
}
