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

import picocli.CommandLine.IVersionProvider;

/**
 * About.
 */
public final class About implements IVersionProvider {
    private static final String ARTIFACT_ID = "${project.artifactId}";
    private static final String BUILD_TIMESTAMP = "${build-helper-maven-plugin.build.timestamp}";
    private static final String COMMIT = "${git.commit.id}";
    private static final String COPYRIGHT = "Copyright (c) 2022-2023 held jointly by the individual authors.";
    private static final String LICENSE = "Licensed Apache License 2.0.";
    private static final String VERSION = "${project.version}";


    /**
     * Return the artifact id.
     *
     * @return the artifact id
     */
    public String artifactId() {
        return ARTIFACT_ID;
    }

    /**
     * Return the build timestamp.
     *
     * @return the build timestamp
     */
    public String buildTimestamp() {
        return BUILD_TIMESTAMP;
    }

    /**
     * Return the last commit.
     *
     * @return the last commit
     */
    public String commit() {
        return COMMIT;
    }

    /**
     * Return the copyright.
     *
     * @return the copyright
     */
    public String copyright() {
        return COPYRIGHT;
    }

    /**
     * Return the license.
     *
     * @return the license
     */
    public String license() {
        return LICENSE;
    }

    /**
     * Return the version.
     *
     * @return the version
     */
    public String version() {
        return VERSION;
    }

    @Override
    public String[] getVersion() {
        return new String[] {
            "@|fg(blue) " + artifactId() + "|@ " + version(),
            "@|fg(yellow) Commit:|@ " + commit() + " @|fg(yellow) Build:|@ " + buildTimestamp(),
            copyright(),
            license()
        };
    }
}
