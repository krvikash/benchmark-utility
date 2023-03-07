/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.krvikash.iceberg.benchmark.statistics;

import com.google.common.collect.ImmutableList;
import io.krvikash.iceberg.benchmark.Benchmark;
import io.krvikash.iceberg.benchmark.S3Client;

import java.util.List;

import static io.krvikash.iceberg.benchmark.BenchmarkUtils.getDataPathPrefix;
import static io.krvikash.iceberg.benchmark.BenchmarkUtils.getTableLocation;
import static io.krvikash.iceberg.benchmark.BenchmarkUtils.humanReadableByteCountSI;
import static java.lang.String.format;

public abstract class Statistics
{
    protected S3Client s3Client;
    protected Benchmark benchmark;

    public List<NDV> getNDV()
    {
        ImmutableList.Builder counter = new ImmutableList.Builder();
        for (String tableName : benchmark.getTableNames()) {
            String path = getDataPathPrefix(benchmark.schemaName(), tableName);
            NDV ndv = s3Client.getNDV(path);
            counter.add(ndv);
            // To print incrementally
            // System.out.println(ndv);
        }
        return counter.build();
    }

    public void printNDV(List<NDV> ndvList)
    {
        long totalSize = 0;
        System.out.println(format(
                "============== NDV: [%s] ============== iceberg.target_max_file_size = '%s'; ==============",
                benchmark.schemaName(),
                benchmark.targetMaxFileSize()));
        int i = 1;
        for (NDV ndv : ndvList)
        {
            System.out.println(i++ + ". " + ndv);
            totalSize += ndv.totalContentSize();
        }
        System.out.println(format("============== Total Size: [%s] ==============\n", humanReadableByteCountSI(totalSize)));
    }

    public void printRegisterQuery(String bucket)
    {
        for (String tableName : benchmark.getTableNames()) {
            String path = getTableLocation(bucket, benchmark.schemaName(), tableName);
            System.out.println(format("CALL iceberg.system.register_table ('%s', '%s', '%s');", clean(benchmark.schemaName()), tableName, path));
        }
    }

    public void printUnregisterQuery()
    {
        for (String tableName : benchmark.getTableNames()) {
            System.out.println(format("CALL iceberg.system.unregister_table ('%s', '%s');", clean(benchmark.schemaName()), tableName));
        }
    }

    public void printDropTableQuery()
    {
        for (String tableName : benchmark.getTableNames()) {
            System.out.println(format("DROP TABLE IF EXISTS iceberg.%s.%s;", clean(benchmark.schemaName()), tableName));
        }
    }

    public void printDropTableAndSchemaQuery()
    {
        for (String tableName : benchmark.getTableNames()) {
            System.out.println(format("DROP TABLE IF EXISTS iceberg.%s.%s;", clean(benchmark.schemaName()), tableName));
        }
        System.out.println(format("DROP SCHEMA IF EXISTS iceberg.%s;", clean(benchmark.schemaName())));
    }

    public void downloadMetadata()
    {
        String bucket = s3Client.getBucket();
        for (String tableName : benchmark.getTableNames()) {
            String source = getTableLocation(bucket, benchmark.schemaName(), tableName) + "/metadata";
            String target = "download/" + source.replace("s3://" + bucket, "");
            s3Client.download(source, target);
            System.out.println("Downloaded " + source + " at " + target);
        }
    }

    public void printDropSchemaQuery()
    {
        System.out.println(format("DROP SCHEMA IF EXISTS iceberg.%s;", clean(benchmark.schemaName())));
    }

    private static String clean(String schemaName)
    {
        return schemaName.replace("-", "_");
    }
}
