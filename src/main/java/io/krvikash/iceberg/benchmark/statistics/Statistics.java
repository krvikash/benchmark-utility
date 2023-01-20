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
            counter.add(s3Client.getNDV(path));
        }
        return counter.build();
    }

    public void printNDV(List<NDV> ndvList)
    {
        long totalSize = 0;
        System.out.println(format("============== NDV: [%s] ==============", benchmark.schemaName()));
        int i = 1;
        for (NDV ndv : ndvList)
        {
            System.out.println(i++ + ". " + ndv);
            totalSize += ndv.totalContentSize();
        }
        System.out.println(format("============== Total Size: [%s] ==============\n", humanReadableByteCountSI(totalSize)));
    }
}
