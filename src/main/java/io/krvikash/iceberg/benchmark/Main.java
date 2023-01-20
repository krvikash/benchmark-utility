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
package io.krvikash.iceberg.benchmark;

import io.krvikash.iceberg.benchmark.statistics.StatisticsDriver;

import static io.krvikash.iceberg.benchmark.Format.Case.LOWER;
import static io.krvikash.iceberg.benchmark.Format.ORC;
import static io.krvikash.iceberg.benchmark.Format.PARQUET;

public class Main
{
    public static void main(String[] args)
    {
        String bucket = "benchmarks-data"; // change here if needed
        String schemaPrefix = "iceberg"; // change here if needed
        int scaleFactor = 1000; // change here if needed
        Format.Case formatCase = LOWER; // change here if needed

        S3Client s3Client = new S3Client.Builder()
                .withBucket(bucket)
                .build();
        s3Client.createS3Client();
        StatisticsDriver statisticsDriver = new StatisticsDriver(s3Client);

        // TPC-H files
        statisticsDriver.printTpchStatistics(schemaPrefix, scaleFactor, PARQUET, formatCase, false);
        statisticsDriver.printTpchStatistics(schemaPrefix, scaleFactor, PARQUET, formatCase, true);
        statisticsDriver.printTpchStatistics(schemaPrefix, scaleFactor, ORC, formatCase, false);
        statisticsDriver.printTpchStatistics(schemaPrefix, scaleFactor, ORC, formatCase, true);

        // TPC-DS files
        statisticsDriver.printTpcdsStatistics(schemaPrefix, scaleFactor, PARQUET, formatCase, false);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, scaleFactor, PARQUET, formatCase, true);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, scaleFactor, ORC, formatCase, false);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, scaleFactor, ORC, formatCase, true);
    }
}
