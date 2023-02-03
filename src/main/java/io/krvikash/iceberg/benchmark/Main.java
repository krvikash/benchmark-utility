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

import java.util.Optional;

import static io.krvikash.iceberg.benchmark.Format.Case.UPPER;
import static io.krvikash.iceberg.benchmark.Format.ORC;
import static io.krvikash.iceberg.benchmark.Format.PARQUET;

public class Main
{
    public static void main(String[] args)
    {
        String bucket = "benchmarks-data"; // change here if needed
        String schemaPrefix = "iceberg"; // change here if needed
        Optional<FileSize> fileSize = Optional.of(FileSize.of(1280, FileSize.Unit.KB)); // change here if needed
        // Use this when size is not needed
        // fileSize = Optional.empty();
        int scaleFactor = 1000; // change here if needed
        Format.Case formatCase = UPPER; // change here if needed
        // formatCase = LOWER;

        S3Client s3Client = new S3Client.Builder()
                .withBucket(bucket)
                .build();
        s3Client.createS3Client();
        StatisticsDriver statisticsDriver = new StatisticsDriver(s3Client);

        // TPC-H files
        statisticsDriver.printTpchStatistics(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, false);
        statisticsDriver.printTpchStatistics(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, true);
        statisticsDriver.printTpchStatistics(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, false);
        statisticsDriver.printTpchStatistics(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, true);

        // TPC-DS files
        statisticsDriver.printTpcdsStatistics(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, false);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, true);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, false);
        statisticsDriver.printTpcdsStatistics(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, true);

        // TPC-H files
//        statisticsDriver.printTpchDropTable(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, false);
//        statisticsDriver.printTpchDropTable(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, true);
//        statisticsDriver.printTpchDropTable(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, false);
//        statisticsDriver.printTpchDropTable(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, true);

        // TPC-DS files
//        statisticsDriver.printTpcdsDropTable(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, false);
//        statisticsDriver.printTpcdsDropTable(schemaPrefix, fileSize, scaleFactor, PARQUET, formatCase, true);
//        statisticsDriver.printTpcdsDropTable(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, false);
//        statisticsDriver.printTpcdsDropTable(schemaPrefix, fileSize, scaleFactor, ORC, formatCase, true);
    }
}
