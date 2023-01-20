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

import io.krvikash.iceberg.benchmark.Benchmark;
import io.krvikash.iceberg.benchmark.Format;
import io.krvikash.iceberg.benchmark.S3Client;
import io.krvikash.iceberg.benchmark.tpcds.TpcdsBenchmark;
import io.krvikash.iceberg.benchmark.tpcds.TpcdsStatistics;
import io.krvikash.iceberg.benchmark.tpch.TpchBenchmark;
import io.krvikash.iceberg.benchmark.tpch.TpchStatistics;

public class StatisticsDriver
{
    private S3Client s3Client;

    public StatisticsDriver(S3Client s3Client)
    {
        this.s3Client = s3Client;
    }

    public void printTpchStatistics(String schemaPrefix, int scaleFactor, Format format, Format.Case formatCase, boolean isPartitioned)
    {
        Benchmark benchmark = new TpchBenchmark(schemaPrefix, scaleFactor, format, formatCase, isPartitioned);
        Statistics statistics = new TpchStatistics(s3Client, benchmark);
        statistics.printNDV(statistics.getNDV());
    }

    public void printTpcdsStatistics(String schemaPrefix, int scaleFactor, Format format, Format.Case formatCase, boolean isPartitioned)
    {
        Benchmark benchmark = new TpcdsBenchmark(schemaPrefix, scaleFactor, format, formatCase, isPartitioned);
        Statistics statistics = new TpcdsStatistics(s3Client, benchmark);
        statistics.printNDV(statistics.getNDV());
    }
}
