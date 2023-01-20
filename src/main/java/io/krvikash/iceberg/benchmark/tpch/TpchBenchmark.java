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
package io.krvikash.iceberg.benchmark.tpch;

import io.krvikash.iceberg.benchmark.Benchmark;
import io.krvikash.iceberg.benchmark.Format;
import io.trino.tpch.TpchTable;

import java.util.List;

public class TpchBenchmark
        extends Benchmark
{
    public TpchBenchmark(String prefix, int scaleFactor, Format format, Format.Case formatCase, boolean isPartitioned)
    {
        this.prefix = prefix;
        this.name = "tpch";
        this.format = format;
        this.scaleFactor = scaleFactor;
        this.formatCase = formatCase;
        this.isPartitioned = isPartitioned;
    }
    @Override
    public List<String> getTableNames()
    {
        return TpchTable.getTables().stream()
                .map(table -> table.getTableName())
                .toList();
    }
}
