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
package io.krvikash.iceberg.benchmark.tpcds;

import io.krvikash.iceberg.benchmark.Benchmark;
import io.krvikash.iceberg.benchmark.FileSize;
import io.krvikash.iceberg.benchmark.Format;
import io.trino.tpcds.Table;

import java.util.List;
import java.util.Optional;

public class TpcdsBenchmark
        extends Benchmark
{
    public TpcdsBenchmark(String prefix, Optional<FileSize> fileSize, int scaleFactor, Format format, Format.Case lowercaseFormat, boolean isPartitioned)
    {
        this.prefix = prefix;
        this.name = "tpcds";
        this.format = format;
        this.scaleFactor = scaleFactor;
        this.fileSize = fileSize;
        this.formatCase = lowercaseFormat;
        this.isPartitioned = isPartitioned;
    }

    @Override
    public List<String> getTableNames()
    {
        return Table.getBaseTables().stream()
                .filter(table -> !table.getName().equals("dbgen_version"))
                .map(table -> table.getName())
                .toList();
    }
}
