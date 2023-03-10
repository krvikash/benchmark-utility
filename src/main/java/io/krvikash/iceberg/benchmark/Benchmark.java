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

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.krvikash.iceberg.benchmark.Format.Case.LOWER;
import static java.lang.String.format;

public abstract class Benchmark
{
    protected String prefix;
    protected Optional<FileSize> fileSize;
    protected int scaleFactor;
    protected String name;
    protected Format format;
    protected Format.Case formatCase;
    protected boolean isPartitioned;

    public abstract List<String> getTableNames();

    public String targetMaxFileSize()
    {
        if (fileSize.isPresent()) {
            return fileSize.get().toString();
        }
        return "1GB";
    }

    public String schemaName()
    {
        return format(
                "%s-%s-sf%s-%s%s",
                getPrefixWithFileSizeIfNeeded(),
                name,
                scaleFactor,
                formatCase == LOWER
                        ? format.toString().toLowerCase(Locale.ENGLISH)
                        : format.toString().toUpperCase(Locale.ENGLISH),
                isPartitioned ? "-part" : "");
    }

    private String getPrefixWithFileSizeIfNeeded()
    {
        if (fileSize.isEmpty()) {
            return prefix;
        }
        // Use it, if you want file size to be included in prefix
        //return prefix + "-" + fileSize.get() + "-files";
        return prefix + "-" + "small" + "-files";
    }
}
