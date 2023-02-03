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

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import static java.lang.String.format;

public class BenchmarkUtils
{
    public static String humanReadableByteCountSI(long bytes) {
        if (-1000 < bytes && bytes < 1000) {
            return bytes + " B";
        }
        CharacterIterator ci = new StringCharacterIterator("kMGTPE");
        while (bytes <= -999_950 || bytes >= 999_950) {
            bytes /= 1000;
            ci.next();
        }
        return String.format("%.1f %cB", bytes / 1000.0, ci.current());
    }

    public static String getDataPath(String bucket, String dataPathPrefix)
    {
        return format("s3://%s/%s", bucket, dataPathPrefix);
    }

    public static String getTableLocation(String bucket, String schemaName, String tableName)
    {
        return format("s3://%s/%s/%s", bucket, schemaName, tableName);
    }

    public static String getDataPathPrefix(String schemaName, String tableName)
    {
        return format("%s/%s/data", schemaName, tableName);
    }
}
