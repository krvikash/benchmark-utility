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

public class FileSize
{
    private int size;
    private Unit unit;

    private FileSize(int size, Unit unit)
    {
        this.size = size;
        this.unit = unit;
    }

    public static FileSize of(int size, Unit unit)
    {
        return new FileSize(size, unit);
    }

    @Override
    public String toString()
    {
        return String.valueOf(size) + unit;
    }

    public enum Unit
    {
        KB ("kB"),
        MB("MB"),
        GB("GB");

        private String name;

        Unit(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
