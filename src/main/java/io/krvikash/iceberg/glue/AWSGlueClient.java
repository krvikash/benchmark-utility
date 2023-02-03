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
package io.krvikash.iceberg.glue;

import com.amazonaws.services.glue.AWSGlueAsync;
import com.amazonaws.services.glue.AWSGlueAsyncClientBuilder;
import com.amazonaws.services.glue.model.Database;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;

import static java.lang.String.format;

public class AWSGlueClient
{
    private AWSGlueAsync glueClient;

    public AWSGlueClient()
    {
        this.glueClient = AWSGlueAsyncClientBuilder.defaultClient();
    }

    public void printDatabases()
    {
        GetDatabasesRequest databaseRequest = new GetDatabasesRequest();
        GetDatabasesResult result;
        result = glueClient.getDatabases(databaseRequest);
        result.getDatabaseList().forEach(database -> System.out.println(database.getName()));
        System.out.println("Total Databases: " + result.getDatabaseList().size());
        databaseRequest.setNextToken(databaseRequest.getNextToken());
        result = glueClient.getDatabases(databaseRequest);
        result.getDatabaseList().forEach(database -> System.out.println(database.getName()));
        System.out.println("Total Databases: " + result.getDatabaseList().size());
    }

    public void printTables()
    {
        GetDatabasesRequest databaseRequest = new GetDatabasesRequest();
        GetDatabasesResult result = glueClient.getDatabases(databaseRequest);
        for (Database database : result.getDatabaseList())
        {
            GetTablesRequest request = new GetTablesRequest().withDatabaseName(database.getName());
            GetTablesResult tablesResult = glueClient.getTables(request);
            tablesResult.getTableList().forEach(table -> System.out.println(table.getName()));
            System.out.println(format("Total Tables in Database %s is %s ", database.getName(), tablesResult.getTableList().size()));
        }
    }

    public void printTables(String database)
    {
        GetTablesRequest request = new GetTablesRequest().withDatabaseName(database);
        glueClient.getTables(request).getTableList().forEach(table -> System.out.println(table.getName()));
    }

    public boolean databaseExists(String database)
    {
        return glueClient.getDatabase(new GetDatabaseRequest().withName(database)).getDatabase().getName().equals(database);
    }
}
