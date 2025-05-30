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
package io.trino.plugin.hive;

import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;

import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_OFFLINE;

public class TableOfflineException
        extends TrinoException
{
    public TableOfflineException(SchemaTableName tableName, boolean forPresto, String offlineMessage)
    {
        super(HIVE_TABLE_OFFLINE, formatMessage(tableName, forPresto, offlineMessage));
    }

    private static String formatMessage(SchemaTableName tableName, boolean forPresto, String offlineMessage)
    {
        StringBuilder resultBuilder = new StringBuilder()
                .append("Table '").append(tableName).append("'")
                .append(" is offline");
        if (forPresto) {
            resultBuilder.append(" for Presto");
        }
        if (!isNullOrEmpty(offlineMessage)) {
            resultBuilder.append(": ").append(offlineMessage);
        }
        return resultBuilder.toString();
    }
}
