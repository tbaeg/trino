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
package io.trino.metastore;

import io.trino.spi.TrinoException;

import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;

public class SchemaAlreadyExistsException
        extends TrinoException
{
    private final String schemaName;

    public SchemaAlreadyExistsException(String schemaName)
    {
        this(schemaName, null);
    }

    public SchemaAlreadyExistsException(String schemaName, Throwable cause)
    {
        this(schemaName, format("Schema already exists: '%s'", schemaName), cause);
    }

    public SchemaAlreadyExistsException(String schemaName, String message, Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.schemaName = schemaName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }
}
