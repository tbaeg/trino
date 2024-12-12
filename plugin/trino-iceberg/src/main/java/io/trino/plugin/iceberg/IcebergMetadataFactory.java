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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class IcebergMetadataFactory
{
    private final TypeManager typeManager;
    private final CatalogHandle trinoCatalogHandle;
    private final JsonCodec<CommitTaskData> commitTaskCodec;
    private final TrinoCatalogFactory catalogFactory;
    private final IcebergFileSystemFactory fileSystemFactory;
    private final TableStatisticsWriter tableStatisticsWriter;
    private final Optional<HiveMetastoreFactory> metastoreFactory;
    private final boolean addFilesProcedureEnabled;
    private final Predicate<String> allowedExtraProperties;
    private final ExecutorService executor;

    @Inject
    public IcebergMetadataFactory(
            TypeManager typeManager,
            CatalogHandle trinoCatalogHandle,
            JsonCodec<CommitTaskData> commitTaskCodec,
            TrinoCatalogFactory catalogFactory,
            IcebergFileSystemFactory fileSystemFactory,
            TableStatisticsWriter tableStatisticsWriter,
            @RawHiveMetastoreFactory Optional<HiveMetastoreFactory> metastoreFactory,
            @ForIcebergScanPlanning ExecutorService executor,
            IcebergConfig config)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.trinoCatalogHandle = requireNonNull(trinoCatalogHandle, "trinoCatalogHandle is null");
        this.commitTaskCodec = requireNonNull(commitTaskCodec, "commitTaskCodec is null");
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.tableStatisticsWriter = requireNonNull(tableStatisticsWriter, "tableStatisticsWriter is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.addFilesProcedureEnabled = config.isAddFilesProcedureEnabled();
        if (config.getAllowedExtraProperties().equals(ImmutableList.of("*"))) {
            this.allowedExtraProperties = _ -> true;
        }
        else {
            this.allowedExtraProperties = ImmutableSet.copyOf(requireNonNull(config.getAllowedExtraProperties(), "allowedExtraProperties is null"))::contains;
        }
    }

    public IcebergMetadata create(ConnectorIdentity identity)
    {
        return new IcebergMetadata(
                typeManager,
                trinoCatalogHandle,
                commitTaskCodec,
                catalogFactory.create(identity),
                fileSystemFactory,
                tableStatisticsWriter,
                metastoreFactory,
                addFilesProcedureEnabled,
                allowedExtraProperties,
                executor);
    }
}
