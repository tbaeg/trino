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
package io.trino.plugin.iceberg.system;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemView;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types.NestedField;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergUtil.getIdentityPartitions;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getAllPartitionFields;
import static io.trino.plugin.iceberg.util.SystemTableUtil.getPartitionColumnType;
import static io.trino.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class PartitionsView
        implements SystemView
{
    private static final Joiner COMMA_JOINER = Joiner.on(", ").skipNulls();

    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final boolean hasPartitionColumn;
    private final boolean hasDataColumn;
    private final String dataAggregationSql;
    private final List<ViewColumn> viewColumns;

    public PartitionsView(TypeManager typeManager, Table icebergTable, String catalogName, String schemaName, String tableName)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        ImmutableList.Builder<ViewColumn> viewColumns = ImmutableList.builder();
        Optional<IcebergPartitionColumn> partitionType = getPartitionColumnType(typeManager, getAllPartitionFields(icebergTable), icebergTable.schema());

        if (partitionType.isPresent()) {
            hasPartitionColumn = true;
            viewColumns.add(new ViewColumn("partition", partitionType.get().rowType().getTypeId(), Optional.empty()));
        }
        else {
            hasPartitionColumn = false;
        }

        Stream.of("record_count", "file_count", "total_size")
                .forEach(column -> viewColumns.add(new ViewColumn(column, BIGINT.getTypeId(), Optional.empty())));

        Set<Integer> identityPartitionIds = getIdentityPartitions(icebergTable.spec()).keySet().stream()
                .map(PartitionField::sourceId)
                .collect(toImmutableSet());

        List<NestedField> nonPartitionPrimitiveColumns = icebergTable.schema().columns().stream()
                .filter(column -> !identityPartitionIds.contains(column.fieldId()) && column.type().isPrimitiveType())
                .collect(toImmutableList());

        Optional<RowType> dataColumnType = getMetricsColumnType(typeManager, nonPartitionPrimitiveColumns);

        if (dataColumnType.isPresent()) {
            hasDataColumn = true;
            viewColumns.add(new ViewColumn("data", dataColumnType.get().getTypeId(), Optional.empty()));
            dataAggregationSql = buildDataAggregation(typeManager, nonPartitionPrimitiveColumns);
        }
        else {
            hasDataColumn = false;
            dataAggregationSql = "";
        }

        this.viewColumns = viewColumns.build();
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        return SchemaTableName.schemaTableName(schemaName, tableName + "$partitions");
    }

    @Override
    public ConnectorViewDefinition getViewDefinition()
    {
        String viewSql = """
                SELECT %s SUM(record_count) AS record_count, COUNT(*) AS file_count, SUM(file_size_in_bytes) AS total_size%s
                FROM "%s"."%s"."%s$files"%s
                """.formatted(
                hasPartitionColumn ? "partition," : "",
                hasDataColumn ? ", " + dataAggregationSql : "",
                catalogName,
                schemaName,
                tableName,
                hasPartitionColumn ? "\nGROUP BY 1" : "");

        return new ConnectorViewDefinition(
                viewSql,
                Optional.of(catalogName),
                Optional.of(schemaName),
                viewColumns,
                Optional.empty(),
                Optional.empty(),
                true,
                ImmutableList.of());
    }

    public static String buildDataAggregation(TypeManager typeManager, List<NestedField> nonPartitionColumns)
    {
        ImmutableList.Builder<String> rowValues = ImmutableList.builder();
        ImmutableList.Builder<String> rowTypes = ImmutableList.builder();

        for (NestedField column : nonPartitionColumns) {
            String trinoTypeDisplayName = toTrinoType(column.type(), typeManager).getDisplayName();
            rowValues.add(buildColumnAggregation(column.fieldId(), trinoTypeDisplayName));
            rowTypes.add(buildColumnRowType(column.name(), trinoTypeDisplayName));
        }

        return "CAST(ROW(%s) AS ROW(%s)) AS data".formatted(COMMA_JOINER.join(rowValues.build()), COMMA_JOINER.join(rowTypes.build()));
    }

    private static String buildColumnAggregation(int fieldId, String trinoTypeDisplayName)
    {
        String min = getTypedProjection("MIN", "lower_bounds", trinoTypeDisplayName, fieldId);
        String max = getTypedProjection("MAX", "upper_bounds", trinoTypeDisplayName, fieldId);
        String nullCount = "SUM(element_at(null_value_counts, %d))".formatted(fieldId);
        String nanCount = "SUM(element_at(nan_value_counts, %d))".formatted(fieldId);

        // we need this case to ensure that it is compatible with the current $partitions implementation
        return """
                CASE
                    WHEN %1$s IS NULL AND %2$s IS NULL AND %3$s IS NULL AND %4$s IS NULL THEN NULL
                    ELSE ROW(%1$s, %2$s, %3$s, %4$s)
                END
                """
                .formatted(min, max, nullCount, nanCount);
    }

    private static String buildColumnRowType(String columnName, String trinoTypeDisplayName)
    {
        return "%s ROW(min %2$s, max %2$s, null_count BIGINT, nan_count BIGINT)"
                .formatted("\"" + columnName + "\"", trinoTypeDisplayName);
    }

    private static String getTypedProjection(String aggregationName, String fieldName, String trinoTypeDisplayName, int fieldId)
    {
        String normalizedTrinoType = trinoTypeDisplayName.toLowerCase(Locale.ROOT);

        if (normalizedTrinoType.startsWith("timestamp")) {
            return "%s(from_iso8601_timestamp_nanos(element_at(%s, %d)))".formatted(aggregationName, fieldName, fieldId);
        }
        if (normalizedTrinoType.equals("varbinary")) {
            return "%s(from_base64(element_at(%s, %d)))".formatted(aggregationName, fieldName, fieldId);
        }
        if (normalizedTrinoType.startsWith("varchar") || normalizedTrinoType.equals("uuid") || normalizedTrinoType.equals("boolean")) {
            return "%s(element_at(%s, %d))".formatted(aggregationName, fieldName, fieldId);
        }
        return "%s(CAST(element_at(%s, %d) AS %s))".formatted(aggregationName, fieldName, fieldId, trinoTypeDisplayName);
    }

    private static Optional<RowType> getMetricsColumnType(TypeManager typeManager, List<NestedField> columns)
    {
        List<RowType.Field> metricColumns = columns.stream()
                .map(column -> {
                    Type trinoType = toTrinoType(column.type(), typeManager);
                    return RowType.field(
                            column.name(),
                            RowType.from(ImmutableList.of(
                                    new RowType.Field(Optional.of("min"), trinoType),
                                    new RowType.Field(Optional.of("max"), trinoType),
                                    new RowType.Field(Optional.of("null_count"), BIGINT),
                                    new RowType.Field(Optional.of("nan_count"), BIGINT))));
                })
                .collect(toImmutableList());

        if (metricColumns.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(RowType.from(metricColumns));
    }
}
