/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import com.sics.ystream.exception.YstreamSqlException;
import com.sics.ystream.result.YstreamColumn;
import com.sics.ystream.result.YstreamColumns;
import io.debezium.connector.yashandb.BaseChangeRecordEmitter;
import io.debezium.connector.yashandb.YashanDBConnectorConfig;
import io.debezium.connector.yashandb.YashanDBDatabaseSchema;
import io.debezium.connector.yashandb.YashanDBPartition;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Emits change data based on a single {@link YStreamDataChangeRecord} event.
 *
 * @author Gunnar Morling
 */
public class YStreamChangeRecordEmitter extends BaseChangeRecordEmitter<YStreamDataChangeRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamChangeRecordEmitter.class);

    private final YStreamDataChangeRecord record;

    public YStreamChangeRecordEmitter(YashanDBConnectorConfig connectorConfig, YashanDBPartition partition, OffsetContext offset, YStreamDataChangeRecord record,
                                      Table table, YashanDBDatabaseSchema schema, Clock clock, Object[] newValues, Object[] oldValues) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues,
                newValues);
        this.record = record;
    }

    @Override
    public Operation getOperation() {
        if (record.isTruncateTable()) {
            return Operation.TRUNCATE;
        } else {
            switch (record.getYstreamDml().getDmlType()) {
                case INSERT:
                    return Operation.CREATE;
                case DELETE:
                    return Operation.DELETE;
                case UPDATE:
                    return Operation.UPDATE;
                case CHUNK:
                default:
                    throw new IllegalArgumentException("Received event of unexpected command type: " + record);
            }
        }
    }

    public static void calculateColumnValues(Object[] oldValues, Object[] newValues) {
        // calculate values
        for (int i = 0; i < oldValues.length; i++) {
            if (oldValues[i] != null && newValues[i] == null) {
                newValues[i] = oldValues[i];
            }
        }
    }

    public static Object[] getColumnValues(Table table, YstreamColumns columnValues, Map<String, Object> chunkValues) {
        Object[] values = new Object[table.columns().size()];
        if (columnValues != null) {
            List<YstreamColumn> columns = columnValues.getColumns().stream()
                    .filter(ystreamColumn -> !ystreamColumn.getColumn().isDeleted()).collect(Collectors.toList());
            for (YstreamColumn columnValue : columns) {
                try {
                    Column column = table.columnWithName(columnValue.getColumn().getColumnName());
                    if (column != null) {
                        int index = column.position() - 1;
                        values[index] = columnValue.getData();
                    }else {
                        throw new IllegalStateException("The schema metadata is different from event,maybe NPE");
                    }
                } catch (YstreamSqlException e) {
                    LOGGER.error("convert data error", e);
                    throw new RuntimeException("convert data error", e);
                }
            }
        }

        // Overlay chunk values into non-chunk value array
        for (Map.Entry<String, Object> entry : chunkValues.entrySet()) {
            final int index = table.columnWithName(entry.getKey()).position() - 1;
            if (values[index] == null) {
                values[index] = entry.getValue();
            }
        }

        return values;
    }
}