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
import io.debezium.relational.Table;
import io.debezium.util.Clock;

import java.util.Map;

/**
 * Emits change data based on a single {@link YStreamDmlRecord} event.
 *
 * @author Gunnar Morling
 */
public class YStreamChangeRecordEmitter extends BaseChangeRecordEmitter<YStreamDmlRecord> {

    private final YStreamDmlRecord record;

    public YStreamChangeRecordEmitter(YashanDBConnectorConfig connectorConfig, YashanDBPartition partition, OffsetContext offset, YStreamDmlRecord record,
                                      Table table, YashanDBDatabaseSchema schema, Clock clock, Object[] newValues, Object[] oldValues) {
        super(connectorConfig, partition, offset, schema, table, clock, oldValues,
                newValues);
        this.record = record;
    }

    @Override
    public Operation getOperation() {
        switch (record.getYstreamDml().getDmlType()) {
            case INSERT:
                return Operation.CREATE;
            case DELETE:
                return Operation.DELETE;
            case UPDATE:
                return Operation.UPDATE;
            // TODO : return Operation.TRUNCATE;
            case CHUNK:
            default:
                throw new IllegalArgumentException("Received event of unexpected command type: " + record);
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
            for (YstreamColumn columnValue : columnValues.getColumns()) {
                int index = table.columnWithName(columnValue.getColumn().getColumnName()).position() - 1;
                try {
                    values[index] = columnValue.getData();
                }
                catch (YstreamSqlException e) {
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
