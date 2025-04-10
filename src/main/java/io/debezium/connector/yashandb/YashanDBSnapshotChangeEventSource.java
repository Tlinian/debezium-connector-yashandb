/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import com.sics.ystream.result.Position;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.MainConnectionProvidingConnectionFactory;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.schema.SchemaChangeEvent;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Clock;
import io.debezium.util.Strings;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * A {@link StreamingChangeEventSource} for YashanDB.
 *
 * @author Gunnar Morling
 */
public class YashanDBSnapshotChangeEventSource extends RelationalSnapshotChangeEventSource<YashanDBPartition, YashanDBOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YashanDBSnapshotChangeEventSource.class);

    private final YashanDBConnectorConfig connectorConfig;
    private final YashanDBConnection jdbcConnection;
    private final YashanDBDatabaseSchema databaseSchema;

    public YashanDBSnapshotChangeEventSource(YashanDBConnectorConfig connectorConfig, MainConnectionProvidingConnectionFactory<YashanDBConnection> connectionFactory,
                                             YashanDBDatabaseSchema schema, EventDispatcher<YashanDBPartition, TableId> dispatcher, Clock clock,
                                             SnapshotProgressListener<YashanDBPartition> snapshotProgressListener,
                                             NotificationService<YashanDBPartition, YashanDBOffsetContext> notificationService) {
        super(connectorConfig, connectionFactory, schema, dispatcher, clock, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.jdbcConnection = connectionFactory.mainConnection();
        this.databaseSchema = schema;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(YashanDBPartition yashanDBPartition, YashanDBOffsetContext previousOffset) {
        boolean snapshotSchema = true;
        List<String> dataCollectionsToBeSnapshotted = this.connectorConfig.getDataCollectionsToBeSnapshotted();
        Map<String, String> snapshotSelectOverridesByTable = (Map) this.connectorConfig.getSnapshotSelectOverridesByTable().entrySet().stream()
                .collect(Collectors.toMap((e) -> {
                    return ((DataCollectionId) e.getKey()).identifier();
                }, Map.Entry::getValue));
        boolean snapshotData;
        if (YashanDBConnectorConfig.SnapshotMode.ALWAYS == this.connectorConfig.getSnapshotMode()) {
            LOGGER.info("Snapshot mode is set to ALWAYS, not checking exiting offset.");
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        }
        else if (previousOffset != null && !previousOffset.isSnapshotRunning()) {
            LOGGER.info("The previous offset has been found.");
            snapshotSchema = this.databaseSchema.isStorageInitializationExecuted();
            snapshotData = false;
        }
        else {
            LOGGER.info("No previous offset has been found.");
            snapshotData = this.connectorConfig.getSnapshotMode().includeData();
        }

        if (snapshotData && snapshotSchema) {
            LOGGER.info("According to the connector configuration both schema and data will be snapshot.");
        }
        else if (snapshotSchema) {
            LOGGER.info("According to the connector configuration only schema will be snapshot.");
        }

        return new SnapshottingTask(snapshotSchema, snapshotData, dataCollectionsToBeSnapshotted, snapshotSelectOverridesByTable, false);
    }

    @Override
    protected SnapshotContext<YashanDBPartition, YashanDBOffsetContext> prepare(YashanDBPartition partition)
            throws Exception {
        return new YashanDBSnapshotContext(partition, connectorConfig.getDatabaseName());
    }

    @Override
    protected Set<TableId> getAllTableIds(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx)
            throws Exception {
        return jdbcConnection.getAllTableIds(ctx.catalogName);
        // this very slow approach(commented out), it took 30 minutes on an instance with 600 tables
        // return jdbcConnection.readTableNames(ctx.catalogName, null, null, new String[] {"TABLE"} );
    }

    @Override
    protected void lockTablesForSchemaSnapshot(ChangeEventSourceContext sourceContext,
                                               RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext)
            throws SQLException, InterruptedException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            LOGGER.info(" yashandb not support lock table");
            // ((YashanDBOffsetContext) snapshotContext).preSchemaSnapshotSavepoint = jdbcConnection.connection().setSavepoint("dbz_schema_snapshot");
            //
            // try (Statement statement = jdbcConnection.connection().createStatement()) {
            // for (TableId tableId : snapshotContext.capturedTables) {
            // if (!sourceContext.isRunning()) {
            // throw new InterruptedException("Interrupted while locking table " + tableId);
            // }
            //
            // LOGGER.debug("Locking table {}", tableId);
            // statement.execute("LOCK TABLE " + quote(tableId) + " IN ROW SHARE MODE");
            // }
            // }
        }
        else {
            LOGGER.info("Schema locking was disabled in connector configuration");
        }
    }

    @Override
    protected void releaseSchemaSnapshotLocks(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext)
            throws SQLException {
        if (connectorConfig.getSnapshotLockingMode().usesLocking()) {
            // jdbcConnection.connection().rollback(((OracleSnapshotContext) snapshotContext).preSchemaSnapshotSavepoint);
        }
    }

    @Override
    protected void determineSnapshotOffset(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> ctx,
                                           YashanDBOffsetContext previousOffset)
            throws Exception {
        // Support the existence of the case when the previous offset.
        // e.g., schema_only_recovery snapshot mode
        if (connectorConfig.getSnapshotMode() != YashanDBConnectorConfig.SnapshotMode.ALWAYS && previousOffset != null) {
            ctx.offset = previousOffset;
            tryStartingSnapshot(ctx);
            return;
        }

        ctx.offset = connectorConfig.getAdapter().determineSnapshotOffset(ctx, connectorConfig, jdbcConnection);
    }

    @Override
    protected void readTableStructure(ChangeEventSourceContext sourceContext, RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                      YashanDBOffsetContext yashanDBOffsetContext, SnapshottingTask snapshottingTask)
            throws Exception {
        Set<TableId> capturedSchemaTables;
        if (databaseSchema.storeOnlyCapturedTables()) {
            capturedSchemaTables = snapshotContext.capturedTables;
            LOGGER.info("Only captured tables schema should be captured, capturing: {}", capturedSchemaTables);
        }
        else {
            capturedSchemaTables = snapshotContext.capturedSchemaTables;
            LOGGER.info("All eligible tables schema should be captured, capturing: {}", capturedSchemaTables);
        }

        Set<String> schemas = capturedSchemaTables.stream().map(TableId::schema).collect(Collectors.toSet());

        // reading info only for the schemas we're interested in as per the set of captured tables;
        // while the passed table name filter alone would skip all non-included tables, reading the schema
        // would take much longer that way
        // however, for users interested only in captured tables, we need to pass also table filter
        final Tables.TableFilter tableFilter = connectorConfig.storeOnlyCapturedTables() ? connectorConfig.getTableFilters().dataCollectionFilter() : null;
        for (String schema : schemas) {
            if (!sourceContext.isRunning()) {
                throw new InterruptedException("Interrupted while reading structure of schema " + schema);
            }
            jdbcConnection.readSchema(
                    snapshotContext.tables,
                    null,
                    schema,
                    tableFilter,
                    null,
                    false);
        }
    }

    @Override
    protected String enhanceOverriddenSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                             String overriddenSelect, TableId tableId) {
        String snapshotOffset = (String) snapshotContext.offset.getOffset().get(SourceInfo.SCN_KEY);
        String token = connectorConfig.getTokenToReplaceInSnapshotPredicate();
        if (token != null) {
            return overriddenSelect.replaceAll(token, " AS OF SCN " + snapshotOffset);
        }
        return overriddenSelect;
    }

    @Override
    protected Collection<TableId> getTablesForSchemaChange(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) {
        return snapshotContext.capturedSchemaTables;
    }

    @Override
    protected SchemaChangeEvent getCreateTableEvent(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                    Table table)
            throws SQLException {
        return SchemaChangeEvent.ofCreate(
                snapshotContext.partition,
                snapshotContext.offset,
                snapshotContext.catalogName,
                table.id().schema(),
                jdbcConnection.getTableMetadataDdl(table.id()),
                table,
                true);
    }

    @Override
    protected Instant getSnapshotSourceTimestamp(JdbcConnection jdbcConnection, YashanDBOffsetContext offset, TableId tableId) {
        try {
            Optional<Instant> snapshotTs = ((YashanDBConnection) jdbcConnection).getScnToTimestamp(offset.getScn());
            if (snapshotTs.isEmpty()) {
                throw new ConnectException("Failed reading SCN timestamp from source database");
            }

            return snapshotTs.get();
        }
        catch (SQLException e) {
            throw new ConnectException("Failed reading SCN timestamp from source database", e);
        }
    }

    /**
     * Generate a valid Oracle query string for the specified table and columns
     *
     * @param tableId the table to generate a query for
     * @return a valid query string
     */
    @Override
    protected Optional<String> getSnapshotSelect(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext,
                                                 TableId tableId, List<String> columns) {
        final YashanDBOffsetContext offset = snapshotContext.offset;
        final String snapshotOffset = offset.getScn().toString();
        String snapshotSelectColumns = columns.stream()
                .collect(Collectors.joining(", "));
        assert snapshotOffset != null;
        return Optional.of(String.format("SELECT %s FROM %s AS OF SCN %s", snapshotSelectColumns, quote(tableId), snapshotOffset));
    }

    @Override
    protected List<Pattern> getSignalDataCollectionPattern(String signalingDataCollection) {
        // Oracle expects this value to be supplied using "<database>.<schema>.<table>"; however the
        // TableIdMapper used by the connector uses only "<schema>.<table>". This primarily targets
        // a fix for this specific use case as a much larger refactor is likely necessary long term.
        final TableId tableId = TableId.parse(signalingDataCollection);
        return Strings.listOfRegex(tableId.schema() + "." + tableId.table(), Pattern.CASE_INSENSITIVE);
    }

    @Override
    public void close() {
    }

    private String quote(TableId tableId) {
        return new TableId(null, tableId.schema(), tableId.table()).toDoubleQuotedString();
    }

    /**
     * Mutable context which is populated in the course of snapshotting.
     */
    private static class YashanDBSnapshotContext extends RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> {

        private Savepoint preSchemaSnapshotSavepoint;

        YashanDBSnapshotContext(YashanDBPartition partition, String catalogName) throws SQLException {
            super(partition, catalogName);
        }
    }

    @Override
    protected YashanDBOffsetContext copyOffset(RelationalSnapshotContext<YashanDBPartition, YashanDBOffsetContext> snapshotContext) {
        return load(snapshotContext.offset.getOffset());
    }

    public YashanDBOffsetContext load(Map<String, ?> offset) {
        boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY));
        boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(YashanDBOffsetContext.SNAPSHOT_COMPLETED_KEY));
        boolean isCreateServer = Boolean.TRUE.equals(offset.get(YashanDBOffsetContext.YSTREAM_SERVER_CREATE));
        Scn scn = YashanDBOffsetContext.getScnFromOffsetMapByKey(offset, SourceInfo.SCN_KEY);
        CommitScn commitScn = CommitScn.load(offset);
        Map<String, Scn> snapshotPendingTransactions = YashanDBOffsetContext.loadSnapshotPendingTransactions(offset);
        Scn snapshotScn = YashanDBOffsetContext.loadSnapshotScn(offset);
        Scn ystreamStartScn = YashanDBOffsetContext.loadYstreamStartScn(offset);
        Position recoverPosition = YashanDBOffsetContext.loadRecoverPosition(offset);
        return new YashanDBOffsetContext(connectorConfig, scn, commitScn, snapshotScn, ystreamStartScn, recoverPosition, snapshotPendingTransactions, snapshot,
                snapshotCompleted,
                TransactionContext.load(offset),
                SignalBasedIncrementalSnapshotContext.load(offset), isCreateServer);
    }
}
