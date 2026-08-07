/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Stopwatch;

/**
 * Effectively writes the batches using Hibernate {@link Work}
 *
 * @author Mario Fiore Vitale
 */
public class RecordWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecordWriter.class);
    private final SharedSessionContract session;
    private final QueryBinderResolver queryBinderResolver;
    private final JdbcSinkConnectorConfig config;
    private final DatabaseDialect dialect;

    public RecordWriter(SharedSessionContract session, QueryBinderResolver queryBinderResolver, JdbcSinkConnectorConfig config, DatabaseDialect dialect) {
        this.session = session;
        this.queryBinderResolver = queryBinderResolver;
        this.config = config;
        this.dialect = dialect;
    }

    public void write(List<JdbcSinkRecord> records, String sqlStatement) {
        executeInTransaction("write", processBatch(records, sqlStatement));
    }

    public void writeMultiValue(List<JdbcSinkRecord> records, TableDescriptor table) {
        if (records.isEmpty()) {
            return;
        }
        executeInTransaction("multi-value write", processMultiValue(records, table));
    }

    private void executeInTransaction(String description, Work work) {
        Stopwatch writeStopwatch = Stopwatch.reusable();
        writeStopwatch.start();
        final Transaction transaction = session.beginTransaction();

        try {
            session.doWork(work);
            transaction.commit();
        }
        catch (Exception e) {
            transaction.rollback();
            throw e;
        }
        writeStopwatch.stop();
        LOGGER.trace("[PERF] Total {} execution time {}", description, writeStopwatch.durations());
    }

    private Work processMultiValue(List<JdbcSinkRecord> records, TableDescriptor table) {
        return conn -> {
            final JdbcSinkRecord first = records.get(0);
            final boolean isDelete = first.isDelete();
            // A flushed buffer is schema-homogeneous (RecordBuffer and ReducedRecordBuffer flush on any
            // key or value schema change), so the first record's field counts hold for every record.
            final int paramsPerRow = isDelete
                    ? first.keyFieldNames().size()
                    : first.keyFieldNames().size() + first.nonKeyFieldNames().size();
            final int rowsPerStatement = computeRowsPerStatement(paramsPerRow, dialect.getMaxBindParameters(), records.size());

            // Deletes may affect fewer rows than records, and a key-only upsert MERGE carries no
            // WHEN MATCHED clause, so re-delivered keys legitimately affect zero rows there; only
            // inserts and updating upserts must apply exactly one row per record.
            final boolean expectOneRowPerRecord = !isDelete
                    && !(config.getInsertMode() == JdbcSinkConnectorConfig.InsertMode.UPSERT && first.nonKeyFieldNames().isEmpty());

            // Full chunks share one SQL shape and only the final remainder differs, so a flush prepares
            // at most two distinct statements; remainder sizes vary across flushes and may churn the
            // provider's statement cache, which is acceptable.
            for (int from = 0; from < records.size(); from += rowsPerStatement) {
                final List<JdbcSinkRecord> chunk = records.subList(from, Math.min(from + rowsPerStatement, records.size()));
                final String sql = getMultiValueStatement(table, chunk.get(0), chunk.size(), isDelete);
                try (PreparedStatement prepareStatement = conn.prepareStatement(sql)) {
                    QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
                    int index = 1;
                    for (JdbcSinkRecord record : chunk) {
                        final int nextIndex = bindValues(record, queryBinder, index);
                        // The SQL emits one placeholder per field, but a type may bind more or fewer values
                        // per field, which would silently misalign every subsequent row; fail on the first
                        // offending record, before the misalignment reaches the driver.
                        if (nextIndex != index + paramsPerRow) {
                            throw new ConnectException("Bound " + (nextIndex - index) + " parameters for a record but the multi-value statement expects "
                                    + paramsPerRow + " per row for table '" + table.getId().toFullIdentiferString() + "'");
                        }
                        index = nextIndex;
                    }

                    final int affectedRows;
                    try {
                        affectedRows = prepareStatement.executeUpdate();
                    }
                    catch (SQLException e) {
                        // A multi-value statement fails as a whole, without the per-record detail of a
                        // BatchUpdateException; log the chunk's coordinates so the failure can be traced.
                        // Records may span topics and are not necessarily offset-ordered (reduced buffers
                        // iterate in hash order), so only the first record's coordinates are reported.
                        final JdbcSinkRecord firstOfChunk = chunk.get(0);
                        LOGGER.error("Multi-value statement failed for table '{}' for a chunk of {} records, the first from topic '{}' partition {} offset {}",
                                table.getId().toFullIdentiferString(), chunk.size(), firstOfChunk.topicName(), firstOfChunk.partition(), firstOfChunk.offset());
                        throw e;
                    }
                    if (expectOneRowPerRecord && affectedRows != chunk.size()) {
                        LOGGER.warn("Multi-value statement affected {} rows but {} records were bound for table '{}'",
                                affectedRows, chunk.size(), table.getId().toFullIdentiferString());
                    }
                    LOGGER.trace("Multi-value statement affected {} rows for {} records", affectedRows, chunk.size());
                }
            }
        };
    }

    private String getMultiValueStatement(TableDescriptor table, JdbcSinkRecord record, int rowCount, boolean isDelete) {
        if (isDelete) {
            return dialect.getMultiValueDeleteStatement(table, record, rowCount);
        }
        // UPDATE mode never reaches the multi-value path
        if (config.getInsertMode() == JdbcSinkConnectorConfig.InsertMode.UPSERT) {
            return dialect.getMultiValueUpsertStatement(table, record, rowCount);
        }
        return dialect.getMultiValueInsertStatement(table, record, rowCount);
    }

    static int computeRowsPerStatement(int paramsPerRow, int maxBindParameters, int totalRows) {
        if (paramsPerRow <= 0) {
            return totalRows;
        }
        return Math.max(1, Math.min(totalRows, maxBindParameters / paramsPerRow));
    }

    private Work processBatch(List<JdbcSinkRecord> records, String sqlStatement) {
        return conn -> {
            try (PreparedStatement prepareStatement = conn.prepareStatement(sqlStatement)) {

                QueryBinder queryBinder = queryBinderResolver.resolve(prepareStatement);
                Stopwatch allbindStopwatch = Stopwatch.reusable();
                allbindStopwatch.start();
                for (JdbcSinkRecord record : records) {

                    Stopwatch singlebindStopwatch = Stopwatch.reusable();
                    singlebindStopwatch.start();
                    bindValues(record, queryBinder, 1);
                    singlebindStopwatch.stop();

                    Stopwatch addBatchStopwatch = Stopwatch.reusable();
                    addBatchStopwatch.start();
                    prepareStatement.addBatch();
                    addBatchStopwatch.stop();

                    LOGGER.trace("[PERF] Bind single record execution time {}", singlebindStopwatch.durations());
                    LOGGER.trace("[PERF] Add batch execution time {}", addBatchStopwatch.durations());
                }
                allbindStopwatch.stop();
                LOGGER.trace("[PERF] All records bind execution time {}", allbindStopwatch.durations());

                Stopwatch executeStopwatch = Stopwatch.reusable();
                executeStopwatch.start();
                int[] batchResult = prepareStatement.executeBatch();
                executeStopwatch.stop();
                for (int updateCount : batchResult) {
                    if (updateCount == Statement.EXECUTE_FAILED) {
                        throw new BatchUpdateException("Execution failed for part of the batch", batchResult);
                    }
                }
                LOGGER.trace("[PERF] Execute batch execution time {}", executeStopwatch.durations());
            }
        };
    }

    private int bindValues(JdbcSinkRecord record, QueryBinder queryBinder, int startIndex) {
        int index = startIndex;
        if (record.isDelete()) {
            return bindKeyValuesToQuery(record, queryBinder, index);
        }

        switch (config.getInsertMode()) {
            case INSERT:
            case UPSERT:
                index = bindKeyValuesToQuery(record, queryBinder, index);
                index = bindNonKeyValuesToQuery(record, queryBinder, index);
                break;
            case UPDATE:
                index = bindNonKeyValuesToQuery(record, queryBinder, index);
                index = bindKeyValuesToQuery(record, queryBinder, index);
                break;
        }
        return index;
    }

    private int bindKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        final Struct keySource = record.filteredKey();
        if (keySource != null) {
            index = bindFieldValuesToQuery(record, query, index, keySource, record.keyFieldNames());
        }
        return index;
    }

    private int bindNonKeyValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index) {
        return bindFieldValuesToQuery(record, query, index, record.getPayload(), record.nonKeyFieldNames());
    }

    private int bindFieldValuesToQuery(JdbcSinkRecord record, QueryBinder query, int index, Struct source, Set<String> fieldNames) {
        for (String fieldName : fieldNames) {
            final JdbcFieldDescriptor field = record.jdbcFields().get(fieldName);

            Object value;
            if (field.getSchema().isOptional()) {
                value = source.getWithoutDefault(fieldName);
            }
            else {
                value = source.get(fieldName);
            }
            List<ValueBindDescriptor> boundValues = dialect.bindValue(field, index, value);

            boundValues.forEach(query::bind);
            index += boundValues.size();
        }
        return index;
    }
}
