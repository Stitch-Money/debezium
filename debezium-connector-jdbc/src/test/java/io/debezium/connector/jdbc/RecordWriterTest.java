/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.hibernate.SharedSessionContract;
import org.hibernate.Transaction;
import org.hibernate.jdbc.Work;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.field.JdbcFieldDescriptor;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.metadata.CollectionId;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * Unit tests for the {@link RecordWriter} class.
 */
@Tag("UnitTests")
public class RecordWriterTest {

    @Test
    public void testRowsPerStatementCappedByTotalRows() {
        assertThat(RecordWriter.computeRowsPerStatement(3, 16_384, 500)).isEqualTo(500);
    }

    @Test
    public void testRowsPerStatementCappedByMaxBindParameters() {
        assertThat(RecordWriter.computeRowsPerStatement(40, 16_384, 500)).isEqualTo(409);
    }

    @Test
    public void testRowsPerStatementIsAtLeastOneForVeryWideRows() {
        assertThat(RecordWriter.computeRowsPerStatement(20_000, 16_384, 500)).isEqualTo(1);
    }

    @Test
    public void testRowsPerStatementDefensiveOnZeroWidthRows() {
        assertThat(RecordWriter.computeRowsPerStatement(0, 16_384, 500)).isEqualTo(500);
    }

    @Test
    public void testMultiValueWriteChunksRecordsAndBindsSequentially() throws Exception {
        final Fixture fixture = new Fixture(4);
        when(fixture.statements[0].executeUpdate()).thenReturn(2);
        when(fixture.statements[1].executeUpdate()).thenReturn(2);
        when(fixture.statements[2].executeUpdate()).thenReturn(1);

        final List<JdbcSinkRecord> records = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            records.add(insertRecord(i));
        }

        fixture.writer().writeMultiValue(records, fixture.table);

        // Two parameters per row against a limit of four bind parameters yields chunks of 2, 2 and 1
        verify(fixture.connection, times(2)).prepareStatement("INSERT-2");
        verify(fixture.connection).prepareStatement("INSERT-1");

        // Bind indexes continue across the records of a chunk and restart at one for the next chunk
        final InOrder firstChunk = inOrder(fixture.statements[0]);
        firstChunk.verify(fixture.statements[0]).setObject(1, 0);
        firstChunk.verify(fixture.statements[0]).setObject(2, "name-0");
        firstChunk.verify(fixture.statements[0]).setObject(3, 1);
        firstChunk.verify(fixture.statements[0]).setObject(4, "name-1");
        firstChunk.verify(fixture.statements[0]).executeUpdate();
        firstChunk.verify(fixture.statements[0]).close();

        verify(fixture.statements[2]).setObject(1, 4);
        verify(fixture.statements[2]).setObject(2, "name-4");
        verify(fixture.statements[2], never()).setObject(eq(3), any());

        verify(fixture.transaction).commit();
    }

    @Test
    public void testMultiValueWriteFailsFastWhenBindCountsMisalign() throws Exception {
        final Fixture fixture = new Fixture(16_384);
        // Bind two values per field while the SQL only emits one placeholder per field
        when(fixture.dialect.bindValue(any(), anyInt(), any()))
                .thenAnswer(invocation -> {
                    final int index = invocation.getArgument(1, Integer.class);
                    return List.of(new ValueBindDescriptor(index, invocation.getArgument(2)),
                            new ValueBindDescriptor(index + 1, invocation.getArgument(2)));
                });

        final List<JdbcSinkRecord> records = List.of(insertRecord(0), insertRecord(1));

        assertThatThrownBy(() -> fixture.writer().writeMultiValue(records, fixture.table))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Bound 4 parameters for a record")
                .hasMessageContaining("expects 2 per row");

        verify(fixture.statements[0], never()).executeUpdate();
        verify(fixture.transaction).rollback();
    }

    @Test
    public void testMultiValueWriteToleratesEmptyRecordList() throws Exception {
        final Fixture fixture = new Fixture(16_384);

        fixture.writer().writeMultiValue(List.of(), fixture.table);

        verify(fixture.session, never()).beginTransaction();
        verify(fixture.connection, never()).prepareStatement(anyString());
    }

    @Test
    public void testMultiValueWriteUsesDeleteStatementForDeleteRecords() throws Exception {
        final Fixture fixture = new Fixture(2);
        when(fixture.statements[0].executeUpdate()).thenReturn(2);
        when(fixture.statements[1].executeUpdate()).thenReturn(1);

        // One key parameter per row against a limit of two bind parameters yields chunks of 2 and 1
        final List<JdbcSinkRecord> records = List.of(deleteRecord(0), deleteRecord(1), deleteRecord(2));

        fixture.writer().writeMultiValue(records, fixture.table);

        verify(fixture.connection).prepareStatement("DELETE-2");
        verify(fixture.connection).prepareStatement("DELETE-1");
        verify(fixture.dialect, never()).getMultiValueInsertStatement(any(), any(), anyInt());
        verify(fixture.dialect, never()).getMultiValueUpsertStatement(any(), any(), anyInt());

        // Only key fields are bound for deletes
        final InOrder firstChunk = inOrder(fixture.statements[0]);
        firstChunk.verify(fixture.statements[0]).setObject(1, 0);
        firstChunk.verify(fixture.statements[0]).setObject(2, 1);
        firstChunk.verify(fixture.statements[0]).executeUpdate();
        verify(fixture.statements[1]).setObject(1, 2);
        verify(fixture.statements[1], never()).setObject(eq(2), any());

        verify(fixture.transaction).commit();
    }

    @Test
    public void testMultiValueWriteUsesUpsertStatementInUpsertMode() throws Exception {
        final Fixture fixture = new Fixture(16_384);
        when(fixture.config.getInsertMode()).thenReturn(JdbcSinkConnectorConfig.InsertMode.UPSERT);
        when(fixture.statements[0].executeUpdate()).thenReturn(2);

        final List<JdbcSinkRecord> records = List.of(insertRecord(0), insertRecord(1));

        fixture.writer().writeMultiValue(records, fixture.table);

        verify(fixture.connection).prepareStatement("UPSERT-2");
        verify(fixture.dialect, never()).getMultiValueInsertStatement(any(), any(), anyInt());
        verify(fixture.dialect, never()).getMultiValueDeleteStatement(any(), any(), anyInt());
        verify(fixture.transaction).commit();
    }

    @Test
    public void testMultiValueWriteDoesNotWarnWhenKeyOnlyUpsertAffectsFewerRows() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RecordWriter.class);
        final Fixture fixture = new Fixture(16_384);
        when(fixture.config.getInsertMode()).thenReturn(JdbcSinkConnectorConfig.InsertMode.UPSERT);
        // A key-only MERGE has no WHEN MATCHED clause, so re-delivered keys affect zero rows
        when(fixture.statements[0].executeUpdate()).thenReturn(0);

        final List<JdbcSinkRecord> records = List.of(keyOnlyRecord(0), keyOnlyRecord(1));

        fixture.writer().writeMultiValue(records, fixture.table);

        assertThat(logInterceptor.containsWarnMessage("Multi-value statement affected")).isFalse();
        verify(fixture.transaction).commit();
    }

    @Test
    public void testMultiValueWriteWarnsWhenInsertAffectsFewerRows() throws Exception {
        final LogInterceptor logInterceptor = new LogInterceptor(RecordWriter.class);
        final Fixture fixture = new Fixture(16_384);
        when(fixture.statements[0].executeUpdate()).thenReturn(1);

        final List<JdbcSinkRecord> records = List.of(insertRecord(0), insertRecord(1));

        fixture.writer().writeMultiValue(records, fixture.table);

        assertThat(logInterceptor.containsWarnMessage("Multi-value statement affected 1 rows but 2 records were bound")).isTrue();
        verify(fixture.transaction).commit();
    }

    @Test
    public void testMultiValueWriteRollsBackWhenExecuteFails() throws Exception {
        final Fixture fixture = new Fixture(16_384);
        when(fixture.statements[0].executeUpdate()).thenThrow(new SQLException("constraint violation"));

        final List<JdbcSinkRecord> records = List.of(insertRecord(0), insertRecord(1));

        assertThatThrownBy(() -> fixture.writer().writeMultiValue(records, fixture.table))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("constraint violation");

        verify(fixture.statements[0]).close();
        verify(fixture.transaction).rollback();
        verify(fixture.transaction, never()).commit();
    }

    /**
     * A {@link RecordWriter} wired against a mocked Hibernate session, connection and dialect. The
     * dialect answers {@code INSERT-<rowCount>}, {@code UPSERT-<rowCount>} and {@code DELETE-<rowCount>}
     * for multi-value SQL and binds one value per field unless re-stubbed.
     */
    private static class Fixture {
        final SharedSessionContract session = mock(SharedSessionContract.class);
        final Transaction transaction = mock(Transaction.class);
        final Connection connection = mock(Connection.class);
        final PreparedStatement[] statements = { mock(PreparedStatement.class), mock(PreparedStatement.class), mock(PreparedStatement.class) };
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final DatabaseDialect dialect = mock(DatabaseDialect.class);
        final TableDescriptor table = mock(TableDescriptor.class);

        Fixture(int maxBindParameters) throws Exception {
            when(session.beginTransaction()).thenReturn(transaction);
            doAnswer(invocation -> {
                invocation.getArgument(0, Work.class).execute(connection);
                return null;
            }).when(session).doWork(any(Work.class));
            when(connection.prepareStatement(anyString())).thenReturn(statements[0], statements[1], statements[2]);
            when(config.getInsertMode()).thenReturn(JdbcSinkConnectorConfig.InsertMode.INSERT);
            when(dialect.getMaxBindParameters()).thenReturn(maxBindParameters);
            when(dialect.getMultiValueInsertStatement(any(), any(), anyInt()))
                    .thenAnswer(invocation -> "INSERT-" + invocation.getArgument(2, Integer.class));
            when(dialect.getMultiValueUpsertStatement(any(), any(), anyInt()))
                    .thenAnswer(invocation -> "UPSERT-" + invocation.getArgument(2, Integer.class));
            when(dialect.getMultiValueDeleteStatement(any(), any(), anyInt()))
                    .thenAnswer(invocation -> "DELETE-" + invocation.getArgument(2, Integer.class));
            when(dialect.bindValue(any(), anyInt(), any()))
                    .thenAnswer(invocation -> List.of(new ValueBindDescriptor(invocation.getArgument(1, Integer.class), invocation.getArgument(2))));
            when(table.getId()).thenReturn(new CollectionId(null, "public", "customers"));
        }

        RecordWriter writer() {
            return new RecordWriter(session, new QueryBinderResolver(), config, dialect);
        }
    }

    private static JdbcSinkRecord insertRecord(int id) {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        final Schema valueSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

        // Build the descriptor map up front; creating stubs while another stubbing is in progress
        // trips Mockito's UnfinishedStubbing detection
        final Map<String, JdbcFieldDescriptor> jdbcFields = Map.of(
                "id", fieldDescriptor(Schema.INT32_SCHEMA),
                "name", fieldDescriptor(Schema.STRING_SCHEMA));

        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(new LinkedHashSet<>(List.of("id")));
        when(record.nonKeyFieldNames()).thenReturn(new LinkedHashSet<>(List.of("name")));
        when(record.filteredKey()).thenReturn(new Struct(keySchema).put("id", id));
        when(record.getPayload()).thenReturn(new Struct(valueSchema).put("name", "name-" + id));
        when(record.jdbcFields()).thenReturn(jdbcFields);
        return record;
    }

    private static JdbcSinkRecord keyOnlyRecord(int id) {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        final Map<String, JdbcFieldDescriptor> jdbcFields = Map.of("id", fieldDescriptor(Schema.INT32_SCHEMA));

        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(false);
        when(record.keyFieldNames()).thenReturn(new LinkedHashSet<>(List.of("id")));
        when(record.nonKeyFieldNames()).thenReturn(new LinkedHashSet<>());
        when(record.filteredKey()).thenReturn(new Struct(keySchema).put("id", id));
        when(record.getPayload()).thenReturn(new Struct(SchemaBuilder.struct().build()));
        when(record.jdbcFields()).thenReturn(jdbcFields);
        return record;
    }

    private static JdbcSinkRecord deleteRecord(int id) {
        final Schema keySchema = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();
        final Map<String, JdbcFieldDescriptor> jdbcFields = Map.of("id", fieldDescriptor(Schema.INT32_SCHEMA));

        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(true);
        when(record.keyFieldNames()).thenReturn(new LinkedHashSet<>(List.of("id")));
        when(record.nonKeyFieldNames()).thenReturn(new LinkedHashSet<>());
        when(record.filteredKey()).thenReturn(new Struct(keySchema).put("id", id));
        when(record.jdbcFields()).thenReturn(jdbcFields);
        return record;
    }

    private static JdbcFieldDescriptor fieldDescriptor(Schema schema) {
        final JdbcFieldDescriptor field = mock(JdbcFieldDescriptor.class);
        when(field.getSchema()).thenReturn(schema);
        return field;
    }
}
