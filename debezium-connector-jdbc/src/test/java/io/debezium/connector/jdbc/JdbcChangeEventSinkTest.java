/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.hibernate.StatelessSession;
import org.hibernate.dialect.DatabaseVersion;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;

/**
 * Unit tests for the multi-value statement gating in {@link JdbcChangeEventSink}.
 */
@Tag("UnitTests")
public class JdbcChangeEventSinkTest {

    @Test
    public void testMultiValueDisabledByConfiguration() {
        final Fixture fixture = new Fixture(false, true, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of("id"), Set.of("name")))).isFalse();
    }

    @Test
    public void testMultiValueDisabledForUnsupportedDialect() {
        final Fixture fixture = new Fixture(true, false, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of("id"), Set.of("name")))).isFalse();
    }

    @Test
    public void testMultiValueEnabledForInsertMode() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of("id"), Set.of("name")))).isTrue();
    }

    @Test
    public void testMultiValueDisabledForUpdateMode() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.UPDATE);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of("id"), Set.of("name")))).isFalse();
    }

    @Test
    public void testMultiValueEnabledForKeyedDelete() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(true, Set.of("id"), Set.of()))).isTrue();
    }

    @Test
    public void testMultiValueDisabledForKeylessDelete() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(true, Set.of(), Set.of()))).isFalse();
    }

    @Test
    public void testMultiValueDisabledForKeylessUpsert() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.UPSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of(), Set.of("name")))).isFalse();
    }

    @Test
    public void testMultiValueEnabledForKeyedUpsert() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.UPSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of("id"), Set.of("name")))).isTrue();
    }

    @Test
    public void testMultiValueDisabledForRecordWithoutBindableFields() {
        final Fixture fixture = new Fixture(true, true, JdbcSinkConnectorConfig.InsertMode.INSERT);
        assertThat(fixture.sink().useMultiValueStatements(record(false, Set.of(), Set.of()))).isFalse();
    }

    private static class Fixture {
        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final DatabaseDialect dialect = mock(DatabaseDialect.class);

        Fixture(boolean useMultiValueStatements, boolean dialectSupportsMultiValue, JdbcSinkConnectorConfig.InsertMode insertMode) {
            when(config.isUseMultiValueStatements()).thenReturn(useMultiValueStatements);
            when(config.getInsertMode()).thenReturn(insertMode);
            when(dialect.supportsMultiValueStatements()).thenReturn(dialectSupportsMultiValue);
            when(dialect.getVersion()).thenReturn(DatabaseVersion.make(1, 0));
        }

        JdbcChangeEventSink sink() {
            return new JdbcChangeEventSink(config, mock(StatelessSession.class), dialect, mock(RecordWriter.class), null);
        }
    }

    private static JdbcSinkRecord record(boolean isDelete, Set<String> keyFieldNames, Set<String> nonKeyFieldNames) {
        final JdbcSinkRecord record = mock(JdbcSinkRecord.class);
        when(record.isDelete()).thenReturn(isDelete);
        when(record.keyFieldNames()).thenReturn(new LinkedHashSet<>(List.copyOf(keyFieldNames)));
        when(record.nonKeyFieldNames()).thenReturn(new LinkedHashSet<>(List.copyOf(nonKeyFieldNames)));
        return record;
    }
}
