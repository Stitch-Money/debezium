/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy;
import io.debezium.metadata.CollectionId;

/**
 * Unit tests for {@link SnowflakeDatabaseDialect} SQL generation and table-existence logic.
 * These tests do not require a real Snowflake connection.
 *
 * @author Marinus Krommenhoek
 */
@Tag("UnitTests")
class SnowflakeDatabaseDialectTest {

    private SnowflakeDatabaseDialect dialect;

    // Reusable mocks for tableExists() tests
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;

    @BeforeEach
    void setUp() throws Exception {
        // Build a minimally-stubbed SessionFactory so GeneralDatabaseDialect's constructor
        // can complete without a real Hibernate bootstrap.
        final SessionFactoryImplementor sfi = mock(SessionFactoryImplementor.class, RETURNS_DEEP_STUBS);
        final SessionFactory sf = mock(SessionFactory.class);
        doReturn(sfi).when(sf).unwrap(SessionFactoryImplementor.class);

        final JdbcSinkConnectorConfig config = mock(JdbcSinkConnectorConfig.class);
        final Configuration hibernateConfig = mock(Configuration.class);
        when(config.getColumnNamingStrategy()).thenReturn(new DefaultColumnNamingStrategy());
        when(config.getHibernateConfiguration()).thenReturn(hibernateConfig);
        when(config.isQuoteIdentifiers()).thenReturn(false);
        when(config.useTimeZone()).thenReturn("UTC");

        // getDatabaseTimeZone() in the constructor opens a StatelessSession and executes a query;
        // with deep stubs the doReturningWork call returns null, which is caught/ignored.
        dialect = (SnowflakeDatabaseDialect) new SnowflakeDatabaseDialect.SnowflakeDatabaseDialectProvider()
                .instantiate(config, sf);

        // Common JDBC mocks used by the tableExists() tests
        connection = mock(Connection.class);
        preparedStatement = mock(PreparedStatement.class);
        resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
    }

    // -------------------------------------------------------------------------
    // Group A: ALTER TABLE syntax overrides
    // -------------------------------------------------------------------------

    @Test
    void alterTablePrefix_isEmptyString() {
        assertThat(dialect.getAlterTablePrefix()).isEqualTo("");
    }

    @Test
    void alterTableSuffix_isEmptyString() {
        assertThat(dialect.getAlterTableSuffix()).isEqualTo("");
    }

    @Test
    void alterTableColumnPrefix_isAddColumnWithTrailingSpace() {
        assertThat(dialect.getAlterTableColumnPrefix()).isEqualTo("ADD COLUMN ");
    }

    // -------------------------------------------------------------------------
    // Group B: tableExists() — INFORMATION_SCHEMA SQL correctness
    // -------------------------------------------------------------------------

    @Test
    void tableExists_withSchema_usesSchemaAndNameFilter() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        dialect.tableExists(connection, new CollectionId(null, "MY_SCHEMA", "ORDERS"));

        verify(connection).prepareStatement(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");
        verify(preparedStatement).setString(1, "MY_SCHEMA");
        verify(preparedStatement).setString(2, "ORDERS");
    }

    @Test
    void tableExists_withoutSchema_usesOnlyNameFilter() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        dialect.tableExists(connection, new CollectionId(null, null, "ORDERS"));

        verify(connection).prepareStatement(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?");
        verify(preparedStatement).setString(1, "ORDERS");
    }

    @Test
    void tableExists_withRealm_qualifiesInformationSchemaWithDatabase() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        dialect.tableExists(connection, new CollectionId("MY_DB", "MY_SCHEMA", "ORDERS"));

        verify(connection).prepareStatement(
                "SELECT COUNT(*) FROM MY_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");
        verify(preparedStatement).setString(1, "MY_SCHEMA");
        verify(preparedStatement).setString(2, "ORDERS");
    }

    @Test
    void tableExists_whenCountIsOne_returnsTrue() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        assertThat(dialect.tableExists(connection, new CollectionId(null, "MY_SCHEMA", "ORDERS"))).isTrue();
    }

    @Test
    void tableExists_whenCountIsZero_returnsFalse() throws SQLException {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(0);

        assertThat(dialect.tableExists(connection, new CollectionId(null, "MY_SCHEMA", "ORDERS"))).isFalse();
    }

    @Test
    void tableExists_lowercaseIdentifiers_areUppercasedBeforeQuery() throws SQLException {
        // isQuoteIdentifiers() == false (stubbed in setUp) triggers collectionId.toUpperCase()
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);

        dialect.tableExists(connection, new CollectionId(null, "my_schema", "orders"));

        verify(preparedStatement).setString(1, "MY_SCHEMA");
        verify(preparedStatement).setString(2, "ORDERS");
    }
}
