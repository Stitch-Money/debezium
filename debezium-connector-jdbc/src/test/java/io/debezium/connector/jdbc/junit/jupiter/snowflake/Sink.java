/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.snowflake;

import static org.fest.assertions.Assertions.assertThat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import org.assertj.db.api.AbstractColumnAssert;
import org.assertj.db.api.TableAssert;
import org.assertj.db.type.ValueType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.ThrowingFunction;

import io.debezium.connector.jdbc.junit.jupiter.SinkType;

/**
 * A test parameter object that represents the sink database in a JDBC end-to-end test pipeline.
 *
 * @author Marinus Krommenhoek
 */
public class Sink extends JdbcConnectionProvider {

    protected static final Logger LOGGER = LoggerFactory.getLogger(Sink.class);

    private final SinkType type;

    public Sink(String url, Properties properties) {
        super(url, properties);
        this.type = SinkType.SNOWFLAKE;
    }

    public SinkType getType() {
        return type;
    }

    public String getJdbcUrl(Map<String, String> urlParameters) {
        return this.getJdbcUrl();
    }

    public String formatTableName(String tableName) {
        if (type.is(SinkType.ORACLE, SinkType.DB2, SinkType.SNOWFLAKE)) {
            return tableName.toUpperCase();
        }
        return tableName;
    }

    public String formatColumnName(String columnName) {
        if (type.is(SinkType.ORACLE, SinkType.DB2, SinkType.SNOWFLAKE)) {
            return columnName.toUpperCase();
        }
        return columnName;
    }

    public AbstractColumnAssert assertColumnType(TableAssert table, String columnName, ValueType type, boolean lenient) {
        return table.column(columnName).isOfType(type, lenient);
    }

    public AbstractColumnAssert assertColumnType(TableAssert table, String columnName, Class classType, Object values) {
        return table.column(columnName).isOfClass(classType, false).hasValues(values);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type) {
        assertColumnType(table, columnName, type, false);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, Number... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, String... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumnType(TableAssert table, String columnName, ValueType type, byte[]... values) {
        assertColumnType(table, columnName, type, isAnyValueNull(values)).hasValues(values);
    }

    public void assertColumnHasNullValue(TableAssert table, String columnName) {
        assertColumnType(table, columnName, ValueType.NOT_IDENTIFIED, false).hasOnlyNullValues();
    }

    public void assertColumn(String tableName, String columnName, String expectedType) {
        tableName = formatTableName(tableName).replace("_", "\\_");
        columnName = formatColumnName(columnName);
        String dbName = System.getenv("SNOWFLAKE_DB_NAME");
        String schemaName = System.getenv("SNOWFLAKE_SCHEMA_NAME");
        try (ResultSet rs = getConnection().getMetaData().getColumns(dbName, schemaName, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString("TYPE_NAME")).as(String.format("Column %s", columnName)).isEqualToIgnoringCase(expectedType);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertColumn(String tableName, String columnName, String expectedType, int length) {
        tableName = formatTableName(tableName);
        columnName = formatColumnName(columnName);
        try (ResultSet rs = getConnection().getMetaData().getColumns(null, null, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString(6)).isEqualToIgnoringCase(expectedType);
                assertThat(rs.getInt(7)).isEqualTo(length);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertColumn(String tableName, String columnName, String expectedType, int precision, int scale) {
        tableName = formatTableName(tableName);
        columnName = formatColumnName(columnName);
        try (ResultSet rs = getConnection().getMetaData().getColumns(null, null, tableName, columnName)) {
            if (rs.next()) {
                assertThat(rs.getString(6)).isEqualToIgnoringCase(expectedType);
                assertThat(rs.getInt(7)).isEqualTo(precision);
                assertThat(rs.getInt(9)).isEqualTo(scale);
                return;
            }
            throw new AssertionError(String.format("Column %s not found in table %s.", columnName, tableName));
        }
        catch (SQLException e) {
            throw new AssertionError(String.format("Failed to get column %s in table %s", columnName, tableName), e);
        }
    }

    public void assertRows(String tableName, ThrowingFunction<ResultSet, Void> consumer) throws Exception {
        try (Statement st = getConnection().createStatement()) {
            try (ResultSet rs = st.executeQuery("SELECT * FROM " + tableName)) {
                assertThat(rs.next()).isTrue();
                consumer.apply(rs);
            }
        }
        catch (SQLException e) {
            throw new AssertionError("Failed to assert rows", e);
        }
    }

    @SafeVarargs
    private <T> boolean isAnyValueNull(T... values) {
        return Arrays.stream(values).anyMatch(Objects::isNull);
    }

}
