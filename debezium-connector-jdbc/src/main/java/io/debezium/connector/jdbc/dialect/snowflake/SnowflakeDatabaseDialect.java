/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.SessionFactory;
import org.hibernate.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.JdbcSinkConnectorConfig;
import io.debezium.connector.jdbc.JdbcSinkRecord;
import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.dialect.DatabaseDialectProvider;
import io.debezium.connector.jdbc.dialect.GeneralDatabaseDialect;
import io.debezium.connector.jdbc.dialect.SqlStatementBuilder;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectBooleanType;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectDateType;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectInt16Type;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectInt32Type;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectInt8Type;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectTimeType;
import io.debezium.connector.jdbc.dialect.snowflake.connect.ConnectTimestampType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.DateType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.DebeziumZonedTimestampType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.MicroTimeType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.MicroTimestampType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.NanoTimeType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.NanoTimestampType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.TimeType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.TimestampType;
import io.debezium.connector.jdbc.dialect.snowflake.debezium.ZonedTimeType;
import io.debezium.connector.jdbc.hibernate.dialect.snowflake.SnowflakeDialect;
import io.debezium.connector.jdbc.relational.TableDescriptor;
import io.debezium.connector.jdbc.type.connect.ConnectInt64Type;
import io.debezium.data.Json;
import io.debezium.data.Xml;
import io.debezium.metadata.CollectionId;
import io.debezium.util.Strings;

public class SnowflakeDatabaseDialect extends GeneralDatabaseDialect {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeDatabaseDialect.class);

    private static final int MAX_VARCHAR_LENGTH = 134217728;
    private static final int MAX_VARBINARY_LENGTH = 67108864;
    private static final int DEFAULT_DECIMAL_PRECISION = 38;
    private static final int DEFAULT_TIMESTAMP_PRECISION = 9;
    private static final int DEFAULT_TIME_PRECISION = 9;

    public static class SnowflakeDatabaseDialectProvider implements DatabaseDialectProvider {
        @Override
        public boolean supports(Dialect dialect) {
            return dialect instanceof SnowflakeDialect;
        }

        @Override
        public Class<?> name() {
            return SnowflakeDatabaseDialect.class;
        }

        @Override
        public DatabaseDialect instantiate(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
            return new SnowflakeDatabaseDialect(config, sessionFactory);
        }
    }

    private SnowflakeDatabaseDialect(JdbcSinkConnectorConfig config, SessionFactory sessionFactory) {
        super(config, sessionFactory);
    }

    @Override
    public boolean tableExists(Connection connection, CollectionId collectionId) throws SQLException {
        // Snowflake's JDBC DatabaseMetaData.getTables() with a null schema searches ALL accessible
        // schemas, which produces false positives when a table with the same name exists elsewhere.
        // Querying INFORMATION_SCHEMA.TABLES directly avoids this. INFORMATION_SCHEMA is
        // database-scoped: without qualification it resolves to the session's current database
        // (set via ?db=... in the connection URL). When realm is set (3-part topic name, e.g.
        // "mydb.myschema.orders") qualify the reference so we search the correct database even
        // if it differs from the session database.
        if (!getConfig().isQuoteIdentifiers()) {
            collectionId = collectionId.toUpperCase();
        }
        final boolean hasRealm = !Strings.isNullOrBlank(collectionId.realm());
        final boolean hasSchema = !Strings.isNullOrBlank(collectionId.namespace());

        final String infoSchema = hasRealm
                ? collectionId.realm() + ".INFORMATION_SCHEMA.TABLES"
                : "INFORMATION_SCHEMA.TABLES";

        final String sql = hasSchema
                ? "SELECT COUNT(*) FROM " + infoSchema + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?"
                : "SELECT COUNT(*) FROM " + infoSchema + " WHERE TABLE_NAME = ?";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            if (hasSchema) {
                stmt.setString(1, collectionId.namespace());
                stmt.setString(2, collectionId.name());
            }
            else {
                stmt.setString(1, collectionId.name());
            }
            try (ResultSet rs = stmt.executeQuery()) {
                return rs.next() && rs.getInt(1) > 0;
            }
        }
    }

    @Override
    public TableDescriptor readTable(Connection connection, CollectionId collectionId) throws SQLException {
        // Apply Snowflake's uppercase-when-unquoted rule before delegating to the parent's
        // JDBC metadata calls (getTables / getPrimaryKeys / getColumns).
        if (!getConfig().isQuoteIdentifiers()) {
            collectionId = collectionId.toUpperCase();
        }
        return super.readTable(connection, collectionId);
    }

    @Override
    public CollectionId getCollectionId(String tableName) {
        // Centralise Snowflake's uppercase-when-unquoted rule at the point where the
        // CollectionId is created, so every downstream use (DDL, buffer map key, log
        // messages) consistently reflects the casing that Snowflake actually stores.
        CollectionId id = super.getCollectionId(tableName);
        if (!getConfig().isQuoteIdentifiers()) {
            return id.toUpperCase();
        }
        return id;
    }

    @Override
    public String getAlterTablePrefix() {
        // Snowflake ALTER TABLE syntax: ADD COLUMN col TYPE (no wrapping parentheses)
        return "";
    }

    @Override
    public String getAlterTableSuffix() {
        return "";
    }

    @Override
    public String getAlterTableColumnPrefix() {
        return "ADD COLUMN ";
    }

    @Override
    public String getInsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        LOGGER.info("Compiling Insert");
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("INSERT INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" ( ");
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") SELECT ");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(),
                (name) -> transformedNameFromField(name, record, table));
        builder.append(" FROM ( VALUES (");
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> "?"); // replacing columnQueryBindingFromField(name, table, record));
        builder.append(" )) AS S(");
        builder.appendLists(", ", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(")");

        final String statement = builder.build();
        LOGGER.info(statement);
        return statement;
    }

    @Override
    public String getUpsertStatement(TableDescriptor table, JdbcSinkRecord record) {
        LOGGER.info("Compiling Upsert");
        final SqlStatementBuilder builder = new SqlStatementBuilder();
        builder.append("MERGE INTO ");
        builder.append(getQualifiedTableName(table.getId()));
        builder.append(" AS T ");
        builder.append("USING (SELECT ");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(),
                (name) -> transformedNameFromField(name, record, table));
        builder.append(" FROM (VALUES (");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> "?"); // replacing columnQueryBindingFromField(name, table, record));
        builder.append(")) AS TBL (");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(")) AS S (");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") ON ");
        builder.appendList(" AND ", record.keyFieldNames(), (String name) -> {
            String field = columnNameFromField(name, record);
            return "T." + field + "=S." + field;
        });
        builder.append(" WHEN MATCHED THEN UPDATE SET ");
        builder.appendList(",", record.nonKeyFieldNames(), (String name) -> {
            String field = columnNameFromField(name, record);
            return "T." + field + "=S." + field;
        });
        builder.append(" WHEN NOT MATCHED THEN INSERT (");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(), (name) -> columnNameFromField(name, record));
        builder.append(") VALUES (");
        builder.appendLists(",", record.keyFieldNames(), record.nonKeyFieldNames(), (String name) -> {
            String field = columnNameFromField(name, record);
            return "S." + field;
        });
        builder.append(")");

        final String statement = builder.build();
        LOGGER.info(String.format("Upsert statement: %s", statement));

        return statement;
    }

    @Override
    public int getMaxVarcharLengthInKey() {
        return MAX_VARCHAR_LENGTH;
    }

    @Override
    public int getMaxVarbinaryLength() {
        return MAX_VARBINARY_LENGTH;
    }

    @Override
    public int getDefaultDecimalPrecision() {
        return DEFAULT_DECIMAL_PRECISION;
    }

    @Override
    public int getDefaultTimestampPrecision() {
        return DEFAULT_TIMESTAMP_PRECISION;
    }

    @Override
    public int getMaxTimePrecision() {
        return DEFAULT_TIME_PRECISION;
    }

    @Override
    public String getFormattedBoolean(boolean value) {
        // PostgreSQL maps logical TRUE/FALSE for boolean data types
        return value ? "TRUE" : "FALSE";
    }

    @Override
    public String getFormattedTimestampWithTimeZone(String value) {
        // Snowflake rejects bare string literals as defaults for TIMESTAMP_TZ columns.
        // Wrapping with TO_TIMESTAMP_TZ() ensures the DDL is accepted.
        return String.format("TO_TIMESTAMP_TZ('%s')", value);
    }

    @Override
    protected Optional<String> getDatabaseTimeZoneQuery() {
        return Optional.of("SHOW PARAMETERS LIKE 'TIMEZONE' IN SESSION");
    }

    @Override
    protected String getDatabaseTimeZoneQueryResult(ResultSet rs) throws SQLException {
        return rs.getString(2);
    }

    protected String transformedNameFromField(String fieldName, JdbcSinkRecord record, TableDescriptor table) {
        final Schema schema = record.jdbcFields().get(fieldName).getSchema();
        final String name = columnNameFromField(fieldName, record);
        String columnQueryBinding = columnQueryBindingFromField(fieldName, table, record);
        columnQueryBinding = columnQueryBinding.replace("?", name) + " AS " + name;

        if (schema.name() != null) {
            if (schema.name().equals(Json.LOGICAL_NAME)) {
                return String.format("PARSE_JSON(%s) AS %s", name, name);
            }
            if (Set.of(Xml.LOGICAL_NAME, "XML").contains(schema.name())) {
                return String.format("PARSE_XML(%s) AS %s", name, name);
            }
        }
        if (schema.name() == null && Schema.Type.BYTES.equals(schema.type())) {
            // decimal type and other logical types are often described with schema type BYTES, but with a name
            // thus here we focus on the core type bytes instead of the logical type decimal or any other logical type
            // that is transfered as bytes but has a logical name
            return String.format("TO_BINARY(%s) AS %s", name, name);
        }
        if (Schema.Type.MAP.equals(schema.type())) {
            return String.format("PARSE_JSON(%s) AS %s", name, name);
        }
        // return name;
        return columnQueryBinding;
    }

    @Override
    protected void registerTypes() {
        super.registerTypes();

        /* connect types */
        registerType(ConnectInt8Type.INSTANCE);
        registerType(ConnectInt16Type.INSTANCE);
        registerType(ConnectInt32Type.INSTANCE);
        registerType(ConnectInt64Type.INSTANCE);
        registerType(ConnectBooleanType.INSTANCE);
        registerType(ConnectDateType.INSTANCE);
        registerType(ConnectTimeType.INSTANCE);
        registerType(ConnectTimestampType.INSTANCE);

        /* debezium types */
        registerType(DateType.INSTANCE);
        registerType(TimeType.INSTANCE);
        registerType(MicroTimeType.INSTANCE);
        registerType(NanoTimeType.INSTANCE);
        registerType(TimestampType.INSTANCE);
        registerType(MicroTimestampType.INSTANCE);
        registerType(NanoTimestampType.INSTANCE);
        registerType(ZonedTimeType.INSTANCE);
        registerType(DebeziumZonedTimestampType.INSTANCE);

        /* snowflake types */
        registerType(ArrayType.INSTANCE);
        registerType(BitType.INSTANCE);
        registerType(SerialType.INSTANCE);
        registerType(JsonType.INSTANCE);
        registerType(XmlType.INSTANCE);
        registerType(UuidType.INSTANCE);
        registerType(EnumType.INSTANCE);
        registerType(BinaryType.INSTANCE);
        registerType(OidType.INSTANCE);
        registerType(MapToJsonType.INSTANCE);
    }

    @Override
    protected boolean isIdentifierUppercaseWhenNotQuoted() {
        return true;
    }

    @Override
    public String getTimestampPositiveInfinityValue() {
        // snowflake max timestamp is 9999-12-31T23:59:59Z
        return "9999-12-31T23:59:59Z";
    }

    @Override
    public String getTimestampNegativeInfinityValue() {
        /*
         * snowflake min timestamp is 1582-01-01T00:00:00Z
         * Snowflake uses the Gregorian Calendar for all dates and timestamps.
         * The Gregorian Calendar starts in the year 1582, but recognizes prior years,
         * which is important to note because Snowflake does not adjust dates prior to 1582 (or calculations involving dates prior to 1582) to match the Julian Calendar.
         * The UUUU format element supports negative years.
         */
        return "1582-01-01T00:00:00Z";
    }

}
