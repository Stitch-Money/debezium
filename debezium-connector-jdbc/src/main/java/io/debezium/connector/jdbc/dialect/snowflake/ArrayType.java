/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code ARRAY} column types.
 *
 * @author Marinus Krommenhoek
 */

public class ArrayType extends AbstractType {

    public static final ArrayType INSTANCE = new ArrayType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "ARRAY(" + getElementTypeName(getDialect(), schema, isKey) + ")";
    }

    private String getElementTypeName(DatabaseDialect dialect, Schema schema, boolean isKey) {
        JdbcType elementJdbcType = dialect.getSchemaType(schema.valueSchema());
        return elementJdbcType.getTypeName(schema.valueSchema(), isKey);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        // Connection.createArrayOf() expects a bare SQL type name (e.g. "VARCHAR"), not a DDL
        // expression with size (e.g. "VARCHAR(16777216)"). The Snowflake JDBC driver validates the
        // type name against java.sql.JDBCType enum constants, which have no size qualifier.
        String elementTypeName = getElementTypeName(this.getDialect(), schema, false);
        int parenIdx = elementTypeName.indexOf('(');
        if (parenIdx >= 0) {
            elementTypeName = elementTypeName.substring(0, parenIdx);
        }
        return List.of(new ValueBindDescriptor(index, value, java.sql.Types.ARRAY, elementTypeName));
    }
}
