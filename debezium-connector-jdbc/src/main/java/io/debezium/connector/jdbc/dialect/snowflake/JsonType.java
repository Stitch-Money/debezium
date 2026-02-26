/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Json;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Json} types.
 *
 * @author Marinus Krommenhoek
 */
class JsonType extends AbstractType {

    public static final JsonType INSTANCE = new JsonType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Json.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {

        return "?";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "VARIANT";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof String) {
            return super.bind(index, schema, value);
        }
        return super.bind(index, schema, (String) value);
    }

}
