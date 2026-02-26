/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.Uuid;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

class UuidType extends AbstractType {

    public static final UuidType INSTANCE = new UuidType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Uuid.LOGICAL_NAME };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        return "?";
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "VARCHAR";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Uuid) {
            super.bind(index, schema, ((Uuid) value).toString());
        }
        return super.bind(index, schema, value.toString());
    }

}
