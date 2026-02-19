/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.data.Enum;
import io.debezium.data.Uuid;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@link Enum} column types.
 *
 * @author Marinus Krommenhoek
 */
class EnumType extends AbstractType {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnumType.class);

    public static final EnumType INSTANCE = new EnumType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Enum.LOGICAL_NAME };
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
