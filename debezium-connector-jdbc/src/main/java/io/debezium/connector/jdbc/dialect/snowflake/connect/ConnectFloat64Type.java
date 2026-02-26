/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.connect;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.jdbc.type.connect.AbstractConnectSchemaType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class ConnectFloat64Type extends AbstractConnectSchemaType {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectFloat32Type.class);

    public static final ConnectFloat64Type INSTANCE = new ConnectFloat64Type();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "FLOAT64" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.DOUBLE);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        /* Snowflake uses double-precision (64 bit) IEEE 754 floating-point numbers. */
        if (value instanceof Byte) {
            /* For primitives debezium tends to wrap them in a Byte */
            LOGGER.info("Float passed in as Byte.");
            return List.of(new ValueBindDescriptor(index, Double.valueOf(value.toString())));
        }
        if (value instanceof Number) {
            LOGGER.info("Float passed in as Number.");
            return List.of(new ValueBindDescriptor(index, Double.valueOf(value.toString())));
        }
        return List.of(new ValueBindDescriptor(index, value));
    }
}
