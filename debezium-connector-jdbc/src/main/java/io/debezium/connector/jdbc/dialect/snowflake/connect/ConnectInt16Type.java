/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake.connect;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.connect.AbstractConnectSchemaType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

public class ConnectInt16Type extends AbstractConnectSchemaType {

    public static final ConnectInt16Type INSTANCE = new ConnectInt16Type();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "INT16" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.SMALLINT);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Byte) {
            /* For primitives debezium tends to wrap them in a Byte */
            return List.of(new ValueBindDescriptor(index, ((Byte) value).shortValue()));
        }
        return List.of(new ValueBindDescriptor(index, value));
    }

}
