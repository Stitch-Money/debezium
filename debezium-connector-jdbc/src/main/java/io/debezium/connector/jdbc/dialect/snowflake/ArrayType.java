/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code ARRAY} column types.
 *
 * @author Marinus Krommenhoek
 */

public class ArrayType extends AbstractType {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static final ArrayType INSTANCE = new ArrayType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "ARRAY" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        // Snowflake's ARRAY is a semi-structured type with no element-type qualifier in DDL
        return "ARRAY";
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }
        // Snowflake ARRAY is semi-structured (like VARIANT). Values must be passed as a JSON
        // string and converted in SQL via PARSE_JSON() — the same pattern used for MAP types.
        // Using Types.ARRAY / Connection.createArrayOf() causes Snowflake to interpret the value
        // as VARCHAR, producing a type-mismatch error.
        try {
            return List.of(new ValueBindDescriptor(index, OBJECT_MAPPER.writeValueAsString(value)));
        }
        catch (JsonProcessingException e) {
            throw new ConnectException("Failed to serialize ARRAY to JSON", e);
        }
    }
}
