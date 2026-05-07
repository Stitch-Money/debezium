/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

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

    static final String DEBEZIUM_UNAVAILABLE_VALUE = "__debezium_unavailable_value";

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
        if (DEBEZIUM_UNAVAILABLE_VALUE.equals(value)) {
            throw new DataException(
                    "Encountered a PostgreSQL TOAST sentinel value (\"" + DEBEZIUM_UNAVAILABLE_VALUE + "\") for a JSON/VARIANT column. " +
                            "The column was not included in the WAL record because it was not changed and its value is stored out-of-line (TOASTed). " +
                            "To resolve this, choose one of the following options: " +
                            "(1) Set REPLICA IDENTITY FULL on the source table so all column values are always included in the WAL record. " +
                            "(2) Configure the ReselectColumnsPostProcessor on the Debezium source connector to re-fetch TOASTed column values. " +
                            "(3) Add the ToastColumnFilter SMT to the sink connector configuration to strip TOASTed columns from the record before it reaches this connector: "
                            +
                            "transforms=stripToast, transforms.stripToast.type=io.debezium.connector.jdbc.transforms.ToastColumnFilter.");
        }
        if (value instanceof String) {
            return super.bind(index, schema, value);
        }
        return super.bind(index, schema, (String) value);
    }

}
