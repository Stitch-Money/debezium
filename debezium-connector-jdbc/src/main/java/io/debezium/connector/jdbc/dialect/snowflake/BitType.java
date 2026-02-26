/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.math.BigInteger;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.data.Bits;
import io.debezium.sink.column.ColumnDescriptor;
import io.debezium.sink.valuebinding.ValueBindDescriptor;
import io.debezium.util.Strings;

class BitType extends AbstractType {

    public static final BitType INSTANCE = new BitType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ Bits.LOGICAL_NAME, "BIT", "VARBIT" };
    }

    @Override
    public String getQueryBinding(ColumnDescriptor column, Schema schema, Object value) {
        if (isBitOne(schema)) {
            final Optional<String> columnType = getSourceColumnType(schema);
            if (columnType.isPresent() && "BIT".equals(columnType.get())) {
                final Optional<String> columnSize = getSourceColumnSize(schema);
                if (columnSize.isPresent() && "1".equals(columnSize.get())) {
                    return "cast(? as BOOLEAN)";
                }
            }
        }
        return "cast(? as VARCHAR)";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // todo: add support for BIT/VARBIT/BIT VARYING(n) default values
        return null;
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        if (isBitOne(schema)) {
            // when only one bit store as a boolean
            return "BOOLEAN";
        }

        final int bitSize = Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
        if (Integer.MAX_VALUE == bitSize) {
            // snowflake doesn't have type for bits so we will store them as varbinary
            // the max_value will place a limitation on what we can represent
            return "VARCHAR";
        }

        final Optional<String> columnType = getSourceColumnType(schema);
        if (columnType.isPresent() && "VARBIT".equals(columnType.get())) {
            return String.format("VARCHAR(%s)", bitSize);
        }

        return String.format("VARCHAR(%s)", bitSize);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {

        if (value == null) {
            return List.of(new ValueBindDescriptor(index, null));
        }

        if (isBitOne(schema) && (value instanceof Boolean)) {
            // snowflake driver version 3.28 cannot bind java.lang.Character, so we use boolean instead
            return List.of(new ValueBindDescriptor(index, value));
        }

        final int length = Integer.parseInt(schema.parameters().get(Bits.LENGTH_FIELD));
        final String binaryBitString = new BigInteger((byte[]) value).toString(2);
        if (length == Integer.MAX_VALUE) {
            return List.of(new ValueBindDescriptor(index, binaryBitString));
        }

        return List.of(new ValueBindDescriptor(index, Strings.justifyRight(binaryBitString, length, '0')));
    }

    private boolean isBitOne(Schema schema) {
        return Objects.isNull(schema.name());
    }

}
