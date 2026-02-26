/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.nio.ByteBuffer;
import java.sql.Types;
import java.util.HexFormat;
import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.hibernate.engine.jdbc.Size;

import io.debezium.connector.jdbc.dialect.DatabaseDialect;
import io.debezium.connector.jdbc.type.AbstractBytesType;
import io.debezium.connector.jdbc.util.ByteArrayUtils;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

class BinaryType extends AbstractBytesType {

    public static final BinaryType INSTANCE = new BinaryType();

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        return String.format(getDialect().getByteArrayFormat(), ByteArrayUtils.getByteArrayAsHex(value));
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        final int columnSize = Integer.parseInt(getSourceColumnSize(schema).orElse("0"));
        DatabaseDialect dialect = getDialect();
        if (columnSize > 0) {
            /* snowflake cannot store more than 64 MB in its BINARY column */
            /* if left to its own devices debezium retrieves an internal size */
            return dialect.getJdbcTypeName(Types.VARBINARY, Size.length(Math.min(columnSize, dialect.getMaxVarbinaryLength())));
        }
        /* choose max because snowflake varbinary is not variable */
        return dialect.getJdbcTypeName(Types.VARBINARY, Size.length(dialect.getMaxVarbinaryLength()));

    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof ByteBuffer) {
            final ByteBuffer buffer = (ByteBuffer) value;
            final HexFormat format = HexFormat.of();
            final String hex = format.formatHex(buffer.array());
            return List.of(new ValueBindDescriptor(
                    index,
                    hex));
        }
        return super.bind(index, schema, value);
    }
}
