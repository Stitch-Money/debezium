/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import java.sql.Types;
import java.util.List;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;
import io.debezium.connector.jdbc.type.JdbcType;
import io.debezium.sink.valuebinding.ValueBindDescriptor;

/**
 * An implementation of {@link JdbcType} for {@code OID} data types.
 *
 * @author Marinus Krommenhoek
 */
class OidType extends AbstractType {

    public static final OidType INSTANCE = new OidType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "OID" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return getDialect().getJdbcTypeName(Types.INTEGER);
    }

    @Override
    public List<ValueBindDescriptor> bind(int index, Schema schema, Object value) {
        if (value instanceof Byte) {
            /*
             * For primitives debezium tends to wrap them in a Byte:
             * https://github.com/snowflakedb/snowflake-jdbc/blob/e1437bdb279aaa3bb17c5e8df6bf65d360b44d49/src/main/java/net/snowflake/client/jdbc/SnowflakePreparedStatementV1.java#L539
             */
            return List.of(new ValueBindDescriptor(index, Integer.valueOf(((Byte) value).intValue())));
        }
        return List.of(new ValueBindDescriptor(index, value));
    }

}
