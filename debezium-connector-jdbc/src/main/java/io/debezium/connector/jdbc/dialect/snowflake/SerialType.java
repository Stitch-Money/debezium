/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.dialect.snowflake;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.jdbc.type.AbstractType;

public class SerialType extends AbstractType {

    public static final SerialType INSTANCE = new SerialType();

    @Override
    public String[] getRegistrationKeys() {
        return new String[]{ "SMALLSERIAL", "SERIAL", "BIGSERIAL" };
    }

    @Override
    public String getTypeName(Schema schema, boolean isKey) {
        return "NUMBER";
    }

    @Override
    public String getDefaultValueBinding(Schema schema, Object value) {
        // PostgreSQL does not allow specifying a default value for these data types
        // By returning a null value, no default value clause gets bound
        return null;
    }

}
