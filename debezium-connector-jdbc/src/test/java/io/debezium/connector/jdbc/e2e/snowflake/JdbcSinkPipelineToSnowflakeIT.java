/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.e2e.snowflake;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;

import io.debezium.connector.jdbc.junit.jupiter.e2e.source.Source;
import io.debezium.connector.jdbc.junit.jupiter.snowflake.SnowflakeSinkDatabaseContextProvider;

/**
 * Implementation of the JDBC sink connector multi-source pipeline that writes to Snowflake.
 *
 * @author Marinus Krommenhoek
 */
@Tag("all")
@Tag("e2e")
@Tag("e2e-snowflake")
@ExtendWith(SnowflakeSinkDatabaseContextProvider.class)
public class JdbcSinkPipelineToSnowflakeIT extends AbstractJdbcSinkPipelineIT {

    @Override
    protected boolean isBitCoercedToBoolean() {
        return true;
    }

    @Override
    protected String getBooleanType() {
        return "BOOLEAN";
    }

    @Override
    protected String getBitsDataType() {
        return "VARCHAR";
    }

    @Override
    protected String getInt8Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt16Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt32Type() {
        return "NUMBER";
    }

    @Override
    protected String getInt64Type() {
        return "NUMBER";
    }

    @Override
    protected String getVariableScaleDecimalType() {
        return "FLOAT";
    }

    @Override
    protected String getDecimalType() {
        return "FLOAT";
    }

    @Override
    protected String getFloat32Type() {
        return "FLOAT";
    }

    @Override
    protected String getFloat64Type() {
        return "FLOAT";
    }

    @Override
    protected String getCharType(Source source, boolean key, boolean nationalized) {
        return "VARCHAR";
    }

    @Override
    protected String getStringType(Source source, boolean key, boolean nationalized, boolean maxLength) {
        if (maxLength) {
            return getTextType(nationalized);
        }
        if (!source.getOptions().isColumnTypePropagated() || key) {
            // Debezium does not propagate column type details for keys.
            return getTextType();
        }
        return "VARCHAR";
    }

    @Override
    protected String getTextType(boolean nationalized) {
        return "VARCHAR";
    }

    @Override
    protected String getBinaryType(Source source, String sourceDataType) {
        return "BINARY";
    }

    @Override
    protected String getJsonType(Source source) {
        return "VARIANT";
    }

    @Override
    protected String getJsonbType(Source source) {
        return getJsonType(source);
    }

    @Override
    protected String getXmlType(Source source) {
        return "VARIANT";
    }

    @Override
    protected String getUuidType(Source source, boolean key) {
        return "VARCHAR";
    }

    @Override
    protected String getEnumType(Source source, boolean key) {
        // The io.debezium.data.Enum implementation does not pass data type names, so we cannot replicate
        // the enum in the destination system, so we have to assume that STRING fallback is the all that
        // can be resolved.
        return getTextType();
    }

    @Override
    protected String getSetType(Source source, boolean key) {
        return getTextType();
    }

    @Override
    protected String getYearType() {
        return getInt32Type();
    }

    @Override
    protected String getDateType() {
        return "DATE";
    }

    @Override
    protected String getTimeType(Source source, boolean key, int precision) {
        return "TIME";
    }

    @Override
    protected String getTimeWithTimezoneType() {
        return "TIME";
    }

    @Override
    protected String getTimestampType(Source source, boolean key, int precision) {
        return "TIMESTAMPNTZ";
    }

    @Override
    protected String getTimestampWithTimezoneType(Source source, boolean key, int precision) {
        return "TIMESTAMPTZ";
    }

    @Override
    protected String getIntervalType(Source source, boolean numeric) {
        return "VARCHAR";
    }

}
