/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.converters;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * A Debezium {@link CustomConverter} that maps PostgreSQL <em>unconstrained</em> {@code NUMERIC}
 * columns (declared without precision and scale, e.g. {@code amount numeric}) to a Kafka Connect
 * {@code STRING} field instead of the default {@code io.debezium.data.VariableScaleDecimal} struct.
 *
 * <p>Under {@code decimal.handling.mode=precise} the PostgreSQL source connector emits
 * unconstrained numerics as a variable scale decimal, which downstream sinks such as the JDBC sink
 * connector cannot represent faithfully: on Snowflake the column ends up as a fixed-scale
 * {@code NUMERIC(38,0)} and fractional digits are lost. Switching the whole source connector to
 * {@code decimal.handling.mode=string} fixes that, but also turns every <em>constrained</em>
 * {@code NUMERIC(p,s)} column into a string. This converter only intercepts columns that the
 * PostgreSQL connector itself classifies as variable scale decimals, so constrained numerics keep
 * their precise {@code org.apache.kafka.connect.data.Decimal} mapping. The JDBC sink then creates
 * a {@code VARCHAR} column for the string field, preserving the full value.
 *
 * <p>A column is considered unconstrained when its JDBC type is {@code NUMERIC} or {@code DECIMAL},
 * its type name is {@code numeric} or {@code decimal}, its reported length (precision) is absent,
 * {@code 0} or {@code 131089} (the pgjdbc default for a typmod of {@code -1}) and its scale is
 * absent or {@code 0}. This mirrors {@code PostgresValueConverter#isVariableScaleDecimal}. Columns
 * such as {@code numeric(10)} or {@code numeric(10,2)} are not touched.
 *
 * <p>Values are rendered with {@link BigDecimal#toPlainString()} so no scientific notation is ever
 * produced and trailing zeros are preserved exactly as stored. The PostgreSQL special values are
 * emitted with their PostgreSQL spelling ({@code NaN}, {@code Infinity}, {@code -Infinity}) so the
 * text can be loaded back into PostgreSQL. SQL {@code NULL} is passed through as {@code null}.
 *
 * <p>This converter runs inside the <strong>source</strong> connector: custom converters are not
 * applied by sink connectors. The jar containing this class must therefore be placed in the
 * PostgreSQL connector's plug-in directory. The converter only depends on {@code debezium-api},
 * {@code debezium-core} and {@code connect-api}, all of which are already on that classpath.
 *
 * <h2>Configuration</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Required</th><th>Description</th></tr>
 *   <tr>
 *     <td>{@code type}</td>
 *     <td>Yes</td>
 *     <td>Fully qualified class name of this converter.</td>
 *   </tr>
 * </table>
 *
 * <h2>Connector configuration example</h2>
 * <pre>
 * decimal.handling.mode=precise
 * converters=unconstrainedNumeric
 * unconstrainedNumeric.type=io.debezium.connector.jdbc.converters.UnconstrainedNumericToStringConverter
 * </pre>
 *
 * @author Marinus Krommenhoek
 */
public class UnconstrainedNumericToStringConverter implements CustomConverter<SchemaBuilder, RelationalColumn> {

    private static final Logger LOGGER = LoggerFactory.getLogger(UnconstrainedNumericToStringConverter.class);

    /**
     * Length (precision) reported by pgjdbc for a {@code numeric} column whose typmod is {@code -1},
     * i.e. a column declared without precision and scale.
     */
    static final int VARIABLE_SCALE_DECIMAL_LENGTH = 131089;

    static final String NAN = "NaN";
    static final String POSITIVE_INFINITY = "Infinity";
    static final String NEGATIVE_INFINITY = "-Infinity";

    @Override
    public void configure(Properties props) {
        LOGGER.debug("Configured {}; unconstrained NUMERIC columns will be emitted as STRING", getClass().getSimpleName());
    }

    @Override
    public void converterFor(RelationalColumn field, ConverterRegistration<SchemaBuilder> registration) {
        if (isUnconstrainedNumeric(field)) {
            LOGGER.debug("Registering STRING converter for unconstrained numeric column {}.{}", field.dataCollection(), field.name());
            registration.register(SchemaBuilder.string(), UnconstrainedNumericToStringConverter::convert);
        }
    }

    /**
     * Returns whether the column is a PostgreSQL {@code NUMERIC}/{@code DECIMAL} declared without
     * precision and scale.
     */
    static boolean isUnconstrainedNumeric(RelationalColumn field) {
        if (field.jdbcType() != Types.NUMERIC && field.jdbcType() != Types.DECIMAL) {
            return false;
        }
        final String typeName = field.typeName();
        if (typeName == null || !(typeName.equalsIgnoreCase("numeric") || typeName.equalsIgnoreCase("decimal"))) {
            return false;
        }
        final OptionalInt length = field.length();
        final boolean unconstrainedLength = length.isEmpty() || length.getAsInt() == 0 || length.getAsInt() == VARIABLE_SCALE_DECIMAL_LENGTH;
        final boolean unconstrainedScale = field.scale().orElse(0) == 0;
        return unconstrainedLength && unconstrainedScale;
    }

    /**
     * Converts a raw numeric value from the PostgreSQL source (snapshot, streaming or column
     * default) to its exact string representation.
     */
    static Object convert(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof SpecialValueDecimal) {
            final SpecialValueDecimal special = (SpecialValueDecimal) value;
            final Optional<BigDecimal> decimal = special.getDecimalValue();
            if (decimal.isPresent()) {
                return decimal.get().toPlainString();
            }
            if (SpecialValueDecimal.NOT_A_NUMBER.equals(special)) {
                return NAN;
            }
            if (SpecialValueDecimal.POSITIVE_INF.equals(special)) {
                return POSITIVE_INFINITY;
            }
            if (SpecialValueDecimal.NEGATIVE_INF.equals(special)) {
                return NEGATIVE_INFINITY;
            }
            return special.toString();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof Double || value instanceof Float) {
            final double d = ((Number) value).doubleValue();
            if (Double.isNaN(d)) {
                return NAN;
            }
            if (d == Double.POSITIVE_INFINITY) {
                return POSITIVE_INFINITY;
            }
            if (d == Double.NEGATIVE_INFINITY) {
                return NEGATIVE_INFINITY;
            }
            return new BigDecimal(value.toString()).toPlainString();
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof String) {
            return value;
        }
        LOGGER.warn("Unexpected value type {} for unconstrained numeric column; using its string representation", value.getClass().getName());
        return value.toString();
    }
}
