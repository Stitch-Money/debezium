/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.converters;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Types;
import java.util.OptionalInt;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.spi.converter.CustomConverter.Converter;
import io.debezium.spi.converter.CustomConverter.ConverterRegistration;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link UnconstrainedNumericToStringConverter}.
 */
@Tag("UnitTests")
class UnconstrainedNumericToStringConverterTest {

    private static final int VARIABLE_SCALE_LENGTH = UnconstrainedNumericToStringConverter.VARIABLE_SCALE_DECIMAL_LENGTH;

    /** Captures what the converter registers for a column, if anything. */
    private static final class CapturingRegistration implements ConverterRegistration<SchemaBuilder> {
        private SchemaBuilder schema;
        private Converter converter;
        private int registrations;

        @Override
        public void register(SchemaBuilder fieldSchema, Converter converter) {
            this.schema = fieldSchema;
            this.converter = converter;
            this.registrations++;
        }
    }

    private static UnconstrainedNumericToStringConverter converter() {
        final UnconstrainedNumericToStringConverter converter = new UnconstrainedNumericToStringConverter();
        converter.configure(new Properties());
        return converter;
    }

    private static CapturingRegistration register(RelationalColumn column) {
        final CapturingRegistration registration = new CapturingRegistration();
        converter().converterFor(column, registration);
        return registration;
    }

    private static RelationalColumn column(int jdbcType, String typeName, OptionalInt length, OptionalInt scale) {
        return new RelationalColumn() {
            @Override
            public int jdbcType() {
                return jdbcType;
            }

            @Override
            public int nativeType() {
                return 1700; // PgOid.NUMERIC; irrelevant for the converter
            }

            @Override
            public String typeName() {
                return typeName;
            }

            @Override
            public String typeExpression() {
                return typeName;
            }

            @Override
            public OptionalInt length() {
                return length;
            }

            @Override
            public OptionalInt scale() {
                return scale;
            }

            @Override
            public boolean isOptional() {
                return true;
            }

            @Override
            public Object defaultValue() {
                return null;
            }

            @Override
            public boolean hasDefaultValue() {
                return false;
            }

            @Override
            public String name() {
                return "amount";
            }

            @Override
            public String dataCollection() {
                return "public.orders";
            }
        };
    }

    private static RelationalColumn numeric(OptionalInt length, OptionalInt scale) {
        return column(Types.NUMERIC, "numeric", length, scale);
    }

    // --- column matching ---

    @Test
    void registersStringForNumericWithVariableScaleLengthAndNoScale() {
        final CapturingRegistration registration = register(numeric(OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty()));

        assertThat(registration.registrations).isEqualTo(1);
        assertThat(registration.schema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(registration.schema.isOptional()).isFalse();
        assertThat(registration.schema.name()).isNull();
    }

    @Test
    void registersStringForNumericWithVariableScaleLengthAndZeroScale() {
        final CapturingRegistration registration = register(numeric(OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.of(0)));

        assertThat(registration.registrations).isEqualTo(1);
        assertThat(registration.schema.type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void registersStringForNumericWithZeroLength() {
        final CapturingRegistration registration = register(numeric(OptionalInt.of(0), OptionalInt.of(0)));

        assertThat(registration.registrations).isEqualTo(1);
        assertThat(registration.schema.type()).isEqualTo(Schema.Type.STRING);
    }

    @Test
    void registersStringForNumericWithAbsentLength() {
        final CapturingRegistration registration = register(numeric(OptionalInt.empty(), OptionalInt.empty()));

        assertThat(registration.registrations).isEqualTo(1);
    }

    @Test
    void registersStringForDecimalTypeNameAndJdbcType() {
        final CapturingRegistration registration = register(
                column(Types.DECIMAL, "DECIMAL", OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty()));

        assertThat(registration.registrations).isEqualTo(1);
    }

    @Test
    void doesNotRegisterForNumericWithPrecisionAndScale() {
        assertThat(register(numeric(OptionalInt.of(10), OptionalInt.of(2))).registrations).isZero();
    }

    @Test
    void doesNotRegisterForNumericWithPrecisionOnly() {
        assertThat(register(numeric(OptionalInt.of(10), OptionalInt.of(0))).registrations).isZero();
        assertThat(register(numeric(OptionalInt.of(10), OptionalInt.empty())).registrations).isZero();
    }

    @Test
    void doesNotRegisterForVariableScaleLengthWithNonZeroScale() {
        assertThat(register(numeric(OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.of(4))).registrations).isZero();
    }

    @Test
    void doesNotRegisterForNonNumericColumns() {
        assertThat(register(column(Types.INTEGER, "int4", OptionalInt.of(10), OptionalInt.of(0))).registrations).isZero();
        assertThat(register(column(Types.VARCHAR, "varchar", OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty())).registrations).isZero();
        assertThat(register(column(Types.VARCHAR, "numeric", OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty())).registrations).isZero();
        assertThat(register(column(Types.NUMERIC, "money", OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty())).registrations).isZero();
        assertThat(register(column(Types.NUMERIC, null, OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty())).registrations).isZero();
    }

    // --- value conversion ---

    @Test
    void registeredConverterRendersSpecialValueDecimalAsPlainString() {
        final CapturingRegistration registration = register(numeric(OptionalInt.of(VARIABLE_SCALE_LENGTH), OptionalInt.empty()));

        assertThat(registration.converter.convert(new SpecialValueDecimal(new BigDecimal("1.50")))).isEqualTo("1.50");
    }

    @Test
    void convertsNullToNull() {
        assertThat(UnconstrainedNumericToStringConverter.convert(null)).isNull();
    }

    @Test
    void preservesTrailingZerosAndSign() {
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("1.50")))).isEqualTo("1.50");
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("-0.001")))).isEqualTo("-0.001");
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("0")))).isEqualTo("0");
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("12345678901234567890.123456789"))))
                .isEqualTo("12345678901234567890.123456789");
    }

    @Test
    void neverUsesScientificNotation() {
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("1E+30"))))
                .isEqualTo("1000000000000000000000000000000");
        assertThat(UnconstrainedNumericToStringConverter.convert(new SpecialValueDecimal(new BigDecimal("1E-9")))).isEqualTo("0.000000001");
        assertThat(UnconstrainedNumericToStringConverter.convert(new BigDecimal("1.23E+5"))).isEqualTo("123000");
    }

    @Test
    void rendersSpecialValuesWithPostgresSpelling() {
        assertThat(UnconstrainedNumericToStringConverter.convert(SpecialValueDecimal.NOT_A_NUMBER)).isEqualTo("NaN");
        assertThat(UnconstrainedNumericToStringConverter.convert(SpecialValueDecimal.POSITIVE_INF)).isEqualTo("Infinity");
        assertThat(UnconstrainedNumericToStringConverter.convert(SpecialValueDecimal.NEGATIVE_INF)).isEqualTo("-Infinity");
    }

    @Test
    void convertsBigDecimalDefaultValue() {
        assertThat(UnconstrainedNumericToStringConverter.convert(new BigDecimal("42.000"))).isEqualTo("42.000");
    }

    @Test
    void convertsDoubleValues() {
        assertThat(UnconstrainedNumericToStringConverter.convert(1.0E10d)).isEqualTo("10000000000");
        assertThat(UnconstrainedNumericToStringConverter.convert(2.5d)).isEqualTo("2.5");
        assertThat(UnconstrainedNumericToStringConverter.convert(1.5f)).isEqualTo("1.5");
        assertThat(UnconstrainedNumericToStringConverter.convert(Double.NaN)).isEqualTo("NaN");
        assertThat(UnconstrainedNumericToStringConverter.convert(Double.POSITIVE_INFINITY)).isEqualTo("Infinity");
        assertThat(UnconstrainedNumericToStringConverter.convert(Double.NEGATIVE_INFINITY)).isEqualTo("-Infinity");
    }

    @Test
    void convertsOtherNumbersAndStrings() {
        assertThat(UnconstrainedNumericToStringConverter.convert(7L)).isEqualTo("7");
        assertThat(UnconstrainedNumericToStringConverter.convert("3.14")).isEqualTo("3.14");
    }
}
