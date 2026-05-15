/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InsertCurrentDatetime}.
 *
 * @author Marinus Krommenhoek
 */
class InsertCurrentDatetimeTest {

    private static final String TOPIC = "test.topic";
    private static final String COLUMN = "_loaded_at";

    /**
     * A flat record must gain the configured datetime field with a non-null {@link Date} value
     * bounded by the test's own clock, and all original fields must be preserved.
     */
    @Test
    void flatRecordGainsDatetimeField() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema).put("id", 1).put("name", "alice");
        final SinkRecord record = sinkRecord(valueSchema, value);

        try (InsertCurrentDatetime<SinkRecord> smt = new InsertCurrentDatetime<>()) {
            smt.configure(Map.of("column.name", COLUMN));

            final long before = System.currentTimeMillis();
            final SinkRecord result = smt.apply(record);
            final long after = System.currentTimeMillis();

            assertThat(result.valueSchema().field("id")).isNotNull();
            assertThat(result.valueSchema().field("name")).isNotNull();
            assertThat(result.valueSchema().field(COLUMN)).isNotNull();
            assertThat(result.valueSchema().field(COLUMN).schema().name()).isEqualTo(Timestamp.LOGICAL_NAME);
            assertThat(result.valueSchema().field(COLUMN).schema().isOptional()).isTrue();

            final Date loadedAt = (Date) ((Struct) result.value()).get(COLUMN);
            assertThat(loadedAt).isNotNull();
            assertThat(loadedAt.getTime()).isGreaterThanOrEqualTo(before);
            assertThat(loadedAt.getTime()).isLessThanOrEqualTo(after);

            assertThat(((Struct) result.value()).get("id")).isEqualTo(1);
            assertThat(((Struct) result.value()).get("name")).isEqualTo("alice");
        }
    }

    /**
     * The datetime field must be appended to the {@code after} sub-struct of a Debezium envelope.
     * All outer envelope fields must remain present and the {@code before} field must be null.
     */
    @Test
    void envelopeAfterStructGainsDatetimeField() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 2).put("name", "bob");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertCurrentDatetime<SinkRecord> smt = new InsertCurrentDatetime<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(result.valueSchema().field("before")).isNotNull();
            assertThat(result.valueSchema().field("after")).isNotNull();
            assertThat(result.valueSchema().field("op")).isNotNull();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");
            assertThat(resultEnvelope.get("before")).isNull();

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.schema().field("id")).isNotNull();
            assertThat(resultAfter.schema().field("name")).isNotNull();
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.schema().field(COLUMN).schema().name()).isEqualTo(Timestamp.LOGICAL_NAME);
            assertThat(resultAfter.get(COLUMN)).isNotNull();
            assertThat(resultAfter.get("id")).isEqualTo(2);
            assertThat(resultAfter.get("name")).isEqualTo("bob");
        }
    }

    /**
     * A tombstone record (null value) must be returned as the same object reference unchanged.
     */
    @Test
    void tombstoneIsPassedThrough() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, null, 0);

        try (InsertCurrentDatetime<SinkRecord> smt = new InsertCurrentDatetime<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A DELETE envelope record (where {@code after} is null) must be returned as the same object
     * reference unchanged.
     */
    @Test
    void deleteEnvelopeIsPassedThrough() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct before = new Struct(rowSchema).put("id", 3);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertCurrentDatetime<SinkRecord> smt = new InsertCurrentDatetime<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * Configuring without a {@code column.name} must throw a {@link ConfigException}.
     */
    @Test
    void missingColumnNameThrows() {
        try (InsertCurrentDatetime<SinkRecord> smt = new InsertCurrentDatetime<>()) {
            Assertions.assertThrows(ConfigException.class, () -> smt.configure(Map.of()));
        }
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Struct value) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, 0);
    }
}
