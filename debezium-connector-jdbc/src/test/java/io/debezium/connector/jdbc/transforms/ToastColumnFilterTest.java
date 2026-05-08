/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ToastColumnFilter}.
 *
 * @author Marinus Krommenhoek
 */
class ToastColumnFilterTest {

    private static final String SENTINEL = "__debezium_unavailable_value";
    private static final String TOPIC = "test.topic";

    /**
     * A field carrying the TOAST sentinel must be removed from the output schema and struct.
     * The retained field must be present in both.
     */
    @Test
    void toastFieldIsStripped() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("items", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 1)
                .put("items", SENTINEL);
        final SinkRecord record = sinkRecord(valueSchema, value);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result.valueSchema().fields()).hasSize(1);
            assertThat(result.valueSchema().field("id")).isNotNull();
            assertThat(result.valueSchema().field("items")).isNull();
            assertThat(((Struct) result.value()).get("id")).isEqualTo(1);
        }
    }

    /**
     * A field whose value is a normal (non-sentinel) string must not be removed.
     * The original record reference must be returned unchanged.
     */
    @Test
    void nonToastFieldIsRetained() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("items", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 1)
                .put("items", "{\"foo\":1}");
        final SinkRecord record = sinkRecord(valueSchema, value);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result).isSameAs(record);
        }
    }

    /**
     * When multiple fields carry the sentinel, all of them must be removed.
     */
    @Test
    void multipleToastFieldsAreAllStripped() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("items", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 2)
                .put("items", SENTINEL)
                .put("metadata", SENTINEL);
        final SinkRecord record = sinkRecord(valueSchema, value);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result.valueSchema().fields()).hasSize(1);
            assertThat(result.valueSchema().field("id")).isNotNull();
            assertThat(result.valueSchema().field("items")).isNull();
            assertThat(result.valueSchema().field("metadata")).isNull();
        }
    }

    /**
     * When no fields carry the sentinel the original record must be returned as the same
     * object reference (fast path, zero allocation).
     */
    @Test
    void noToastFieldsReturnsSameRecord() {
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema).put("id", 3);
        final SinkRecord record = sinkRecord(valueSchema, value);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result).isSameAs(record);
        }
    }

    /**
     * A tombstone record (null value) must be returned unchanged.
     */
    @Test
    void tombstoneRecordIsPassedThrough() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, null, 0);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result).isSameAs(record);
        }
    }

    /**
     * When configured with a custom placeholder only fields matching that placeholder must be
     * stripped. The default sentinel must be treated as an ordinary value.
     */
    @Test
    void customPlaceholderIsRespected() {
        final String customSentinel = "__custom_unavailable__";
        final Schema valueSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .field("col_custom", Schema.OPTIONAL_STRING_SCHEMA)
                .field("col_default", Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        final Struct value = new Struct(valueSchema)
                .put("id", 4)
                .put("col_custom", customSentinel)
                .put("col_default", SENTINEL);
        final SinkRecord record = sinkRecord(valueSchema, value);

        final Map<String, String> config = new HashMap<>();
        config.put("unavailable.value.placeholder", customSentinel);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(config);
            final SinkRecord result = filter.apply(record);

            assertThat(result.valueSchema().fields()).hasSize(2);
            assertThat(result.valueSchema().field("id")).isNotNull();
            assertThat(result.valueSchema().field("col_default")).isNotNull();
            assertThat(result.valueSchema().field("col_custom")).isNull();
        }
    }

    /**
     * A TOAST field nested inside the {@code after} struct of a Debezium envelope must be
     * removed from the {@code after} sub-schema and sub-struct. All outer envelope fields must
     * remain present and the non-TOAST field inside {@code after} must be retained.
     */
    @Test
    void envelopeToastFieldInAfterIsStripped() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("json_col", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 1).put("json_col", SENTINEL);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            assertThat(result).isNotSameAs(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(result.valueSchema().field("before")).isNotNull();
            assertThat(result.valueSchema().field("after")).isNotNull();
            assertThat(result.valueSchema().field("op")).isNotNull();

            assertThat(resultEnvelope.get("before")).isNull();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.schema().field("id")).isNotNull();
            assertThat(resultAfter.schema().field("json_col")).isNull();
            assertThat(resultAfter.get("id")).isEqualTo(1);
        }
    }

    /**
     * When no fields in the envelope {@code after} carry the TOAST sentinel the original record
     * must be returned as the same object reference (fast path, zero allocation).
     */
    @Test
    void envelopeNoToastFieldReturnsSameRecord() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("json_col", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 1).put("json_col", "{\"foo\":1}");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            assertThat(filter.apply(record)).isSameAs(record);
        }
    }

    /**
     * A DELETE envelope record whose {@code after} is null must be returned unchanged.
     */
    @Test
    void envelopeNullAfterDeleteIsPassedThrough() {
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

        final Struct before = new Struct(rowSchema).put("id", 1);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            assertThat(filter.apply(record)).isSameAs(record);
        }
    }

    /**
     * When multiple fields inside the envelope {@code after} carry the TOAST sentinel all of
     * them must be removed; the non-TOAST field must be retained.
     */
    @Test
    void envelopeMultipleToastFieldsInAfterAreAllStripped() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("json_col", Schema.OPTIONAL_STRING_SCHEMA)
                .field("metadata", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 2).put("json_col", SENTINEL).put("metadata", SENTINEL);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            final SinkRecord result = filter.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().fields()).hasSize(1);
            assertThat(resultAfter.schema().field("id")).isNotNull();
            assertThat(resultAfter.schema().field("json_col")).isNull();
            assertThat(resultAfter.schema().field("metadata")).isNull();
        }
    }

    /**
     * When configured with a custom placeholder only fields in the envelope {@code after} that
     * match the custom placeholder must be stripped. Fields carrying the default sentinel must
     * be treated as ordinary values.
     */
    @Test
    void envelopeCustomPlaceholderIsRespected() {
        final String customSentinel = "__custom__";
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("json_col", Schema.OPTIONAL_STRING_SCHEMA)
                .field("other_col", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema)
                .put("id", 3)
                .put("json_col", customSentinel)
                .put("other_col", SENTINEL);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        final Map<String, String> config = new HashMap<>();
        config.put("unavailable.value.placeholder", customSentinel);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(config);
            final SinkRecord result = filter.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().fields()).hasSize(2);
            assertThat(resultAfter.schema().field("id")).isNotNull();
            assertThat(resultAfter.schema().field("other_col")).isNotNull();
            assertThat(resultAfter.schema().field("json_col")).isNull();
        }
    }

    /**
     * A tombstone record (null value) with a non-null envelope schema must be returned
     * unchanged — the tombstone guard fires before envelope detection.
     */
    @Test
    void envelopeTombstoneIsPassedThrough() {
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

        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, envelopeSchema, null, 0);

        try (ToastColumnFilter<SinkRecord> filter = new ToastColumnFilter<>()) {
            filter.configure(Map.of());
            assertThat(filter.apply(record)).isSameAs(record);
        }
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Struct value) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, 0);
    }
}
