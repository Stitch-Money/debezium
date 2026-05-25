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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link InsertEventTimestamp}.
 *
 * @author Marinus Krommenhoek
 */
class InsertEventTimestampTest {

    private static final String TOPIC = "test.topic";
    private static final String COLUMN = "_event_at";
    private static final long EVENT_TS_MS = 1_700_000_000_000L;

    /**
     * A Debezium envelope with a non-null {@code after} and a valid {@code source.ts_ms} must
     * gain the configured Timestamp field in the {@code after} sub-struct set to exactly the
     * value from {@code source.ts_ms}. All original fields and the outer envelope structure
     * must be preserved.
     */
    @Test
    void envelopeAfterGainsEventTimestamp() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 1).put("name", "alice");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("c");
            assertThat(resultEnvelope.get("before")).isNull();
            assertThat(resultEnvelope.get("source")).isSameAs(source);

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.schema().field(COLUMN).schema().name()).isEqualTo(Timestamp.LOGICAL_NAME);
            assertThat(resultAfter.schema().field(COLUMN).schema().isOptional()).isTrue();
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
            assertThat(resultAfter.get("id")).isEqualTo(1);
            assertThat(resultAfter.get("name")).isEqualTo("alice");
        }
    }

    /**
     * A DELETE envelope (where {@code after} is null) must be returned as the same object
     * reference unchanged.
     */
    @Test
    void deleteEnvelopeIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct before = new Struct(rowSchema).put("id", 2).put("name", "bob");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("source", source)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A tombstone record (null value) must be returned as the same object reference unchanged.
     */
    @Test
    void tombstoneIsPassedThrough() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, null, 0);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A record whose value is a non-null non-{@link Struct} object (e.g. a primitive wrapper)
     * must be returned as the same object reference unchanged without throwing.
     */
    @Test
    void nonStructValueIsPassedThrough() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, Schema.STRING_SCHEMA, "raw-string", 0);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A flat (already-unwrapped) record must be returned as the same object reference unchanged,
     * with a warning that the SMT requires a Debezium envelope.
     */
    @Test
    void flatRecordIsPassedThrough() {
        final Schema flatSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        final Struct value = new Struct(flatSchema).put("id", 3);
        final SinkRecord record = sinkRecord(flatSchema, value);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * When the pre-existing Timestamp column has a schema-level {@code defaultValue} and the
     * physical slot is null, the event timestamp must still be stamped. {@link Struct#get} returns
     * the schema default for null slots, which would bypass the null check; the implementation
     * must use {@link Struct#getWithoutDefault} to inspect the raw stored value instead.
     */
    @Test
    void preExistingTimestampColumnWithSchemaDefaultAndNullValueIsStamped() {
        final Date schemaDefault = new Date(0L);
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(COLUMN, Timestamp.builder().optional().defaultValue(schemaDefault).build())
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 1).put(COLUMN, null);
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
        }
    }

    /**
     * An envelope that has no {@code source} field must be returned as the same object reference
     * unchanged, with a warning.
     */
    @Test
    void envelopeWithoutSourceFieldIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 4).put("name", "carol");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * An envelope where {@code source.ts_ms} is null must be returned as the same object
     * reference unchanged, with a warning.
     */
    @Test
    void envelopeWithNullSourceTsMsIsPassedThrough() {
        final Schema sourceSchema = SchemaBuilder.struct()
                .name("test.Source")
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build();
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 5).put("name", "dave");
        final Struct source = new Struct(sourceSchema).put("ts_ms", null);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * When the {@code after} struct already contains a Timestamp field matching {@code column.name}
     * with a non-null value, the source value must be preserved unchanged.
     */
    @Test
    void envelopeWithPreExistingTimestampColumnPreservesSourceValue() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(COLUMN, Timestamp.builder().optional().build())
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Date existingTimestamp = new Date(999_000_000_000L);
        final Struct after = new Struct(rowSchema).put("id", 6).put(COLUMN, existingTimestamp);
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result).isNotSameAs(record);
            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().fields()).hasSize(2);
            assertThat(resultAfter.get(COLUMN)).isEqualTo(existingTimestamp);
            assertThat(resultAfter.get("id")).isEqualTo(6);
        }
    }

    /**
     * When the {@code after} struct already contains a Timestamp field matching {@code column.name}
     * with a null value, the event timestamp must be stamped to prevent a null audit column.
     */
    @Test
    void envelopeWithNullPreExistingTimestampColumnIsStamped() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(COLUMN, Timestamp.builder().optional().build())
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 7).put(COLUMN, null);
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
            assertThat(resultAfter.get("id")).isEqualTo(7);
        }
    }

    /**
     * Envelope schema name, version, and doc — and the row schema name on the {@code after}
     * sub-struct — must be preserved on the transformed record.
     */
    @Test
    void envelopeSchemaMetadataIsPreserved() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .version(3)
                .doc("row doc")
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .version(2)
                .doc("envelope doc")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 8);
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result.valueSchema().name()).isEqualTo("test.Envelope");
            assertThat(result.valueSchema().version()).isEqualTo(2);
            assertThat(result.valueSchema().doc()).isEqualTo("envelope doc");

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().name()).isEqualTo("test.Value");
            assertThat(resultAfter.schema().version()).isEqualTo(3);
            assertThat(resultAfter.schema().doc()).isEqualTo("row doc");
            assertThat(resultAfter.schema().isOptional()).isTrue();
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
        }
    }

    /**
     * When the envelope schema itself is optional, the rebuilt envelope schema must also be
     * optional. This exercises the envelope-level {@code isOptional} branch of
     * {@code copySchemaMetadata}, which is distinct from the {@code after} row schema branch
     * tested by {@link #envelopeSchemaMetadataIsPreserved}.
     */
    @Test
    void optionalEnvelopeSchemaOptionalityIsPreserved() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .optional()
                .build();

        final Struct after = new Struct(rowSchema).put("id", 1).put("name", "alice");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result).isNotSameAs(record);
            assertThat(result.valueSchema().isOptional()).isTrue();
            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
        }
    }

    /**
     * Extra envelope fields beyond {@code before}/{@code after}/{@code source}/{@code op} (e.g.
     * the outer {@code ts_ms}) must be copied unchanged into the rebuilt envelope.
     */
    @Test
    void extraEnvelopeFieldsAreCopied() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 9).put("name", "eve");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c")
                .put("ts_ms", 1_700_000_001_000L);
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("source")).isSameAs(source);
            assertThat(resultEnvelope.get("ts_ms")).isEqualTo(1_700_000_001_000L);
            assertThat(resultEnvelope.get("op")).isEqualTo("c");

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
        }
    }

    /**
     * An envelope whose {@code source} field is present in the schema but has a non-STRUCT type
     * (e.g. STRING) must be returned as the same object reference unchanged, with a warning.
     * This exercises the {@code sourceField.schema().type() != Schema.Type.STRUCT} branch of
     * the source-field guard.
     */
    @Test
    void envelopeWithNonStructSourceFieldIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", Schema.OPTIONAL_STRING_SCHEMA)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 14).put("name", "heidi");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", "not-a-struct")
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * An envelope where {@code source.ts_ms} exists in the schema but has a non-INT64 type
     * must be returned as the same object reference unchanged, with a warning, rather than
     * crashing with a {@code ClassCastException} from an unchecked cast inside
     * {@code Struct.getInt64}.
     */
    @Test
    void envelopeWithNonInt64TsMsFieldIsPassedThrough() {
        final Schema sourceSchemaInt32TsMs = SchemaBuilder.struct()
                .name("test.Source")
                .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
                .optional()
                .build();
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchemaInt32TsMs);

        final Struct after = new Struct(rowSchema).put("id", 15).put("name", "ivan");
        final Struct source = new Struct(sourceSchemaInt32TsMs).put("ts_ms", 1_000_000);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * An envelope where the {@code source} struct declares no {@code ts_ms} field at all (schema
     * has no such field, not just a null value) must be returned as the same object reference
     * unchanged, with a warning. {@code Struct.getInt64} throws {@code DataException} for absent
     * fields, so the schema-level check must fire before the value read.
     */
    @Test
    void envelopeWithSourceMissingTsMsFieldIsPassedThrough() {
        final Schema sourceSchemaNoTsMs = SchemaBuilder.struct()
                .name("test.Source")
                .field("connector", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchemaNoTsMs);

        final Struct after = new Struct(rowSchema).put("id", 11).put("name", "frank");
        final Struct source = new Struct(sourceSchemaNoTsMs).put("connector", "postgres");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * When the {@code after} struct already contains a field matching {@code column.name} but with
     * an incompatible (non-Timestamp) type, the SMT must warn and continue — preserving the
     * original field value unchanged — rather than passing through or crashing.
     */
    @Test
    void envelopeWithIncompatibleColumnTypePreservesSourceValue() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(COLUMN, Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 12).put(COLUMN, "not-a-timestamp");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result).isNotSameAs(record);
            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.get(COLUMN)).isEqualTo("not-a-timestamp");
            assertThat(resultAfter.get("id")).isEqualTo(12);
        }
    }

    /**
     * An envelope whose {@code source} field exists in the schema but whose runtime value is null
     * must be returned as the same object reference unchanged, with a warning.
     */
    @Test
    void envelopeWithNullSourceStructIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 13).put("name", "grace");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", null)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A non-null {@code before} struct must survive the envelope rebuild unchanged. All happy-path
     * tests use {@code before = null}; this test ensures the generic field-copy loop correctly
     * propagates a populated {@code before} so that UPDATE events carry the full pre-image.
     */
    @Test
    void nonNullBeforeIsPreservedOnTransform() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct before = new Struct(rowSchema).put("id", 1).put("name", "old-name");
        final Struct after = new Struct(rowSchema).put("id", 1).put("name", "new-name");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", after)
                .put("source", source)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result).isNotSameAs(record);
            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("before")).isSameAs(before);
            assertThat(resultEnvelope.get("op")).isEqualTo("u");
            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
            assertThat(resultAfter.get("name")).isEqualTo("new-name");
        }
    }

    /**
     * The Kafka record timestamp must be preserved unchanged on the transformed output record.
     * A non-zero value is used so that accidental substitution with 0 or null is detectable.
     */
    @Test
    void kafkaRecordTimestampIsPreservedOnTransform() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema, sourceSchema);

        final Struct after = new Struct(rowSchema).put("id", 1).put("name", "alice");
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final long kafkaTimestamp = 1_700_000_002_000L;
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, envelopeSchema, envelope,
                0, kafkaTimestamp, TimestampType.CREATE_TIME);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result).isNotSameAs(record);
            assertThat(result.timestamp()).isEqualTo(kafkaTimestamp);
            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().field(COLUMN)).isNotNull();
            assertThat(resultAfter.get(COLUMN)).isEqualTo(new Date(EVENT_TS_MS));
        }
    }

    /**
     * Schema {@code parameters()} on both the envelope and the {@code after} row schema must be
     * propagated unchanged by {@code copySchemaMetadata}. Parameters drive Avro schema
     * fingerprinting; silent loss would cause schema-cache divergence downstream.
     */
    @Test
    void schemaParametersArePreserved() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .parameter("connect.decimal.precision", "18")
                .optional()
                .build();
        final Schema sourceSchema = sourceSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .parameter("envelope.param", "true")
                .build();

        final Struct after = new Struct(rowSchema).put("id", 10);
        final Struct source = new Struct(sourceSchema).put("ts_ms", EVENT_TS_MS);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            smt.configure(Map.of("column.name", COLUMN));
            final SinkRecord result = smt.apply(record);

            assertThat(result.valueSchema().parameters()).isEqualTo(envelopeSchema.parameters());
            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().parameters()).isEqualTo(rowSchema.parameters());
        }
    }

    /**
     * Configuring without a {@code column.name} must throw a {@link ConfigException}.
     */
    @Test
    void missingColumnNameThrows() {
        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            Assertions.assertThrows(ConfigException.class, () -> smt.configure(Map.of()));
        }
    }

    /**
     * Configuring with a blank {@code column.name} (empty string or whitespace) must throw a
     * {@link ConfigException}.
     */
    @Test
    void blankColumnNameThrows() {
        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            Assertions.assertThrows(ConfigException.class,
                    () -> smt.configure(Map.of("column.name", "")));
        }
        try (InsertEventTimestamp<SinkRecord> smt = new InsertEventTimestamp<>()) {
            Assertions.assertThrows(ConfigException.class,
                    () -> smt.configure(Map.of("column.name", "   ")));
        }
    }

    private static Schema rowSchema() {
        return SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
    }

    private static Schema sourceSchema() {
        return SchemaBuilder.struct()
                .name("test.Source")
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .optional()
                .build();
    }

    private static Schema envelopeSchema(Schema rowSchema, Schema sourceSchema) {
        return SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Struct value) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, 0);
    }
}
