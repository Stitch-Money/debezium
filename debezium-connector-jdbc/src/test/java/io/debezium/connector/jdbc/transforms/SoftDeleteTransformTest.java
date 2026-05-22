/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SoftDeleteTransform}.
 *
 * @author Marinus Krommenhoek
 */
class SoftDeleteTransformTest {

    private static final String TOPIC = "test.topic";
    private static final String DELETED_FIELD = "is_deleted";

    /**
     * A DELETE envelope must be converted to an UPDATE: {@code op} becomes {@code "u"}, the
     * {@code after} struct is populated from {@code before}, and the soft-delete field is
     * {@code true}. All original field values must be preserved.
     */
    @Test
    void deleteEnvelopeIsSoftDeleted() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 1).put("name", "alice");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter).isNotNull();
            assertThat(resultAfter.schema().field(DELETED_FIELD)).isNotNull();
            assertThat(resultAfter.schema().field(DELETED_FIELD).schema().type()).isEqualTo(Schema.Type.BOOLEAN);
            assertThat(resultAfter.schema().field(DELETED_FIELD).schema().isOptional()).isTrue();
            assertThat(resultAfter.get(DELETED_FIELD)).isEqualTo(Boolean.TRUE);
            assertThat(resultAfter.get("id")).isEqualTo(1);
            assertThat(resultAfter.get("name")).isEqualTo("alice");

            assertThat(resultEnvelope.get("before")).isSameAs(before);
        }
    }

    /**
     * An INSERT envelope must gain the soft-delete field set to {@code false} when
     * {@code add.to.non.deletes} is {@code true} (the default). The {@code op} field must be
     * unchanged and all original field values must be preserved.
     */
    @Test
    void insertEnvelopeGainsSoftDeleteFieldWhenAddToNonDeletesTrue() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 2).put("name", "bob");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("c");

            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter).isNotNull();
            assertThat(resultAfter.schema().field(DELETED_FIELD)).isNotNull();
            assertThat(resultAfter.get(DELETED_FIELD)).isEqualTo(Boolean.FALSE);
            assertThat(resultAfter.get("id")).isEqualTo(2);
            assertThat(resultAfter.get("name")).isEqualTo("bob");
        }
    }

    /**
     * An INSERT envelope must be returned unchanged when {@code add.to.non.deletes} is
     * {@code false}.
     */
    @Test
    void insertEnvelopeUnchangedWhenAddToNonDeletesFalse() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 3).put("name", "carol");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD, "add.to.non.deletes", "false"));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * DELETE envelopes must always be soft-deleted regardless of the {@code add.to.non.deletes}
     * flag — that flag only controls annotation of INSERT/UPDATE records.
     */
    @Test
    void deleteEnvelopeIsSoftDeletedWhenAddToNonDeletesFalse() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 13).put("name", "grace");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD, "add.to.non.deletes", "false"));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");
            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter.get(DELETED_FIELD)).isEqualTo(Boolean.TRUE);
            assertThat(resultAfter.get("id")).isEqualTo(13);
            assertThat(resultEnvelope.get("before")).isSameAs(before);
        }
    }

    /**
     * A tombstone record (null value) must be returned as the same object reference unchanged.
     */
    @Test
    void tombstoneIsPassedThrough() {
        final SinkRecord record = new SinkRecord(TOPIC, 0, null, null, null, null, 0);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A DELETE envelope with a null {@code before} struct must be returned unchanged, since there
     * is no row state to write back to the target.
     */
    @Test
    void deleteEnvelopeWithNullBeforeIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A flat (already-unwrapped) record must be returned as the same object reference unchanged,
     * with a warning that the SMT cannot operate without the envelope.
     */
    @Test
    void flatRecordIsPassedThrough() {
        final Schema flatSchema = SchemaBuilder.struct()
                .field("id", Schema.INT32_SCHEMA)
                .build();
        final Struct value = new Struct(flatSchema).put("id", 4);
        final SinkRecord record = sinkRecord(flatSchema, value);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * Configuring without {@code deleted.field.name} must throw a {@link ConfigException}.
     */
    @Test
    void missingDeletedFieldNameThrows() {
        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            Assertions.assertThrows(ConfigException.class, () -> smt.configure(Map.of()));
        }
    }

    /**
     * Configuring with a blank {@code deleted.field.name} (empty string or whitespace) must throw
     * a {@link ConfigException}.
     */
    @Test
    void blankDeletedFieldNameThrows() {
        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            Assertions.assertThrows(ConfigException.class,
                    () -> smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, "")));
        }
        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            Assertions.assertThrows(ConfigException.class,
                    () -> smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, "   ")));
        }
    }

    /**
     * An envelope whose {@code op} field holds a {@code null} value (malformed or non-Debezium
     * event) must be returned unchanged with a warning rather than being treated as a non-delete
     * and gaining an incorrect {@code is_deleted=false} annotation.
     * An optional {@code op} field is used so that null can be placed into the struct.
     */
    @Test
    void envelopeWithNullOpIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.OPTIONAL_STRING_SCHEMA)
                .build();

        final Struct after = new Struct(rowSchema).put("id", 1).put("name", "alice");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", null);
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * Envelope schema name, version, and doc must be preserved on the transformed record.
     * The row schema name on the {@code after} sub-struct must also be preserved.
     */
    @Test
    void envelopeSchemaMetadataIsPreserved() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .version(2)
                .doc("row doc")
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .version(1)
                .doc("envelope doc")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct before = new Struct(rowSchema).put("id", 5);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            assertThat(result.valueSchema().name()).isEqualTo("test.Envelope");
            assertThat(result.valueSchema().version()).isEqualTo(1);
            assertThat(result.valueSchema().doc()).isEqualTo("envelope doc");

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().name()).isEqualTo("test.Value");
            assertThat(resultAfter.schema().version()).isEqualTo(2);
            assertThat(resultAfter.schema().doc()).isEqualTo("row doc");
        }
    }

    /**
     * When the source row already contains a column with the same name as {@code deleted.field.name},
     * the transform must not throw and must still set the field to {@code true} on a DELETE.
     */
    @Test
    void softDeleteFieldAlreadyInSchemaIsIdempotent() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 6).put(DELETED_FIELD, Boolean.FALSE);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");
            final Struct resultAfter = (Struct) resultEnvelope.get("after");
            assertThat(resultAfter).isNotNull();
            assertThat(resultAfter.get(DELETED_FIELD)).isEqualTo(Boolean.TRUE);
            assertThat(resultAfter.get("id")).isEqualTo(6);
        }
    }

    /**
     * A struct whose schema has {@code after} (STRUCT) and {@code op} (STRING) but no
     * {@code before} field is not recognised as a Debezium envelope and must be returned
     * unchanged as a flat record. Debezium envelopes always declare a {@code before} field.
     */
    @Test
    void schemaWithAfterAndOpButNoBeforeIsClassifiedAsFlat() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct envelope = new Struct(envelopeSchema)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * When the source row has a pre-existing {@code BOOLEAN} soft-delete field set to {@code true}
     * and an UPDATE event arrives, the field value must be preserved — not overwritten to {@code false}.
     */
    @Test
    void updateOfAlreadySoftDeletedRowPreservesDeletedFlag() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 9).put(DELETED_FIELD, Boolean.TRUE);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * When the source row has a pre-existing {@code BOOLEAN} soft-delete field set to {@code false}
     * and an UPDATE event arrives, the field value must be preserved — not overwritten to anything.
     */
    @Test
    void updatePreservesExistingFalseDeletedFlag() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 14).put(DELETED_FIELD, Boolean.FALSE);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "u");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A DELETE envelope whose row schema has a pre-existing field with the same name as
     * {@code deleted.field.name} but an incompatible (non-BOOLEAN) type must be returned
     * unchanged. Writing a {@code Boolean} into a non-BOOLEAN field would be a type violation,
     * so the DELETE is passed through as-is and a warning is logged.
     */
    @Test
    void deleteWithIncompatibleFieldTypeIsPassedThrough() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 10).put(DELETED_FIELD, "false");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * An INSERT or UPDATE envelope whose {@code after} row schema has a pre-existing field with
     * the same name as {@code deleted.field.name} but an incompatible (non-BOOLEAN) type must be
     * returned unchanged with a warning — consistent with the DELETE path behaviour.
     */
    @Test
    void insertWithIncompatibleFieldTypeIsPassedThrough() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field(DELETED_FIELD, Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 15).put(DELETED_FIELD, "false");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A record whose schema contains an {@code "after"} STRUCT field but no {@code "op"} field
     * must not be routed into the envelope path and must be returned unchanged.
     */
    @Test
    void structWithAfterButNoOpFieldIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema noOpSchema = SchemaBuilder.struct()
                .name("test.NoOp")
                .field("after", rowSchema)
                .build();

        final Struct value = new Struct(noOpSchema)
                .put("after", new Struct(rowSchema).put("id", 7).put("name", "dave"));
        final SinkRecord record = sinkRecord(noOpSchema, value);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A non-delete envelope whose {@code after} struct is {@code null} (e.g. a TRUNCATE or
     * snapshot-complete event with {@code op="t"}) must be returned unchanged when
     * {@code add.to.non.deletes=true} — there is no {@code after} payload to annotate.
     */
    @Test
    void nonDeleteEnvelopeWithNullAfterIsPassedThrough() {
        final Schema rowSchema = rowSchema();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", null)
                .put("op", "t");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A schema whose {@code before} field has a non-STRUCT type must not be recognised as a
     * Debezium envelope — all three fields must be present with the correct types.
     */
    @Test
    void schemaWithNonStructBeforeIsClassifiedAsFlat() {
        final Schema rowSchema = rowSchema();
        final Schema schema = SchemaBuilder.struct()
                .name("test.BadBefore")
                .field("before", Schema.OPTIONAL_STRING_SCHEMA)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("before", null)
                .put("after", new Struct(rowSchema).put("id", 11).put("name", "eve"))
                .put("op", "c");
        final SinkRecord record = sinkRecord(schema, value);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * A schema whose {@code op} field has a non-STRING type must not be recognised as a
     * Debezium envelope.
     */
    @Test
    void schemaWithNonStringOpIsClassifiedAsFlat() {
        final Schema rowSchema = rowSchema();
        final Schema schema = SchemaBuilder.struct()
                .name("test.BadOp")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("before", null)
                .put("after", new Struct(rowSchema).put("id", 12).put("name", "frank"))
                .put("op", 1);
        final SinkRecord record = sinkRecord(schema, value);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            assertThat(smt.apply(record)).isSameAs(record);
        }
    }

    /**
     * Schema {@code parameters()} declared on the row schema must survive the rebuild and be
     * present on the {@code after} sub-schema of the transformed record.
     */
    @Test
    void rowSchemaParametersArePreserved() {
        final Schema rowSchema = SchemaBuilder.struct()
                .name("test.Value")
                .parameter("connect.decimal.precision", "10")
                .field("id", Schema.INT32_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 8);
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().parameters()).isNotNull();
            assertThat(resultAfter.schema().parameters().get("connect.decimal.precision")).isEqualTo("10");
        }
    }

    /**
     * Extra envelope fields beyond {@code before}/{@code after}/{@code op} (e.g. {@code source},
     * {@code ts_ms}) must be copied unchanged into the rebuilt envelope after a soft delete.
     */
    @Test
    void extraEnvelopeFieldsAreCopiedAfterSoftDelete() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = SchemaBuilder.struct()
                .name("test.Source")
                .field("connector", Schema.STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        final Struct source = new Struct(sourceSchema).put("connector", "postgres");
        final Struct before = new Struct(rowSchema).put("id", 16).put("name", "heidi");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("source", source)
                .put("op", "d")
                .put("ts_ms", 1_700_000_000_000L);
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("u");
            assertThat(resultEnvelope.get("source")).isSameAs(source);
            assertThat(resultEnvelope.get("ts_ms")).isEqualTo(1_700_000_000_000L);
        }
    }

    /**
     * Extra envelope fields beyond {@code before}/{@code after}/{@code op} (e.g. {@code source},
     * {@code ts_ms}) must be copied unchanged into the rebuilt envelope after an INSERT/UPDATE
     * with {@code add.to.non.deletes=true}.
     */
    @Test
    void extraEnvelopeFieldsAreCopiedAfterNonDeleteAnnotation() {
        final Schema rowSchema = rowSchema();
        final Schema sourceSchema = SchemaBuilder.struct()
                .name("test.Source")
                .field("connector", Schema.STRING_SCHEMA)
                .optional()
                .build();
        final Schema envelopeSchema = SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
                .build();

        final Struct source = new Struct(sourceSchema).put("connector", "postgres");
        final Struct after = new Struct(rowSchema).put("id", 17).put("name", "ivan");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("source", source)
                .put("op", "c")
                .put("ts_ms", 1_700_000_000_001L);
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultEnvelope = (Struct) result.value();
            assertThat(resultEnvelope.get("op")).isEqualTo("c");
            assertThat(resultEnvelope.get("source")).isSameAs(source);
            assertThat(resultEnvelope.get("ts_ms")).isEqualTo(1_700_000_000_001L);
        }
    }

    /**
     * The rebuilt {@code after} sub-schema must preserve the {@code optional} flag of the
     * original row schema on a DELETE event so that downstream consumers (schema registry,
     * JDBC sink schema cache) see a structurally identical schema.
     */
    @Test
    void rebuiltAfterSchemaPreservesOptionalFlagOnDelete() {
        final Schema rowSchema = rowSchema(); // declared optional
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct before = new Struct(rowSchema).put("id", 18).put("name", "julia");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", before)
                .put("after", null)
                .put("op", "d");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().isOptional()).isTrue();
        }
    }

    /**
     * The rebuilt {@code after} sub-schema must also preserve the {@code optional} flag on
     * INSERT/UPDATE events (non-delete path through {@code applyToNonDeleteEnvelope}).
     */
    @Test
    void rebuiltAfterSchemaPreservesOptionalFlagOnNonDelete() {
        final Schema rowSchema = rowSchema(); // declared optional
        final Schema envelopeSchema = envelopeSchema(rowSchema);

        final Struct after = new Struct(rowSchema).put("id", 19).put("name", "karl");
        final Struct envelope = new Struct(envelopeSchema)
                .put("before", null)
                .put("after", after)
                .put("op", "c");
        final SinkRecord record = sinkRecord(envelopeSchema, envelope);

        try (SoftDeleteTransform<SinkRecord> smt = new SoftDeleteTransform<>()) {
            smt.configure(Map.of(DELETED_FIELD_NAME_CONFIG, DELETED_FIELD));
            final SinkRecord result = smt.apply(record);

            final Struct resultAfter = (Struct) ((Struct) result.value()).get("after");
            assertThat(resultAfter.schema().isOptional()).isTrue();
        }
    }

    private static final String DELETED_FIELD_NAME_CONFIG = "deleted.field.name";

    private static Schema rowSchema() {
        return SchemaBuilder.struct()
                .name("test.Value")
                .field("id", Schema.INT32_SCHEMA)
                .field("name", Schema.OPTIONAL_STRING_SCHEMA)
                .optional()
                .build();
    }

    private static Schema envelopeSchema(Schema rowSchema) {
        return SchemaBuilder.struct()
                .name("test.Envelope")
                .field("before", rowSchema)
                .field("after", rowSchema)
                .field("op", Schema.STRING_SCHEMA)
                .build();
    }

    private static SinkRecord sinkRecord(Schema valueSchema, Struct value) {
        return new SinkRecord(TOPIC, 0, null, null, valueSchema, value, 0);
    }
}
