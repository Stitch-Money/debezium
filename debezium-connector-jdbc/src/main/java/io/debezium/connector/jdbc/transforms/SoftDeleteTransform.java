/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.jdbc.Module;

/**
 * A Kafka Connect SMT that converts Debezium DELETE events into soft deletes by marking the row
 * with a boolean column instead of physically removing it from the target table.
 *
 * <p>When a DELETE envelope is received ({@code op = "d"}), the SMT:
 * <ol>
 *   <li>Promotes the {@code before} struct to {@code after}, preserving the last known row state.</li>
 *   <li>Appends the configured boolean column set to {@code true} to the {@code after} struct.</li>
 *   <li>Changes {@code op} from {@code "d"} to {@code "u"} so the JDBC sink connector upserts
 *       the row rather than deleting it.</li>
 * </ol>
 *
 * <p>This SMT works directly with Debezium envelopes — {@code ExtractNewRecordState} is not
 * required, as the JDBC sink connector handles envelopes natively.
 *
 * <p>Envelope detection requires all three of: an {@code after} field of type STRUCT, a
 * {@code before} field of type STRUCT, and an {@code op} field of type STRING. Records that do
 * not match this shape are treated as flat records and passed through with a warning.
 *
 * <p>The following records are passed through unchanged:
 * tombstones; DELETE envelopes whose {@code before} value is null (ensure
 * {@code REPLICA IDENTITY FULL} on the source table); flat or unrecognised records (with a warning
 * logged); and any envelope where {@code deleted.field.name} already exists in the row schema with
 * a non-BOOLEAN type (with a warning logged — configure it to point to a BOOLEAN column or a column
 * that does not exist in the source schema). When {@code add.to.non.deletes=true} and the source
 * row schema already contains {@code deleted.field.name} as a BOOLEAN column (e.g. the source
 * table itself manages that column), INSERT/UPDATE records are also passed through unchanged — the
 * source value is trusted and the SMT does not force it to {@code false}. The DELETE path always
 * forces the field to {@code true} regardless.
 *
 * <p><b>Schema consistency note:</b> when {@code add.to.non.deletes=false} (non-default), INSERT
 * and UPDATE records retain their original schema while soft-delete records carry an expanded
 * schema with the additional boolean field. If {@code use.reduction.buffer=true} is also set on
 * the JDBC sink, this schema divergence triggers a buffer flush on every soft-delete, degrading
 * batch performance. Keep {@code add.to.non.deletes=true} (the default) when the reduction buffer
 * is active.
 *
 * <h2>Configuration</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Required</th><th>Description</th></tr>
 *   <tr>
 *     <td>{@code deleted.field.name}</td>
 *     <td>Yes</td>
 *     <td>Name of the boolean column to set when a row is soft-deleted.</td>
 *   </tr>
 *   <tr>
 *     <td>{@code add.to.non.deletes}</td>
 *     <td>No (default: {@code true})</td>
 *     <td>When {@code true}, the boolean column is also appended to INSERT/UPDATE records with
 *         value {@code false}, keeping the schema consistent across all rows.</td>
 *   </tr>
 * </table>
 *
 * <h2>Connector configuration example</h2>
 * <pre>
 * transforms=softDelete
 * transforms.softDelete.type=io.debezium.connector.jdbc.transforms.SoftDeleteTransform
 * transforms.softDelete.deleted.field.name=is_deleted
 * transforms.softDelete.add.to.non.deletes=true
 *
 * insert.mode=upsert
 * primary.key.mode=record_key
 * delete.enabled=false
 * </pre>
 *
 * @author Marinus Krommenhoek
 * @param <R> the record type
 */
public class SoftDeleteTransform<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(SoftDeleteTransform.class);

    private static final String DELETED_FIELD_NAME_PARAM = "deleted.field.name";
    private static final String ADD_TO_NON_DELETES_PARAM = "add.to.non.deletes";

    private static final io.debezium.config.Field DELETED_FIELD_NAME = io.debezium.config.Field.create(DELETED_FIELD_NAME_PARAM)
            .withDisplayName("Deleted Field Name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the boolean column to set when a row is soft-deleted.");

    private static final io.debezium.config.Field ADD_TO_NON_DELETES = io.debezium.config.Field.create(ADD_TO_NON_DELETES_PARAM)
            .withDisplayName("Add to Non-Delete Records")
            .withType(ConfigDef.Type.BOOLEAN)
            .withDefault(true)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Whether to append the soft-delete field with value false to INSERT/UPDATE records.");

    private String deletedFieldName;
    private boolean addToNonDeletes;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.deletedFieldName = config.getString(DELETED_FIELD_NAME);
        if (deletedFieldName == null || deletedFieldName.isBlank()) {
            throw new ConfigException(DELETED_FIELD_NAME_PARAM, deletedFieldName, "deleted.field.name must not be blank");
        }
        this.addToNonDeletes = config.getBoolean(ADD_TO_NON_DELETES);
        LOGGER.info("Configured with deleted.field.name='{}', add.to.non.deletes={}", deletedFieldName, addToNonDeletes);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            LOGGER.debug("Skipping tombstone record on topic '{}'", record.topic());
            return record;
        }

        if (!(record.value() instanceof Struct originalStruct)) {
            LOGGER.debug("Skipping non-Struct record value of type '{}'", record.value().getClass().getName());
            return record;
        }

        final Field afterField = originalStruct.schema().field("after");
        final Field opField = originalStruct.schema().field("op");
        final Field beforeField = originalStruct.schema().field("before");
        if (afterField != null && afterField.schema().type() == Schema.Type.STRUCT
                && opField != null && opField.schema().type() == Schema.Type.STRING
                && beforeField != null && beforeField.schema().type() == Schema.Type.STRUCT) {
            return applyToEnvelope(record, originalStruct);
        }

        LOGGER.warn("SoftDeleteTransform received a flat record on topic '{}'. " +
                "This SMT operates on Debezium envelopes and does not require ExtractNewRecordState. " +
                "Record is passed through unchanged.", record.topic());
        return record;
    }

    private R applyToEnvelope(R record, Struct envelope) {
        final String op = envelope.getString("op");
        if (op == null) {
            LOGGER.warn("Skipping record on topic '{}': envelope 'op' field is null.", record.topic());
            return record;
        }

        if ("d".equals(op)) {
            final Struct before = (Struct) envelope.get("before");
            if (before == null) {
                LOGGER.warn("Skipping soft delete on topic '{}': DELETE envelope has no 'before' struct. " +
                        "Ensure REPLICA IDENTITY FULL is configured on the source table.", record.topic());
                return record;
            }
            return applyToDeleteEnvelope(record, envelope, before);
        }

        if (addToNonDeletes) {
            final Struct after = (Struct) envelope.get("after");
            if (after != null) {
                return applyToNonDeleteEnvelope(record, envelope, after);
            }
            LOGGER.debug("Skipping non-delete record with null 'after' on topic '{}'", record.topic());
        }

        return record;
    }

    private R applyToDeleteEnvelope(R record, Struct envelope, Struct before) {
        final Field existingDeleteField = before.schema().field(deletedFieldName);
        if (existingDeleteField != null && existingDeleteField.schema().type() != Schema.Type.BOOLEAN) {
            LOGGER.warn("Skipping soft delete on topic '{}': field '{}' already exists in the row schema " +
                    "with incompatible type {}. The DELETE will be passed through unchanged. " +
                    "Configure deleted.field.name to point to a BOOLEAN column or a column that does " +
                    "not already exist in the source schema.",
                    record.topic(), deletedFieldName, existingDeleteField.schema().type());
            return record;
        }
        LOGGER.debug("Soft-deleting record on topic '{}', setting '{}=true'", record.topic(), deletedFieldName);

        final Schema expandedAfterSchema = buildExpandedSchema(before.schema());
        final Struct expandedAfterStruct = buildExpandedStruct(before, expandedAfterSchema, Boolean.TRUE);

        final Schema expandedEnvelopeSchema = buildEnvelopeSchema(envelope.schema(), expandedAfterSchema);
        final Struct expandedEnvelope = new Struct(expandedEnvelopeSchema);
        for (Field field : expandedEnvelopeSchema.fields()) {
            if ("after".equals(field.name())) {
                expandedEnvelope.put(field.name(), expandedAfterStruct);
            }
            else if ("op".equals(field.name())) {
                expandedEnvelope.put(field.name(), "u");
            }
            else {
                expandedEnvelope.put(field.name(), envelope.get(field.name()));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                expandedEnvelopeSchema,
                expandedEnvelope,
                record.timestamp(),
                record.headers());
    }

    private R applyToNonDeleteEnvelope(R record, Struct envelope, Struct after) {
        final Field existingDeleteField = after.schema().field(deletedFieldName);
        if (existingDeleteField != null) {
            if (existingDeleteField.schema().type() != Schema.Type.BOOLEAN) {
                LOGGER.warn("Skipping soft-delete annotation on topic '{}': field '{}' already exists in the " +
                        "row schema with incompatible type {}. The record will be passed through unchanged. " +
                        "Configure deleted.field.name to point to a BOOLEAN column or a column that does " +
                        "not already exist in the source schema.",
                        record.topic(), deletedFieldName, existingDeleteField.schema().type());
            }
            return record;
        }
        LOGGER.debug("Adding '{}=false' to non-delete record on topic '{}'", deletedFieldName, record.topic());

        final Schema expandedAfterSchema = buildExpandedSchema(after.schema());
        final Struct expandedAfterStruct = buildExpandedStruct(after, expandedAfterSchema, Boolean.FALSE);

        final Schema expandedEnvelopeSchema = buildEnvelopeSchema(envelope.schema(), expandedAfterSchema);
        final Struct expandedEnvelope = new Struct(expandedEnvelopeSchema);
        for (Field field : expandedEnvelopeSchema.fields()) {
            if ("after".equals(field.name())) {
                expandedEnvelope.put(field.name(), expandedAfterStruct);
            }
            else {
                expandedEnvelope.put(field.name(), envelope.get(field.name()));
            }
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                expandedEnvelopeSchema,
                expandedEnvelope,
                record.timestamp(),
                record.headers());
    }

    private Schema buildEnvelopeSchema(Schema originalEnvelopeSchema, Schema expandedAfterSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        copySchemaMetadata(originalEnvelopeSchema, builder);
        for (Field field : originalEnvelopeSchema.fields()) {
            builder.field(field.name(), "after".equals(field.name()) ? expandedAfterSchema : field.schema());
        }
        return builder.build();
    }

    private Schema buildExpandedSchema(Schema originalSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        copySchemaMetadata(originalSchema, builder);
        for (Field field : originalSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        if (originalSchema.field(deletedFieldName) == null) {
            builder.field(deletedFieldName, SchemaBuilder.bool().optional().build());
        }
        return builder.build();
    }

    private Struct buildExpandedStruct(Struct originalStruct, Schema expandedSchema, Boolean deletedValue) {
        final Struct expandedStruct = new Struct(expandedSchema);
        for (Field field : originalStruct.schema().fields()) {
            expandedStruct.put(field.name(), originalStruct.get(field.name()));
        }
        final Field existingField = originalStruct.schema().field(deletedFieldName);
        if (existingField == null) {
            expandedStruct.put(deletedFieldName, deletedValue);
        }
        else if (Boolean.TRUE.equals(deletedValue)) {
            expandedStruct.put(deletedFieldName, Boolean.TRUE);
        }
        return expandedStruct;
    }

    private void copySchemaMetadata(Schema source, SchemaBuilder target) {
        if (source.name() != null) {
            target.name(source.name());
        }
        if (source.version() != null) {
            target.version(source.version());
        }
        if (source.doc() != null) {
            target.doc(source.doc());
        }
        if (source.isOptional()) {
            target.optional();
        }
        if (source.parameters() != null && !source.parameters().isEmpty()) {
            target.parameters(source.parameters());
        }
    }

    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, DELETED_FIELD_NAME, ADD_TO_NON_DELETES);
        return config;
    }

    @Override
    public void close() {
    }

    @Override
    public String version() {
        return Module.version();
    }
}
