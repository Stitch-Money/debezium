/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import java.util.Date;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.connector.jdbc.Module;

/**
 * A Kafka Connect SMT (Single Message Transformation) that appends a datetime field containing
 * the exact timestamp of when the source database event occurred to a {@link ConnectRecord}'s
 * value schema and struct before the record reaches the JDBC sink connector.
 *
 * <p>The event timestamp is read from the {@code source.ts_ms} field inside the Debezium change
 * event envelope — an INT64 representing milliseconds since epoch of when the change was
 * committed to the source database's transaction log. This differs from
 * {@link InsertCurrentDatetime}, which stamps the connector's own processing time.
 *
 * <p>The field name is user-configurable via the {@code column.name} property. The field is
 * added with a Kafka Connect {@link Timestamp} logical type (INT64 millis-since-epoch), which
 * the JDBC sink connector maps to a database {@code TIMESTAMP} column.
 *
 * <p>Only Debezium change event envelopes are supported. Flat (already-unwrapped) records are
 * passed through unchanged with a warning, because the {@code source} metadata is not available
 * after {@code ExtractNewRecordState} has been applied.
 *
 * <p>The following records are passed through unchanged:
 * tombstones; non-Struct values; DELETE envelopes (where {@code after} is null); flat records
 * (with a warning); envelopes whose {@code source} field is absent or is not of type STRUCT
 * (with a warning); envelopes whose {@code source} struct is null (with a warning); envelopes
 * where the {@code source} struct has no {@code ts_ms} field in its schema (with a warning);
 * envelopes where {@code source.ts_ms} is not of type INT64 (with a warning);
 * envelopes where {@code source.ts_ms} is null (with a warning).
 *
 * <p>When {@code column.name} already exists in the {@code after} struct with a non-null
 * {@link Timestamp} value, the source value is preserved unchanged. When it exists with a null
 * value, the event timestamp is stamped to prevent a null audit column. When it exists with a
 * non-Timestamp type, a warning is logged and the source value is preserved unchanged.
 *
 * <h2>Configuration</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Required</th><th>Description</th></tr>
 *   <tr>
 *     <td>{@code column.name}</td>
 *     <td>Yes</td>
 *     <td>Name of the datetime column to insert into the record's value schema.</td>
 *   </tr>
 * </table>
 *
 * <h2>Connector configuration example</h2>
 * <pre>
 * transforms=addEventAt
 * transforms.addEventAt.type=io.debezium.connector.jdbc.transforms.InsertEventTimestamp
 * transforms.addEventAt.column.name=_event_at
 * </pre>
 *
 * @author Marinus Krommenhoek
 * @param <R> the record type
 */
public class InsertEventTimestamp<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertEventTimestamp.class);

    private static final String COLUMN_NAME_PARAM = "column.name";

    private static final io.debezium.config.Field COLUMN_NAME = io.debezium.config.Field.create(COLUMN_NAME_PARAM)
            .withDisplayName("Column Name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the datetime column to insert into the record's value schema.");

    private String columnName;

    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.columnName = config.getString(COLUMN_NAME);
        if (columnName == null || columnName.isBlank()) {
            throw new ConfigException(COLUMN_NAME_PARAM, columnName, "column.name must not be blank");
        }
        LOGGER.info("Configured with column.name='{}'", columnName);
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
        final Field beforeField = originalStruct.schema().field("before");
        final Field opField = originalStruct.schema().field("op");
        if (afterField != null && afterField.schema().type() == Schema.Type.STRUCT
                && beforeField != null && beforeField.schema().type() == Schema.Type.STRUCT
                && opField != null && opField.schema().type() == Schema.Type.STRING) {
            return applyToEnvelope(record, originalStruct);
        }

        LOGGER.warn("InsertEventTimestamp received a flat record on topic '{}'. " +
                "This SMT requires a Debezium envelope with a 'source.ts_ms' field and does not " +
                "support records unwrapped by ExtractNewRecordState. Record is passed through unchanged.",
                record.topic());
        return record;
    }

    private R applyToEnvelope(R record, Struct envelope) {
        final Struct after = (Struct) envelope.getWithoutDefault("after");
        if (after == null) {
            LOGGER.debug("Skipping DELETE envelope (null 'after') on topic '{}'", record.topic());
            return record;
        }

        final Field sourceField = envelope.schema().field("source");
        if (sourceField == null) {
            LOGGER.warn("Skipping record on topic '{}': envelope has no 'source' field. " +
                    "InsertEventTimestamp requires a Debezium envelope with a 'source.ts_ms' field.",
                    record.topic());
            return record;
        }
        if (sourceField.schema().type() != Schema.Type.STRUCT) {
            LOGGER.warn("Skipping record on topic '{}': envelope 'source' field has type {} instead of STRUCT. " +
                    "InsertEventTimestamp requires a Debezium envelope with a 'source.ts_ms' field.",
                    record.topic(), sourceField.schema().type());
            return record;
        }
        final Struct source = (Struct) envelope.getWithoutDefault("source");
        if (source == null) {
            LOGGER.warn("Skipping record on topic '{}': envelope 'source' field is null.",
                    record.topic());
            return record;
        }

        if (source.schema().field("ts_ms") == null) {
            LOGGER.warn("Skipping record on topic '{}': 'source' struct has no 'ts_ms' field. " +
                    "Cannot determine the event timestamp.", record.topic());
            return record;
        }
        if (source.schema().field("ts_ms").schema().type() != Schema.Type.INT64) {
            LOGGER.warn("Skipping record on topic '{}': 'source.ts_ms' field is not of type INT64. " +
                    "Cannot determine the event timestamp.", record.topic());
            return record;
        }
        final Long tsMs = source.getInt64("ts_ms");
        if (tsMs == null) {
            LOGGER.warn("Skipping record on topic '{}': 'source.ts_ms' is null. " +
                    "Cannot determine the event timestamp.", record.topic());
            return record;
        }

        final Date eventTimestamp = new Date(tsMs);

        final Schema expandedAfterSchema = buildExpandedSchema(after.schema());
        final Struct expandedAfterStruct = buildExpandedStruct(after, expandedAfterSchema, eventTimestamp);

        final Schema originalEnvelopeSchema = envelope.schema();
        final SchemaBuilder envelopeBuilder = SchemaBuilder.struct();
        copySchemaMetadata(originalEnvelopeSchema, envelopeBuilder);
        for (Field field : originalEnvelopeSchema.fields()) {
            envelopeBuilder.field(field.name(),
                    "after".equals(field.name()) ? expandedAfterSchema : field.schema());
        }
        final Schema expandedEnvelopeSchema = envelopeBuilder.build();

        final Struct expandedEnvelope = new Struct(expandedEnvelopeSchema);
        for (Field field : expandedEnvelopeSchema.fields()) {
            expandedEnvelope.put(field.name(),
                    "after".equals(field.name()) ? expandedAfterStruct : envelope.get(field.name()));
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

    private Schema buildExpandedSchema(Schema originalSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        copySchemaMetadata(originalSchema, builder);
        for (Field field : originalSchema.fields()) {
            builder.field(field.name(), field.schema());
        }
        final Field existingField = originalSchema.field(columnName);
        if (existingField == null) {
            builder.field(columnName, Timestamp.builder().optional().build());
        }
        else if (!Timestamp.LOGICAL_NAME.equals(existingField.schema().name())) {
            final String schemaDescription = existingField.schema().name() != null
                    ? existingField.schema().name()
                    : existingField.schema().type().getName();
            LOGGER.warn("Field '{}' already exists with schema type '{}' instead of Timestamp. " +
                    "Configure column.name to a column that does not exist in the source schema " +
                    "or is a Timestamp-typed column. Source value will be preserved unchanged.",
                    columnName, schemaDescription);
        }
        return builder.build();
    }

    private Struct buildExpandedStruct(Struct originalStruct, Schema expandedSchema, Date eventTimestamp) {
        final Struct expandedStruct = new Struct(expandedSchema);
        for (Field field : originalStruct.schema().fields()) {
            expandedStruct.put(field.name(), originalStruct.get(field.name()));
        }
        final Field existingField = originalStruct.schema().field(columnName);
        if (existingField == null) {
            expandedStruct.put(columnName, eventTimestamp);
        }
        else if (Timestamp.LOGICAL_NAME.equals(existingField.schema().name()) && originalStruct.getWithoutDefault(columnName) == null) {
            expandedStruct.put(columnName, eventTimestamp);
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
        io.debezium.config.Field.group(config, null, COLUMN_NAME);
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
