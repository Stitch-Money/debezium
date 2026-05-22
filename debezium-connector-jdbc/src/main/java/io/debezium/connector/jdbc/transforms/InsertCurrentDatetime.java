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
 * the current UTC time to a {@link ConnectRecord}'s value schema and struct before the record
 * reaches the JDBC sink connector.
 *
 * <p>The field name is user-configurable via the {@code column.name} property. The field is
 * added with a Kafka Connect {@link Timestamp} logical type (INT64 millis-since-epoch), which
 * the JDBC sink connector maps to a database {@code TIMESTAMP} column.
 *
 * <p>Both Debezium change event envelopes and already-unwrapped (flat) records are supported:
 * <ul>
 *   <li><b>Envelope record</b>: the datetime field is appended to the {@code after} sub-struct.
 *       DELETE events (where {@code after} is null) are passed through unchanged.</li>
 *   <li><b>Flat record</b>: the datetime field is appended to the top-level struct directly.</li>
 * </ul>
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
 * transforms=addLoadedAt
 * transforms.addLoadedAt.type=io.debezium.connector.jdbc.transforms.InsertCurrentDatetime
 * transforms.addLoadedAt.column.name=_loaded_at
 * </pre>
 *
 * @author Marinus Krommenhoek
 * @param <R> the record type
 */
public class InsertCurrentDatetime<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertCurrentDatetime.class);

    private static final String COLUMN_NAME_PARAM = "column.name";

    private static final io.debezium.config.Field COLUMN_NAME = io.debezium.config.Field.create(COLUMN_NAME_PARAM)
            .withDisplayName("Column Name")
            .withType(ConfigDef.Type.STRING)
            .withImportance(ConfigDef.Importance.HIGH)
            .withDescription("Name of the datetime column to insert into the record's value schema.");

    private String columnName;

    /**
     * Configures this transformation.
     *
     * @param configs the connector configuration properties
     * @throws ConfigException if {@code column.name} is absent or blank
     */
    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.columnName = config.getString(COLUMN_NAME);
        if (columnName == null || columnName.isBlank()) {
            throw new ConfigException(COLUMN_NAME_PARAM, columnName, "column.name must not be blank");
        }
        LOGGER.info("Configured with column.name='{}'", columnName);
    }

    /**
     * Appends the current UTC datetime to the record's value struct.
     *
     * <ul>
     *   <li><b>Tombstone</b> ({@code record.value() == null}): returned unchanged.</li>
     *   <li><b>Non-Struct value</b>: returned unchanged.</li>
     *   <li><b>Debezium envelope, {@code after} is non-null</b>: a new record is returned whose
     *       {@code after} sub-struct contains the additional datetime field.</li>
     *   <li><b>Debezium envelope, {@code after} is null</b> (DELETE event): returned unchanged.</li>
     *   <li><b>Flat record</b>: a new record is returned whose top-level struct contains the
     *       additional datetime field.</li>
     * </ul>
     *
     * @param record the record to transform
     * @return the original record if no transformation is needed, otherwise a new record with the
     *         datetime field appended
     */
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

        return applyToFlatRecord(record, originalStruct);
    }

    /**
     * Appends the datetime field to the {@code after} sub-struct of a Debezium change event
     * envelope. All other envelope fields pass through unmodified.
     *
     * @param record   the original record
     * @param envelope the top-level envelope struct
     * @return the original record if {@code after} is null (DELETE), otherwise a new record
     *         with the datetime field appended to the {@code after} sub-struct
     */
    private R applyToEnvelope(R record, Struct envelope) {
        final Struct after = (Struct) envelope.get("after");
        if (after == null) {
            return record;
        }

        if (after.schema().field(columnName) == null) {
            LOGGER.debug("Inserting datetime field '{}' into envelope 'after' on topic '{}'", columnName, record.topic());
        }
        else {
            LOGGER.debug("Field '{}' already exists in envelope 'after' on topic '{}'; preserving source value or stamping null",
                    columnName, record.topic());
        }

        final Schema expandedAfterSchema = buildExpandedSchema(after.schema());
        final Struct expandedAfterStruct = buildExpandedStruct(after, expandedAfterSchema);

        final Schema originalEnvelopeSchema = envelope.schema();
        final SchemaBuilder envelopeBuilder = SchemaBuilder.struct();
        if (originalEnvelopeSchema.name() != null) {
            envelopeBuilder.name(originalEnvelopeSchema.name());
        }
        if (originalEnvelopeSchema.version() != null) {
            envelopeBuilder.version(originalEnvelopeSchema.version());
        }
        if (originalEnvelopeSchema.doc() != null) {
            envelopeBuilder.doc(originalEnvelopeSchema.doc());
        }
        if (originalEnvelopeSchema.isOptional()) {
            envelopeBuilder.optional();
        }
        if (originalEnvelopeSchema.parameters() != null && !originalEnvelopeSchema.parameters().isEmpty()) {
            envelopeBuilder.parameters(originalEnvelopeSchema.parameters());
        }
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

    /**
     * Appends the datetime field to the top-level struct of an already-unwrapped flat record.
     *
     * @param record         the original record
     * @param originalStruct the flat value struct
     * @return a new record with the datetime field appended
     */
    private R applyToFlatRecord(R record, Struct originalStruct) {
        if (originalStruct.schema().field(columnName) == null) {
            LOGGER.debug("Inserting datetime field '{}' into flat record on topic '{}'", columnName, record.topic());
        }
        else {
            LOGGER.debug("Field '{}' already exists in flat record on topic '{}'; preserving source value or stamping null",
                    columnName, record.topic());
        }

        final Schema expandedSchema = buildExpandedSchema(originalStruct.schema());
        final Struct expandedStruct = buildExpandedStruct(originalStruct, expandedSchema);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                expandedSchema,
                expandedStruct,
                record.timestamp(),
                record.headers());
    }

    /**
     * Builds a new {@link Schema} identical to {@code originalSchema} with {@code columnName}
     * appended as an optional {@link Timestamp} field.
     *
     * @param originalSchema the schema to extend
     * @return the expanded schema
     */
    private Schema buildExpandedSchema(Schema originalSchema) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        if (originalSchema.name() != null) {
            builder.name(originalSchema.name());
        }
        if (originalSchema.version() != null) {
            builder.version(originalSchema.version());
        }
        if (originalSchema.doc() != null) {
            builder.doc(originalSchema.doc());
        }
        if (originalSchema.isOptional()) {
            builder.optional();
        }
        if (originalSchema.parameters() != null && !originalSchema.parameters().isEmpty()) {
            builder.parameters(originalSchema.parameters());
        }
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

    /**
     * Builds a new {@link Struct} conforming to {@code expandedSchema} by copying all field
     * values from {@code originalStruct} and setting {@code columnName} to the current UTC time.
     * When {@code columnName} already exists in the source schema with a non-null
     * {@link Timestamp} value, the source value is preserved unchanged. When the source value is
     * {@code null} and the field is a {@link Timestamp} logical type, the current UTC time is
     * stamped to prevent a {@code null} audit column.
     *
     * @param originalStruct the source struct
     * @param expandedSchema the schema of the new struct (datetime field already included)
     * @return the expanded struct
     */
    private Struct buildExpandedStruct(Struct originalStruct, Schema expandedSchema) {
        final Struct expandedStruct = new Struct(expandedSchema);
        for (Field field : originalStruct.schema().fields()) {
            expandedStruct.put(field.name(), originalStruct.get(field.name()));
        }
        final Field existingField = originalStruct.schema().field(columnName);
        if (existingField == null) {
            expandedStruct.put(columnName, new Date());
        }
        else if (Timestamp.LOGICAL_NAME.equals(existingField.schema().name()) && originalStruct.get(columnName) == null) {
            expandedStruct.put(columnName, new Date());
        }
        return expandedStruct;
    }

    /**
     * Returns the configuration definition for this transformation.
     *
     * @return the {@link ConfigDef} describing all supported properties
     */
    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, COLUMN_NAME);
        return config;
    }

    /**
     * Closes this transformation and releases any held resources.
     */
    @Override
    public void close() {
    }

    /**
     * Returns the version of this transformation, sourced from the connector module version.
     *
     * @return the version string
     */
    @Override
    public String version() {
        return Module.version();
    }
}
