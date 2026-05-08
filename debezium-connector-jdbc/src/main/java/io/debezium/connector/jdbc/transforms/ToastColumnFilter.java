/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.transforms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
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
 * A Kafka Connect SMT (Single Message Transformation) that removes fields carrying the
 * PostgreSQL TOAST unavailable-value sentinel from a {@link ConnectRecord}'s value schema
 * and struct before the record reaches the JDBC sink connector.
 *
 * <h2>Background: PostgreSQL TOAST and Debezium</h2>
 * <p>
 * PostgreSQL stores large column values out-of-line using its TOAST (The Oversized-Attribute
 * Storage Technique) mechanism. When an {@code UPDATE} statement does not modify a TOASTed
 * column, the WAL record omits that column entirely — the old value is not re-read from the
 * heap. Debezium fills this gap by substituting a configurable sentinel string, defaulting to
 * {@code __debezium_unavailable_value}, so that downstream consumers always receive a complete
 * set of field names.
 * </p>
 *
 * <h2>Why this SMT is needed</h2>
 * <p>
 * Without this SMT, the sentinel string is passed directly to the JDBC sink connector. For
 * semi-structured column types such as Snowflake {@code VARIANT} or {@code ARRAY}, the
 * connector wraps the bound value in {@code PARSE_JSON(?)}, causing Snowflake to fail with:
 * <pre>
 *   SnowflakeSQLException: Error parsing JSON: unknown keyword "__debezium_unavailable_value"
 * </pre>
 * Rather than silently overwriting the column with {@code NULL}, the connector throws a
 * {@code DataException} with actionable remediation guidance. This SMT is one of the three
 * documented remediation paths.
 * </p>
 *
 * <h2>How it works</h2>
 * <p>
 * On each record the SMT first determines whether the value {@link Struct} is a full Debezium
 * change event envelope or an already-unwrapped (flat) record.
 * </p>
 * <p>
 * <b>Envelope detection:</b> if the top-level struct contains a field named {@code after}
 * whose schema type is {@link Schema.Type#STRUCT}, the record is treated as a Debezium change
 * event envelope. The SMT inspects the fields of the {@code after} sub-struct for the TOAST
 * sentinel. If TOAST fields are found, only the {@code after} sub-struct is rebuilt without
 * those fields; the outer envelope is reconstructed with the reduced {@code after} schema. All
 * other envelope fields ({@code before}, {@code op}, {@code source}, {@code ts_ms}, etc.) pass
 * through unmodified. This is the typical case when no {@code ExtractNewRecordState} SMT is
 * applied upstream.
 * </p>
 * <p>
 * <b>Flat record:</b> if no {@code after}-typed-as-STRUCT field is present, the SMT inspects
 * the top-level fields directly. This is the case when {@code ExtractNewRecordState} has
 * already been applied before this SMT in the transforms chain, and it must be ordered
 * <em>before</em> this SMT so that the record value is already the unwrapped struct.
 * </p>
 * <p>
 * In both cases: if no TOAST fields are found the original record is returned unchanged (zero
 * allocation fast path).
 * </p>
 * <p>
 * When TOAST fields are removed, the modified schema causes the JDBC sink connector's internal
 * record buffer to flush the current batch and start a new one. The {@code MERGE}/{@code UPDATE}
 * SQL generated for a TOAST batch does not reference the stripped column, so the existing
 * database value is preserved.
 * </p>
 *
 * <h2>Configuration</h2>
 * <table border="1">
 *   <tr><th>Property</th><th>Default</th><th>Description</th></tr>
 *   <tr>
 *     <td>{@code unavailable.value.placeholder}</td>
 *     <td>{@code __debezium_unavailable_value}</td>
 *     <td>
 *       The sentinel string that identifies an unavailable TOAST column value. Must match the
 *       {@code unavailable.value.placeholder} configured on the Debezium source connector.
 *     </td>
 *   </tr>
 * </table>
 *
 * <h2>Connector configuration example</h2>
 * <pre>
 * transforms=stripToast
 * transforms.stripToast.type=io.debezium.connector.jdbc.transforms.ToastColumnFilter
 * # Only needed if the source connector uses a non-default placeholder:
 * # transforms.stripToast.unavailable.value.placeholder=__debezium_unavailable_value
 * </pre>
 *
 * @author Marinus Krommenhoek
 * @param <R> the record type
 */
public class ToastColumnFilter<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    private static final Logger LOGGER = LoggerFactory.getLogger(ToastColumnFilter.class);

    static final String DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER = "__debezium_unavailable_value";

    private static final String UNAVAILABLE_VALUE_PLACEHOLDER_PARAM = "unavailable.value.placeholder";

    private static final io.debezium.config.Field UNAVAILABLE_VALUE_PLACEHOLDER = io.debezium.config.Field
            .create(UNAVAILABLE_VALUE_PLACEHOLDER_PARAM)
            .withDisplayName("Unavailable Value Placeholder")
            .withType(ConfigDef.Type.STRING)
            .withDefault(DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription(
                    "The sentinel string emitted by the Debezium source connector for PostgreSQL TOAST columns " +
                            "whose values were not included in the WAL record. " +
                            "Must match the 'unavailable.value.placeholder' setting on the source connector. " +
                            "Defaults to '" + DEFAULT_UNAVAILABLE_VALUE_PLACEHOLDER + "'.");

    private String unavailableValuePlaceholder;

    /**
     * Configures this transformation.
     * <p>
     * Reads the {@code unavailable.value.placeholder} property (default:
     * {@code __debezium_unavailable_value}) and stores it for use in {@link #apply}.
     * </p>
     *
     * @param configs the connector configuration properties
     */
    @Override
    public void configure(Map<String, ?> configs) {
        final Configuration config = Configuration.from(configs);
        this.unavailableValuePlaceholder = config.getString(UNAVAILABLE_VALUE_PLACEHOLDER);
        LOGGER.info("Configured with unavailable.value.placeholder='{}'", unavailableValuePlaceholder);
    }

    /**
     * Applies the TOAST field filter to the given record.
     * <p>
     * The following cases are handled:
     * <ul>
     *   <li><b>Tombstone</b> ({@code record.value() == null}): returned unchanged.</li>
     *   <li><b>Non-Struct value</b>: returned unchanged.</li>
     *   <li><b>Debezium envelope, {@code after} is non-null with TOAST fields</b>: a new record
     *       is returned whose {@code after} sub-schema and sub-struct no longer contain the
     *       sentinel-valued fields; the outer envelope schema is also rebuilt to reference the
     *       reduced {@code after} schema.</li>
     *   <li><b>Debezium envelope, {@code after} is null</b> (DELETE event): returned unchanged.</li>
     *   <li><b>Flat record, no TOAST fields</b>: returned unchanged (zero allocation fast path).</li>
     *   <li><b>Flat record, one or more TOAST fields</b>: a new record is returned whose value
     *       schema and struct no longer contain the sentinel-valued fields.</li>
     * </ul>
     * </p>
     *
     * @param record the record to transform
     * @return the original record if no TOAST fields are present, otherwise a new record
     *         with the TOAST fields removed
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
        if (afterField != null && afterField.schema().type() == Schema.Type.STRUCT) {
            return applyToEnvelope(record, originalStruct);
        }

        return applyToFlatRecord(record, originalStruct);
    }

    /**
     * Applies TOAST filtering to a full Debezium change event envelope. Only the {@code after}
     * sub-struct is inspected; all other envelope fields are preserved as-is.
     *
     * @param record   the original record
     * @param envelope the top-level envelope struct
     * @return the original record if {@code after} is null or contains no TOAST fields,
     *         otherwise a new record with a reduced {@code after} sub-struct
     */
    private R applyToEnvelope(R record, Struct envelope) {
        final Struct after = (Struct) envelope.get("after");
        if (after == null) {
            return record;
        }

        final List<String> toastFieldNames = collectToastFieldNames(after);
        if (toastFieldNames.isEmpty()) {
            return record;
        }

        LOGGER.debug("Stripping {} TOAST field(s) from envelope 'after' on topic '{}': {}",
                toastFieldNames.size(), record.topic(), toastFieldNames);

        final Schema reducedAfterSchema = buildReducedSchema(after.schema(), toastFieldNames);
        final Struct reducedAfterStruct = buildReducedStruct(after, reducedAfterSchema);

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
        for (Field field : originalEnvelopeSchema.fields()) {
            envelopeBuilder.field(field.name(),
                    "after".equals(field.name()) ? reducedAfterSchema : field.schema());
        }
        final Schema reducedEnvelopeSchema = envelopeBuilder.build();

        final Struct reducedEnvelope = new Struct(reducedEnvelopeSchema);
        for (Field field : reducedEnvelopeSchema.fields()) {
            reducedEnvelope.put(field.name(),
                    "after".equals(field.name()) ? reducedAfterStruct : envelope.get(field.name()));
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                reducedEnvelopeSchema,
                reducedEnvelope,
                record.timestamp(),
                record.headers());
    }

    /**
     * Applies TOAST filtering to a flat (already-unwrapped) record whose top-level fields
     * are the column values directly.
     *
     * @param record         the original record
     * @param originalStruct the flat value struct
     * @return the original record if no TOAST fields are present, otherwise a new record
     *         with the TOAST fields removed from both the schema and the struct
     */
    private R applyToFlatRecord(R record, Struct originalStruct) {
        final List<String> toastFieldNames = collectToastFieldNames(originalStruct);

        if (toastFieldNames.isEmpty()) {
            return record;
        }

        LOGGER.debug("Stripping {} TOAST field(s) from record on topic '{}': {}",
                toastFieldNames.size(), record.topic(), toastFieldNames);

        final Schema reducedSchema = buildReducedSchema(originalStruct.schema(), toastFieldNames);
        final Struct reducedStruct = buildReducedStruct(originalStruct, reducedSchema);

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                reducedSchema,
                reducedStruct,
                record.timestamp(),
                record.headers());
    }

    /**
     * Iterates over all fields in the given struct and returns the names of those whose
     * string value equals the configured unavailable-value placeholder.
     *
     * @param struct the value struct to inspect
     * @return a list of field names that carry the TOAST sentinel; empty if none
     */
    private List<String> collectToastFieldNames(Struct struct) {
        final List<String> toastFields = new ArrayList<>();
        for (Field field : struct.schema().fields()) {
            final Object fieldValue = struct.get(field);
            if (unavailableValuePlaceholder.equals(fieldValue)) {
                toastFields.add(field.name());
            }
        }
        return toastFields;
    }

    /**
     * Builds a new {@link Schema} that is identical to {@code originalSchema} except that
     * fields in {@code toastFieldNames} are omitted.
     * <p>
     * The schema name, version, and doc are copied from the original so that downstream
     * components that rely on schema identity (e.g. schema registries) see a consistent
     * lineage.
     * </p>
     *
     * @param originalSchema  the schema to copy
     * @param toastFieldNames names of fields to exclude
     * @return the reduced schema
     */
    private Schema buildReducedSchema(Schema originalSchema, List<String> toastFieldNames) {
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
        for (Field field : originalSchema.fields()) {
            if (!toastFieldNames.contains(field.name())) {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    /**
     * Builds a new {@link Struct} conforming to {@code reducedSchema} by copying all field
     * values from {@code originalStruct}. Fields absent from {@code reducedSchema} (i.e. the
     * TOAST fields) are not copied.
     *
     * @param originalStruct the source struct
     * @param reducedSchema  the schema of the new struct (TOAST fields already removed)
     * @return the reduced struct
     */
    private Struct buildReducedStruct(Struct originalStruct, Schema reducedSchema) {
        final Struct reducedStruct = new Struct(reducedSchema);
        for (Field field : reducedSchema.fields()) {
            reducedStruct.put(field.name(), originalStruct.get(field.name()));
        }
        return reducedStruct;
    }

    /**
     * Returns the configuration definition for this transformation.
     *
     * @return the {@link ConfigDef} describing all supported properties
     */
    @Override
    public ConfigDef config() {
        final ConfigDef config = new ConfigDef();
        io.debezium.config.Field.group(config, null, UNAVAILABLE_VALUE_PLACEHOLDER);
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
