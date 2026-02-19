/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.jdbc.junit.jupiter.snowflake;

import java.util.Properties;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import io.debezium.connector.jdbc.junit.jupiter.SinkType;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSink;
import io.debezium.connector.jdbc.junit.jupiter.e2e.SkipWhenSinks;
import io.debezium.util.Strings;

/**
 * An abstract extension for providing a {@link Sink} parameter object to test methods.
 *
 * The usual AbstractSinkDatabaseContextProvider class is not used for Snowflake because there is no testcontainers container for Snowflake.
 *
 * To configure the sink, we use the following environment variables:
 * - SNOWFLAKE_SCHEMA_NAME: the name of the schema to use for the tests
 * - SNOWFLAKE_DB_NAME: the name of the database to use for the tests
 * - SNOWFLAKE_ACCOUNT_IDENTIFIER: the account identifier of the Snowflake account
 * - SNOWFLAKE_USERNAME: the username to use for the tests
 * - SNOWFLAKE_PASSWORD: the password to use for the tests
 *
 * The password is a PAT (Programmatic Access Token) generated in Snowflake.
 *
 * The schema is created and dropped using a separate setupSink since it allows executing the SQL statements, but starts its session in the public schema.
 *
 * The sink is used to execute the tests in the schema specified by the environment variables.
 *
 * NOTE: the snowflake jdbc url is expected to be of the format "jdbc:snowflake://<account_identifier>.snowflakecomputing.com/?db=<db_name>&schema=<schema_name>"
 *
 * @author Marinus Krommenhoek
 */
public abstract class AbstractSinkDatabaseContextProvider implements BeforeAllCallback, AfterAllCallback, ParameterResolver, ExecutionCondition {

    private final SinkType sinkType;
    private final Sink sink;
    private final Sink setupSink;

    private String dbName;
    private String schemaName;
    private String accountIdentifier;

    public AbstractSinkDatabaseContextProvider() {
        this.sinkType = SinkType.SNOWFLAKE;
        // example: String url = "jdbc:snowflake://MWQFSBB-NE54452.snowflakecomputing.com/?db=DEV&schema=E2E_TESTS";
        this.schemaName = System.getenv("SNOWFLAKE_SCHEMA_NAME");
        this.dbName = System.getenv("SNOWFLAKE_DB_NAME");
        this.accountIdentifier = System.getenv("SNOWFLAKE_ACCOUNT_IDENTIFIER");
        Properties properties = new Properties();
        // example: properties.setProperty("user", "DEV_SERVICE_USER");
        properties.setProperty("user", System.getenv("SNOWFLAKE_USERNAME"));
        // NOTE: the password here is a PAT (Programmatic Access Token) generated in Snowflake
        properties.setProperty("password", System.getenv("SNOWFLAKE_PASSWORD"));
        // NOTE: for deployment it is necessary that the "hibernate.dialect" is set to "io.debezium.connector.jdbc.hibernate.dialect.snowflake.SnowflakeDialect"
        properties.setProperty("hibernate.dialect", "io.debezium.connector.jdbc.hibernate.dialect.snowflake.SnowflakeDialect");

        // NOTE: the sink is used to execute the tests
        this.sink = new Sink(
                String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s", this.accountIdentifier, this.dbName, this.schemaName),
                properties);
        // NOTE: the setup sink is used to create the schema for the test
        this.setupSink = new Sink(
                String.format("jdbc:snowflake://%s.snowflakecomputing.com/?db=%s&schema=%s", this.accountIdentifier, this.dbName, "PUBLIC"),
                properties);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        this.setupSink.execute(String.format("DROP SCHEMA IF EXISTS %s.%s CASCADE;", this.dbName, this.schemaName));
        this.setupSink.execute(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s;", this.dbName, this.schemaName));
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        sink.close();
        setupSink.close();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return parameterContext.getParameter().getType() == Sink.class;
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return sink;
    }

    @Override
    public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
        if (context.getTestMethod().isPresent()) {
            final SkipWhenSinks skipWhenSinks = context.getRequiredTestMethod().getAnnotation(SkipWhenSinks.class);
            if (skipWhenSinks != null) {
                for (SkipWhenSink skipWhenSink : skipWhenSinks.value()) {
                    if (isSkipped(skipWhenSink)) {
                        return getSkippedEvaluationResult(skipWhenSink);
                    }
                }
            }

            final SkipWhenSink skipWhenSink = context.getRequiredTestMethod().getAnnotation(SkipWhenSink.class);
            if (isSkipped(skipWhenSink)) {
                return getSkippedEvaluationResult(skipWhenSink);
            }
        }
        return ConditionEvaluationResult.enabled("Not annotated with SkipWhenSink for " + sinkType);
    }

    protected Sink getSink() {
        return sink;
    }

    private boolean isSkipped(SkipWhenSink skipWhenSink) {
        if (skipWhenSink != null) {
            for (SinkType sinkType : skipWhenSink.value()) {
                if (sinkType == this.sinkType) {
                    return true;
                }
            }
        }
        return false;
    }

    private ConditionEvaluationResult getSkippedEvaluationResult(SkipWhenSink skipWhenSink) {
        if (Strings.isNullOrBlank(skipWhenSink.reason())) {
            return ConditionEvaluationResult.disabled("Annotated with SkipWhenSink for " + sinkType);
        }
        return ConditionEvaluationResult.disabled("Skipped: " + skipWhenSink.reason());
    }

}
