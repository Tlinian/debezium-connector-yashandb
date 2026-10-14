/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import com.sics.ystream.conf.YstreamConfig;

/**
 * Guards the reflective mapping of {@code ystream.additional.properties} entries to YStream builder setters.
 */
class YStreamConfigBuilderTest {

    @Test
    void shouldApplyBooleanOptionToBuilder() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonList("fault.tolerance=true"));
        assertThat(builder.build().getFaultTolerance()).isTrue();
    }

    @Test
    void shouldMapDottedNameToCamelCaseSetter() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonList("enable.sequence.next.val=true"));
        assertThat(builder.build().isEnableSequenceNextVal()).isTrue();
    }

    @Test
    void shouldRejectMalformedEntry() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonList("no-equals-sign")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name=value");
    }

    @Test
    void shouldRejectUnknownOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonList("no.such.option=true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported YStream builder option 'no.such.option'");
    }

    @Test
    void shouldRejectDuplicateOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Arrays.asList("fault.tolerance=true", "fault.tolerance=false")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate YStream additional property: fault.tolerance");
    }

    @Test
    void shouldRejectInvalidBooleanValue() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonList("fault.tolerance=maybe")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected true or false");
    }

    /**
     * A builder with all values that {@link YstreamConfig} validates at build time, so tests can assert on the built config.
     */
    private static YstreamConfig.Builder<String> minimalBuilder() {
        return YstreamConfig.<String> builder()
                .setHost("localhost")
                .setPort("1688")
                .setUser("user")
                .setPassword("password")
                .setServerName("server")
                .setDeserializer((lcr, metadata) -> null);
    }
}