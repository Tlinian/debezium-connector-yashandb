/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.sics.ystream.conf.YstreamConfig;

/**
 * Guards the reflective mapping of {@code ystream.additional.properties.*} options to YStream builder setters.
 */
class YStreamConfigBuilderTest {

    @Test
    void shouldApplyBooleanOptionToBuilder() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("fault.tolerance", "true"));
        assertThat(builder.build().getFaultTolerance()).isTrue();
    }

    @Test
    void shouldDeriveMysqlModeFromAdditionalProperties() {
        assertThat(YStreamConfigBuilder.mysqlMode(Collections.emptyMap())).isFalse();
        assertThat(YStreamConfigBuilder.mysqlMode(Collections.singletonMap("mysql.mode", "false"))).isFalse();
        assertThat(YStreamConfigBuilder.mysqlMode(Collections.singletonMap("mysql.mode", "true"))).isTrue();
        assertThat(YStreamConfigBuilder.mysqlMode(Collections.singletonMap("MYSQL_MODE", "true"))).isTrue();

        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("mysql.mode", "true"));
        assertThat(builder.build().isMysqlMode()).isTrue();

        YstreamConfig.Builder<String> aliasBuilder = minimalBuilder();
        YStreamConfigBuilder.apply(aliasBuilder, Collections.singletonMap("MYSQL_MODE", "true"));
        assertThat(aliasBuilder.build().isMysqlMode()).isTrue();
    }

    @Test
    void shouldRejectInvalidMysqlModeAndAliasConflict() {
        assertThatThrownBy(() -> YStreamConfigBuilder.mysqlMode(Collections.singletonMap("mysql.mode", "maybe")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected true or false");
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("mysql.mode", "true");
        aliases.put("mysql_mode", "false");
        assertThatThrownBy(() -> YStreamConfigBuilder.mysqlMode(aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate YStream additional property");
    }

    @Test
    void shouldMapDottedNameToCamelCaseSetter() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        YStreamConfigBuilder.apply(builder, Collections.singletonMap("enable.sequence.next.val", "true"));
        assertThat(builder.build().isEnableSequenceNextVal()).isTrue();
    }

    @Test
    void shouldRejectEmptyOptionName() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("name must not be empty");
    }

    @Test
    void shouldRejectUnknownOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("no.such.option", "true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported YStream builder option 'no.such.option'");
    }

    @Test
    void shouldRejectDuplicateOption() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        Map<String, String> aliases = new LinkedHashMap<>();
        aliases.put("fault.tolerance", "true");
        aliases.put("fault_tolerance", "false");
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, aliases))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate YStream additional property: fault_tolerance");
    }

    @Test
    void shouldRejectInvalidBooleanValue() {
        YstreamConfig.Builder<String> builder = minimalBuilder();
        assertThatThrownBy(() -> YStreamConfigBuilder.apply(builder, Collections.singletonMap("fault.tolerance", "maybe")))
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
