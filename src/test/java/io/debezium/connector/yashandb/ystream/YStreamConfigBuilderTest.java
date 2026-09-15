/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

import com.sics.ystream.conf.YstreamConfig;

class YStreamConfigBuilderTest {

    @Test
    void shouldApplyNewBuilderOptionsWithoutConnectorCodeChanges() {
        YstreamConfig.Builder<YStreamRecord> builder = YstreamConfig.<YStreamRecord> builder()
                .setHost("localhost")
                .setPort("1688")
                .setUser("user")
                .setPassword("password")
                .setDeserializer(new YStreamDeserializer())
                .setServerName("server");

        YStreamConfigBuilder.apply(builder, Arrays.asList("enable.sequence.next.val=true", "mysql.mode=true", "queue.size=256"));

        YstreamConfig<YStreamRecord> config = builder.build();
        assertThat(config.isEnableSequenceNextVal()).isTrue();
        assertThat(config.isMysqlMode()).isTrue();
        assertThat(config.getQueueSize()).isEqualTo(256);
    }

    @Test
    void shouldRejectUnknownBuilderOption() {
        YstreamConfig.Builder<YStreamRecord> builder = YstreamConfig.<YStreamRecord> builder();

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> YStreamConfigBuilder.apply(builder, Arrays.asList("does.not.exist=true")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported YStream builder option");
    }

    @Test
    void shouldRejectInvalidBooleanValue() {
        YstreamConfig.Builder<YStreamRecord> builder = YstreamConfig.<YStreamRecord> builder();

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> YStreamConfigBuilder.apply(builder, Arrays.asList("mysql.mode=enabled")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expected true or false");
    }
}
