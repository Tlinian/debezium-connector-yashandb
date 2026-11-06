/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.math.BigInteger;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.connector.yashandb.Scn;
import io.debezium.connector.yashandb.SourceInfo;
import io.debezium.connector.yashandb.YashanDBOffsetContext;

/**
 * The logical encapsulation of raw LCR byte array.
 *
 * @author Jiri Pechanec
 */
public class YStreamPosition implements Comparable<YStreamPosition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamPosition.class);

    private final Position rawPosition;
    private Scn scn;

    public YStreamPosition(Position rawPosition) {
        this.rawPosition = rawPosition;
        this.scn = new Scn(BigInteger.valueOf(rawPosition.getCommitScn().getScn()));
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("LCR pos={} scn={}", format(rawPosition), scn);
        }
    }

    public static YStreamPosition valueOf(long scn) {
        Position position = new Position(new SystemChangeNumber(scn), new LogPosition());
        return new YStreamPosition(position);
    }

    public static YStreamPosition valueOf(String scn) {
        if (scn == null) {
            return null;
        }
        Position position = new Position(new SystemChangeNumber(Long.parseLong(scn)), new LogPosition());
        return new YStreamPosition(position);
    }

    public static YStreamPosition valueOf(Map<String, ?> offset) {
        return new YStreamPosition(Objects.requireNonNull(YashanDBOffsetContext.loadRecoverPosition(offset)));
    }

    public static String format(Position position) {
        if (position == null) {
            return null;
        }
        LogPosition logPosition = position.getLogPosition();
        return position.getCommitScn().getScn() + "-" + logPosition.getInstanceId() + "-" + logPosition.getGroupLsn()
                + "-" + logPosition.getGroupOffset() + "-" + logPosition.getBatchRowId();
    }

    public static String format(Map<String, ?> offset) {
        if (offset == null || !offset.containsKey(SourceInfo.POSITION_SCN_KEY)) {
            return null;
        }
        return offset.get(SourceInfo.POSITION_SCN_KEY) + "-" + offset.get(SourceInfo.INSTANCE_ID_KEY) + "-"
                + offset.get(SourceInfo.GROUP_LSN_KEY) + "-" + offset.get(SourceInfo.GROUP_OFFSET_KEY) + "-"
                + offset.get(SourceInfo.BATCH_ROW_ID_KEY);
    }

    public Position getRawPosition() {
        return rawPosition;
    }

    public Scn getScn() {
        return scn;
    }

    @Override
    public int hashCode() {
        return rawPosition != null ? rawPosition.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        YStreamPosition that = (YStreamPosition) o;

        return Objects.equals(rawPosition, that.rawPosition);
    }

    @Override
    public String toString() {
        return rawPosition.toString();
    }

    @Override
    public int compareTo(YStreamPosition o) {
        if (o == null) {
            return 1;
        }

        return this.rawPosition.compareTo(o.rawPosition);
    }
}
