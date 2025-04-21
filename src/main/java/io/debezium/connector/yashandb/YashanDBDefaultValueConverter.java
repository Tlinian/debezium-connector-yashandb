/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import com.yashandb.jdbc.YasTypes;
import io.debezium.annotation.Immutable;
import io.debezium.annotation.ThreadSafe;
import io.debezium.relational.Column;
import io.debezium.relational.DefaultValueConverter;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Collect;
import io.debezium.util.Strings;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Time;
import java.sql.Types;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Chris Cranford
 */
@ThreadSafe
@Immutable
public class YashanDBDefaultValueConverter implements DefaultValueConverter {

    private static Logger LOGGER = LoggerFactory.getLogger(YashanDBDefaultValueConverter.class);

    private final YashanDBValueConverters valueConverters;
    private final Map<Integer, DefaultValueMapper> defaultValueMappers;

    public YashanDBDefaultValueConverter(YashanDBValueConverters valueConverters, YashanDBConnection jdbcConnection) {
        this.valueConverters = valueConverters;
        this.defaultValueMappers = Collections.unmodifiableMap(createDefaultValueMappers(jdbcConnection));
    }

    @Override
    public Optional<Object> parseDefaultValue(Column column, String defaultValue) {
        final int dataType = column.jdbcType();
        final DefaultValueMapper mapper = defaultValueMappers.get(dataType);
        if (mapper == null) {
            LOGGER.warn("Mapper for type '{}' not found.", dataType);
            return Optional.empty();
        }

        try {
            Object rawDefaultValue = mapper.parse(column, defaultValue != null ? defaultValue.trim() : defaultValue);
            Object convertedDefaultValue = convertDefaultValue(rawDefaultValue, column);
            if (convertedDefaultValue instanceof Struct) {
                // Workaround for KAFKA-12694
                LOGGER.warn("Struct can't be used as default value for column '{}', will use null instead.", column.name());
                return Optional.empty();
            }
            return Optional.ofNullable(convertedDefaultValue);
        } catch (Exception e) {
            LOGGER.warn("Cannot parse column default value '{}' to type '{}'.  Expression evaluation is not supported.", defaultValue, dataType);
            LOGGER.debug("Parsing failed due to error", e);
            return Optional.empty();
        }
    }

    private Object convertDefaultValue(Object defaultValue, Column column) {
        // if converters is not null and the default value is not null, we need to convert default value
        if (valueConverters != null && defaultValue != null) {
            final SchemaBuilder schemaBuilder = valueConverters.schemaBuilder(column);
            if (schemaBuilder != null) {
                final Schema schema = schemaBuilder.build();
                // In order to get the valueConverter for this column, we have to create a field;
                // The index value -1 in the field will never used when converting default value;
                // So we can set any number here;
                final Field field = new Field(column.name(), -1, schema);
                final ValueConverter valueConverter = valueConverters.converter(column, field);

                Object result = valueConverter.convert(defaultValue);
                if ((result instanceof BigDecimal) && column.scale().isPresent() && column.scale().get() > ((BigDecimal) result).scale()) {
                    // Note as the scale is increased only, the rounding is more cosmetic.
                    result = ((BigDecimal) result).setScale(column.scale().get(), RoundingMode.HALF_EVEN);
                }
                return result;
            }
        }
        return defaultValue;
    }

    private static Map<Integer, DefaultValueMapper> createDefaultValueMappers(YashanDBConnection jdbcConnection) {
        // Data types that are supported should be registered in the map. Many of the data types
        // have String-based conversions defined in OracleValueConverters since LogMiner provides
        // column values as strings. The only special handling that is needed here is if a type
        // is formatted with unique characteristics such as single/double quotes for strings.
        //
        // Additionally, we use the OracleTypes numeric representation for data types rather than
        // the type name like we do for SQL Server since the type names can include precision
        // and scale, i.e. TIMESTAMP(6) or INTERVAL YEAR(2) TO MONTH.
        final Map<Integer, DefaultValueMapper> result = new HashMap<>();

        // Numeric types
        result.put(YasTypes.NUMERIC, nullableDefaultValueMapper());
        result.put(YasTypes.DECIMAL, nullableDefaultValueMapper());
        result.put(YasTypes.INTEGER, nullableDefaultValueMapper());

        // Approximate numerics
        result.put(YasTypes.BINARY_FLOAT, nullableDefaultValueMapper());
        result.put(YasTypes.FLOAT, nullableDefaultValueMapper());
        result.put(YasTypes.BIGINT, nullableDefaultValueMapper());
        result.put(YasTypes.SMALLINT, nullableDefaultValueMapper());
        result.put(YasTypes.TINYINT, nullableDefaultValueMapper());
        result.put(YasTypes.BOOLEAN, nullableDefaultValueMapper());
        result.put(YasTypes.REAL, nullableDefaultValueMapper());
        result.put(YasTypes.DOUBLE, nullableDefaultValueMapper());

        // Date and time
        result.put(YasTypes.DATE, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(YasTypes.TIME, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(YasTypes.TIMESTAMP, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(YasTypes.TIMESTAMP_TZ, nullableDefaultValueMapper(castTemporalFunctionCall(jdbcConnection)));
        result.put(YasTypes.YM_INTERVAL, nullableDefaultValueMapper(convertIntervalYearMonthStringLiteral()));
        result.put(YasTypes.DS_INTERVAL, nullableDefaultValueMapper(convertIntervalDaySecondStringLiteral()));

        // Character strings
        result.put(YasTypes.CHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(YasTypes.VARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Unicode character strings
        result.put(YasTypes.NCHAR, nullableDefaultValueMapper(enforceCharFieldPadding()));
        result.put(YasTypes.NVARCHAR, nullableDefaultValueMapper(enforceStringUnquote()));

        // Other data types have been omitted.
        return result;
    }

    private static DefaultValueMapper nullableDefaultValueMapper() {
        return nullableDefaultValueMapper(null);
    }

    private static DefaultValueMapper nullableDefaultValueMapper(DefaultValueMapper mapper) {
        return (column, value) -> {
            if ("NULL".equalsIgnoreCase(value)) {
                return null;
            }
            if (mapper != null) {
                return mapper.parse(column, value);
            }
            return value;
        };
    }

    private static DefaultValueMapper convertIntervalDaySecondStringLiteral() {
        return (column, value) -> {
            return value;
        };
    }

    private static DefaultValueMapper convertIntervalYearMonthStringLiteral() {
        return (column, value) -> {
            return value;
        };
    }

    private static DefaultValueMapper enforceCharFieldPadding() {
        return (column, value) -> value != null ? Strings.pad(unquote(value), column.length(), ' ') : null;
    }

    private static DefaultValueMapper enforceStringUnquote() {
        return (column, value) -> value != null ? unquote(value) : null;
    }

    private static DefaultValueMapper castTemporalFunctionCall(YashanDBConnection jdbcConnection) {
        return (column, value) -> {
            if ("SYSDATE".equalsIgnoreCase(value.trim())) {
                if (column.isOptional()) {
                    // If the column is optional, the default value is ignored
                    return null;
                } else if (column.jdbcType() == YasTypes.TIMESTAMP_TZ) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                } else if (column.jdbcType() == YasTypes.DATE) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Date.from(Instant.EPOCH);
                } else if (column.jdbcType() == YasTypes.TIME) {
                    // If the column is a TIMESTAMP WITH [LOCAL] TIME ZONE, the non-null default is based on EPOCH
                    return Time.from(Instant.EPOCH);
                } else {
                    // For all other temporal types, return "0".
                    // The return is a string-value as the OracleValueConverters know how to explicitly infer
                    // whether to emit the final converted value as either a string or numeric value based on
                    // the column's data type.
                    return "0";
                }
            }
            return value;
        };
    }

    private static String unquote(String value) {
        if (value.startsWith("('") && value.endsWith("')")) {
            return value.substring(2, value.length() - 2);
        }
        if (value.startsWith("'") && value.endsWith("'")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
