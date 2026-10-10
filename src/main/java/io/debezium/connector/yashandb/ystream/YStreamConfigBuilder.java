/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.sics.ystream.conf.YstreamConfig;

/**
 * Applies connector-configured options to a YStream builder.
 *
 * <p>YStream exposes options as {@code setXxx} methods. The connector accepts dotted option names and resolves them at
 * runtime, so adding a builder option to the YStream client does not require a corresponding connector code change.</p>
 */
public final class YStreamConfigBuilder {

    private YStreamConfigBuilder() {
    }

    /**
     * Applies the given {@code name=value} entries to the YStream builder and returns it for chaining.
     *
     * <p>Each entry is parsed as {@code name=value}; null or blank entries are skipped, malformed or duplicate
     * entries are rejected with an {@link IllegalArgumentException}. The name is resolved to the builder's single-argument
     * {@code setXxx} method at runtime and the value is converted to the setter's parameter type.</p>
     *
     * <p>Callers set the remaining builder options after this method so that dedicated connector settings take
     * precedence over the additional properties.</p>
     *
     * @param builder the YStream builder to configure, must not be null
     * @param entries the list of {@code name=value} entries, may be empty
     * @param <T> the record type of the builder
     * @return the configured builder, for method chaining
     * @throws IllegalArgumentException if an entry is malformed, duplicated, or names an unavailable option
     */
    public static <T> YstreamConfig.Builder<T> apply(YstreamConfig.Builder<T> builder, List<String> entries) {
        Map<String, String> options = new LinkedHashMap<>();
        for (String entry : entries) {
            if (entry == null || entry.trim().isEmpty()) {
                continue;
            }
            int separator = entry.indexOf('=');
            if (separator <= 0) {
                throw new IllegalArgumentException("YStream additional property must use name=value syntax: " + entry);
            }
            String name = entry.substring(0, separator).trim();
            String value = entry.substring(separator + 1).trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException("YStream additional property name must not be empty");
            }
            if (options.put(name, value) != null) {
                throw new IllegalArgumentException("Duplicate YStream additional property: " + name);
            }
        }

        for (Map.Entry<String, String> option : options.entrySet()) {
            apply(builder, option.getKey(), option.getValue());
        }
        return builder;
    }

    /**
     * Resolves the option's setter by reflection, converts the value to the setter's parameter type, and invokes it.
     *
     * @param builder the target builder instance
     * @param optionName the option name in dotted form, e.g. {@code enable.sequence.next.val}
     * @param value the raw string value to convert
     * @throws IllegalArgumentException if the option is unknown or the value cannot be converted or applied
     */
    private static void apply(Object builder, String optionName, String value) {
        Method setter = findSetter(builder.getClass(), optionName);
        if (setter == null) {
            throw new IllegalArgumentException("Unsupported YStream builder option '" + optionName + "'");
        }
        try {
            setter.invoke(builder, convert(value, setter.getParameterTypes()[0]));
        }
        catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot access YStream builder option '" + optionName + "'", e);
        }
        catch (InvocationTargetException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            throw new IllegalArgumentException("Invalid value for YStream builder option '" + optionName + "': " + value,
                    cause);
        }
    }

    /**
     * Finds the single-argument setter method matching the given option name on the builder class.
     *
     * <p>The dotted option name is converted to a setter name in lower camel case, e.g. {@code enable.sequence.next.val}
     * becomes {@code setEnableSequenceNextVal}.</p>
     *
     * @param builderClass the builder class to search, including inherited public methods
     * @param optionName the option name in dotted form
     * @return the matching single-argument setter, or null if none exists
     */
    private static Method findSetter(Class<?> builderClass, String optionName) {
        StringBuilder methodName = new StringBuilder("set");
        for (String part : optionName.split("[.\\-_]")) {
            if (!part.isEmpty()) {
                methodName.append(Character.toUpperCase(part.charAt(0))).append(part.substring(1));
            }
        }
        for (Method method : builderClass.getMethods()) {
            if (method.getName().equals(methodName.toString()) && method.getParameterCount() == 1) {
                return method;
            }
        }
        return null;
    }

    /**
     * Converts the string value to the setter's target parameter type.
     *
     * <p>Supports strings, primitive wrappers, single-character values, and enum constants (matched case-insensitively);
     * conversion failures raise an {@link IllegalArgumentException} that surfaces to the connector configuration.</p>
     *
     * @param value the raw string value
     * @param targetType the setter's parameter type
     * @return the converted value
     * @throws IllegalArgumentException if the value has an invalid format or the target type is unsupported
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    private static Object convert(String value, Class<?> targetType) {
        if (targetType == String.class) {
            return value;
        }
        if (targetType == boolean.class || targetType == Boolean.class) {
            if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
                throw new IllegalArgumentException("Expected true or false");
            }
            return Boolean.parseBoolean(value);
        }
        if (targetType == int.class || targetType == Integer.class) {
            return Integer.parseInt(value);
        }
        if (targetType == long.class || targetType == Long.class) {
            return Long.parseLong(value);
        }
        if (targetType == short.class || targetType == Short.class) {
            return Short.parseShort(value);
        }
        if (targetType == byte.class || targetType == Byte.class) {
            return Byte.parseByte(value);
        }
        if (targetType == double.class || targetType == Double.class) {
            return Double.parseDouble(value);
        }
        if (targetType == float.class || targetType == Float.class) {
            return Float.parseFloat(value);
        }
        if (targetType == char.class || targetType == Character.class) {
            if (value.length() != 1) {
                throw new IllegalArgumentException("Expected a single character");
            }
            return value.charAt(0);
        }
        if (targetType.isEnum()) {
            return Enum.valueOf((Class<? extends Enum>) targetType, value.toUpperCase(Locale.ROOT));
        }
        throw new IllegalArgumentException("Unsupported YStream builder option type: " + targetType.getName());
    }
}
