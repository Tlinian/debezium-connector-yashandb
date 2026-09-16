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
