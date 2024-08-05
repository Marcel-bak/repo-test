/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.statistics;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.temporal.ChronoUnit.MILLIS;

import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.JsonUtil;
import io.delta.kernel.utils.JsonWriter;

/**
 * Statistics about data file in a Delta Lake table.
 */
public class DataFileStatistics {
    private StructType dataSchema;
    private final long numRecords;
    private final Map<Column, Literal> minValues;
    private final Map<Column, Literal> maxValues;
    private final Map<Column, Long> nullCounts;

    public static final int MICROSECONDS_PER_SECOND = 1_000_000;
    public static final int NANOSECONDS_PER_MICROSECOND = 1_000;

    private static final DateTimeFormatter TIMESTAMP_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");


    /**
     * Create a new instance of {@link DataFileStatistics}.
     *
     * @param dataSchema Schema of the data file.
     * @param numRecords Number of records in the data file.
     * @param minValues  Map of column to minimum value of it in the data file. If the data file has
     *                   all nulls for the column, the value will be null or not present in the
     *                   map.
     * @param maxValues  Map of column to maximum value of it in the data file. If the data file has
     *                   all nulls for the column, the value will be null or not present in the
     *                   map.
     * @param nullCounts Map of column to number of nulls in the data file.
     */
    public DataFileStatistics(
        StructType dataSchema,
        long numRecords,
        Map<Column, Literal> minValues,
        Map<Column, Literal> maxValues,
        Map<Column, Long> nullCounts) {
        this.dataSchema = dataSchema;
        this.numRecords = numRecords;
        this.minValues = Collections.unmodifiableMap(minValues);
        this.maxValues = Collections.unmodifiableMap(maxValues);
        this.nullCounts = Collections.unmodifiableMap(nullCounts);
    }

    /**
     * Get the number of records in the data file.
     *
     * @return Number of records in the data file.
     */
    public long getNumRecords() {
        return numRecords;
    }

    /**
     * Get the minimum values of the columns in the data file. The map may contain statistics for
     * only a subset of columns in the data file.
     *
     * @return Map of column to minimum value of it in the data file.
     */
    public Map<Column, Literal> getMinValues() {
        return minValues;
    }

    /**
     * Get the maximum values of the columns in the data file. The map may contain statistics for
     * only a subset of columns in the data file.
     *
     * @return Map of column to minimum value of it in the data file.
     */
    public Map<Column, Literal> getMaxValues() {
        return maxValues;
    }

    /**
     * Get the number of nulls of columns in the data file. The map may contain statistics for only
     * a subset of columns in the data file.
     *
     * @return Map of column to number of nulls in the data file.
     */
    public Map<Column, Long> getNullCounts() {
        return nullCounts;
    }

    public String serializeAsJson() {
        return JsonUtil.generate(gen -> {
            gen.writeStartObject();
            gen.writeNumberField("numRecords", numRecords);

            gen.writeObjectFieldStart("minValues");
            // TODO: Prune dataSchema to a statsSchema instead?
            writeJsonValues(gen, dataSchema, minValues, new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("maxValues");
            writeJsonValues(gen, dataSchema, maxValues, new Column(new String[0]),
                (g, v) -> writeJsonValue(g, v));
            gen.writeEndObject();

            gen.writeObjectFieldStart("nullCounts");
            writeJsonValues(gen, dataSchema, nullCounts, new Column(new String[0]),
                (g, v) -> g.writeNumber(v));
            gen.writeEndObject();

            gen.writeEndObject();
        });
    }

    private <T> void writeJsonValues(JsonWriter jsonWriter,
                                     StructType schema,
                                     Map<Column, T> values,
                                     Column parentColPath,
                                     JsonUtil.JsonValueWriter<T> writer) throws IOException {
        for (StructField field : schema.fields()) {
            Column colPath = parentColPath.append(field.getName());
            if (field.getDataType() instanceof StructType) {
                jsonWriter.writeObjectFieldStart(field.getName());
                writeJsonValues(jsonWriter, (StructType) field.getDataType(), values, colPath,
                    writer);
                jsonWriter.writeEndObject();
            } else {
                T value = values.get(colPath);
                if (value != null) {
                    jsonWriter.writeFieldName(field.getName());
                    writer.write(jsonWriter, value);
                }
            }
        }
    }

    private void writeJsonValue(JsonWriter jsonWriter,
                                Literal literal) throws IOException {
        if (literal == null || literal.getValue() == null) {
            return;
        }
        DataType type = literal.getDataType();
        Object value = literal.getValue();
        if (type instanceof BooleanType) {
            jsonWriter.writeBoolean((Boolean) value);
        } else if (type instanceof ByteType) {
            jsonWriter.writeNumber(((Number) value).byteValue());
        } else if (type instanceof ShortType) {
            jsonWriter.writeNumber(((Number) value).shortValue());
        } else if (type instanceof IntegerType) {
            jsonWriter.writeNumber(((Number) value).intValue());
        } else if (type instanceof LongType) {
            jsonWriter.writeNumber(((Number) value).longValue());
        } else if (type instanceof FloatType) {
            jsonWriter.writeNumber(((Number) value).floatValue());
        } else if (type instanceof DoubleType) {
            jsonWriter.writeNumber(((Number) value).doubleValue());
        } else if (type instanceof StringType) {
            jsonWriter.writeString((String) value);
        } else if (type instanceof BinaryType) {
            jsonWriter.writeString(new String((byte[]) value, StandardCharsets.UTF_8));
        } else if (type instanceof DecimalType) {
            jsonWriter.writeNumber((BigDecimal) value);
        } else if (type instanceof DateType) {
            jsonWriter.writeString(
                LocalDate.ofEpochDay(((Number) value).longValue()).format(ISO_LOCAL_DATE));
        } else if (type instanceof TimestampType || type instanceof TimestampNTZType) {
            long epochMicros = (long) value;
            long epochSeconds = epochMicros / MICROSECONDS_PER_SECOND;
            int nanoAdjustment =
                (int) (epochMicros % MICROSECONDS_PER_SECOND) * NANOSECONDS_PER_MICROSECOND;
            if (nanoAdjustment < 0) {
                nanoAdjustment += MICROSECONDS_PER_SECOND * NANOSECONDS_PER_MICROSECOND;
            }
            Instant instant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
            jsonWriter.writeString(TIMESTAMP_FORMATTER.format(
                ZonedDateTime.ofInstant(instant.truncatedTo(MILLIS), UTC)));
        } else {
            throw new IllegalArgumentException("Unsupported stats data type: " + type);
        }
    }
}
