/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class StringWriterJsonWriter implements JsonWriter {

    private final StringWriter writer;
    private boolean needsComma = false;

    public StringWriterJsonWriter(StringWriter stringWriter) {
        checkArgument(stringWriter != null, "stringWriter cannot be null");
        this.writer = stringWriter;
    }

    @Override
    public void writeStartObject() {
        writeCommaIfNeeded();
        writer.write('{');
        needsComma = false;
    }

    @Override
    public void writeEndObject() {
        writer.write('}');
        needsComma = true;
    }

    @Override
    public void writeFieldName(String name) {
        writeWithCommaHandling(() -> writer.write("\"" + JsonUtil.escapeJson(name) + "\":"));
        needsComma = false;
    }

    @Override
    public void writeNumberField(String name, long value) {
        writeFieldName(name);
        writeNumber(value);
    }

    @Override
    public void writeStringField(String name, String value) {
        writeFieldName(name);
        writeString(value);
    }

    @Override
    public void writeObjectFieldStart(String name) {
        writeWithCommaHandling(() -> {
            writer.write('\"');
            JsonUtil.escapeJson(name);
            writer.write("\":{");
        });
        needsComma = false;
    }

    @Override
    public void writeBoolean(boolean value) {
        writeWithCommaHandling(() -> writer.write(Boolean.toString(value)));
    }

    @Override
    public void writeNumber(byte value) {
        writeWithCommaHandling(() -> writer.write(Byte.toString(value)));
    }

    @Override
    public void writeNumber(short value) {
        writeCommaIfNeeded();
        writer.write(Short.toString(value));
        needsComma = true;
    }

    @Override
    public void writeNumber(int value) {
        writeCommaIfNeeded();
        writer.write(Integer.toString(value));
        needsComma = true;
    }

    @Override
    public void writeNumber(long value) {
        writeWithCommaHandling(() -> writer.write(Long.toString(value)));
    }

    @Override
    public void writeNumber(float value) {
        writeWithCommaHandling(() -> writer.write(Float.toString(value)));
    }

    @Override
    public void writeNumber(double value) {
        writeWithCommaHandling(() -> writer.write(Double.toString(value)));
    }

    @Override
    public void writeNumber(BigDecimal value) {
        writeWithCommaHandling(() -> writer.write(value.toString()));
    }

    public void writeString(String value) {
        writeWithCommaHandling(() -> writer.write("\"" + JsonUtil.escapeJson(value) + "\""));
    }

    @Override
    public void writeString(byte[] value) {
        writeWithCommaHandling(() -> writer.write(
            "\"" + JsonUtil.escapeJson(new String(value, StandardCharsets.UTF_8)) + "\""));
    }

    @Override
    public void flush() {
        writer.flush();
    }

    private void writeCommaIfNeeded() {
        if (needsComma) {
            writer.write(",");
        }
    }

    private void writeWithCommaHandling(Runnable action) {
        writeCommaIfNeeded();
        action.run();
        needsComma = true;
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (IOException e) { /* Closing a StringWriter is a no-op */ }
    }
}
