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

import java.io.Closeable;
import java.math.BigDecimal;

public interface JsonWriter extends Closeable {
    void writeStartObject();

    void writeEndObject();

    void writeFieldName(String name);

    void writeNumberField(String name, long value);

    void writeStringField(String name, String value);

    void writeObjectFieldStart(String name);

    void writeBoolean(boolean value);

    void writeNumber(byte value);

    void writeNumber(short value);

    void writeNumber(int value);

    void writeNumber(long value);

    void writeNumber(float value);

    void writeNumber(double value);

    void writeNumber(BigDecimal value);

    void writeString(String value);

    void writeString(byte[] value);

    void flush();
}
