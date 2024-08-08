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
package io.delta.kernel.utils;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private JsonUtil() {
    }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonFactory FACTORY = new JsonFactory();
    private static final Map<Character, String> JSON_ESCAPE_MAP;
    static {
        Map<Character, String> map = new HashMap<>();
        map.put('"', "\\\"");
        map.put('\\', "\\\\");
        map.put('/', "\\/");  // Escaping forward slash
        map.put('\b', "\\b");
        map.put('\f', "\\f");
        map.put('\n', "\\n");
        map.put('\r', "\\r");
        map.put('\t', "\\t");
        JSON_ESCAPE_MAP = Collections.unmodifiableMap(map);
    }

    public static JsonFactory factory() {
        return FACTORY;
    }

    public static ObjectMapper mapper() {
        return OBJECT_MAPPER;
    }

    @FunctionalInterface
    public interface ToJson {
        void generate(JsonWriter jsonWriter) throws IOException;
    }

    @FunctionalInterface
    public interface JsonValueWriter<T> {
        void write(JsonWriter jsonWriter, T value) throws IOException;
    }

    /**
     * Utility class for writing JSON with a Jackson {@link JsonGenerator}.
     *
     * @param toJson function that produces JSON using a {@link JsonGenerator}
     * @return a JSON string produced from the generator
     */
    public static String generate(ToJson toJson) {
        try (StringWriter writer = new StringWriter();
             JsonWriter jsonWriter = new StringWriterJsonWriter(writer)) {
            toJson.generate(jsonWriter);
            jsonWriter.flush();
            return writer.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Utility class escapes the characters in a String using Json String rules.
     * The only difference between Java strings and Json strings is that in Json,
     * forward-slash (/) is escaped.
     *
     * See http:// www. ietf. org/ rfc/ rfc4627.txt for further details.
     * @param input The input string.
     * @return String with escaped values, null if null string input
     */
    public static String escapeJson(String input) {
        String result = null;
        if (input != null) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < input.length(); i++) {
                char c = input.charAt(i);
                String escaped = JSON_ESCAPE_MAP.get(c);
                if (escaped != null) {
                    sb.append(escaped);
                } else if (Character.isISOControl(c)) {
                    sb.append(String.format("\\u%04x", (int) c));
                } else {
                    sb.append(c);
                }
            }
            result = sb.toString();
        }
        return result;
    }
}
