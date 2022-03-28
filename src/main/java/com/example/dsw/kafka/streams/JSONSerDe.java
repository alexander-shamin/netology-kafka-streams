package com.example.dsw.kafka.streams;

import com.example.dsw.kafka.streams.pageview.POJO;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JSONSerDe<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
        private final TypeReference<T> typeReference;

        public JSONSerDe() {
            this.typeReference = new TypeReference<T>() {};
        }

        public JSONSerDe(TypeReference<T> typeReference) {
            this.typeReference = typeReference;
        }

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {

            if (data == null) {
                return null;
            }

            try {
                T obj = (T) OBJECT_MAPPER.readValue(data, typeReference);
                return obj;
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }
