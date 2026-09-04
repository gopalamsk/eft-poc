package com.bns.fsl.rtpeft.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.bns.fsl.rtpeft.exception.InternalSystemException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class JsonUtil {

    private final ObjectMapper objectMapper;

    public String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (Exception e) {
            throw new InternalSystemException("Failed to serialize to JSON", e);
        }
    }

    public <T> T fromJson(String json, Class<T> type) {
        try {
            return objectMapper.readValue(json, type);
        } catch (Exception e) {
            throw new InternalSystemException("Failed to deserialize JSON to " + type.getSimpleName(), e);
        }
    }
}
