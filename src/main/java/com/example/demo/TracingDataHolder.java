package com.example.demo;

import java.util.HashMap;
import java.util.Map;

public class TracingDataHolder {
    private final Map<String, String> data = new HashMap<>();

    public void add(String key, String value) {
        data.put(key, value);
    }

    public String get(String key) {
        return data.get(key);
    }
}
