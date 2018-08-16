package com.migration.mongoes.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class UtilService {

    private static final Logger LOGGER = LogManager.getLogger(UtilService.class);

    public static Object getValueFromPath(Map<String, Object> inputMap, String inputPath){
        Object value = null;

        try {
            String[] inputkeys = inputPath.split("\\.");
            Map<String,Object> lastInputMap = new HashMap<>(inputMap);
            for(int i=0;i<inputkeys.length-1;i++){
                if(lastInputMap != null){
                    lastInputMap = getMap(lastInputMap, inputkeys[i]);
                }
            }
            if (lastInputMap != null) {
                String lastInputKey = inputPath;
                if(inputkeys.length > 0){
                    lastInputKey = inputkeys[inputkeys.length-1];
                }
                value = lastInputMap.get(lastInputKey);
            }
        } catch (Exception e) {
            try {
                LOGGER.warn("Exception while getting value from path:" + inputPath + " in map:" + (new ObjectMapper().writeValueAsString(inputMap)), e);
            } catch (JsonProcessingException e1) {
                LOGGER.warn("Exception in parsing map");
            }
        }

        return value;
    }

    public static Map<String, Object> getMap(Map<String, Object> map, String key) {
        if (map != null && map.get(key)
                != null) {
            @SuppressWarnings("unchecked")
            HashMap<String, Object> hashMap = (HashMap<String, Object>) map.get(key)
                    ;
            if (hashMap != null)
                return hashMap;
            else
                return null;
        } else
            return null;
    }
}
