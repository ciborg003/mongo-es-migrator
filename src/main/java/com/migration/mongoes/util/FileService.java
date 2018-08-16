package com.migration.mongoes.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class FileService {

    private static final Logger LOGGER = LogManager.getLogger(FileService.class);

    public static void rewriteToFileValue(String fileName, String value) {
        if (value == null) {
            return;
        }

        try {
            if (!Files.exists(Paths.get(fileName), LinkOption.NOFOLLOW_LINKS)) {
                Files.createFile(Paths.get(fileName));
            }
            Files.write(Paths.get(fileName),value.getBytes(), StandardOpenOption.WRITE, StandardOpenOption.CREATE);
        } catch (IOException e) {
            LOGGER.warn("Exception in writing to file, file name: '" + fileName + "'. ", e);
        }
    }

    public static String getValueFromFile(String fileName) {
        try {
            if (Files.exists(Paths.get(fileName), LinkOption.NOFOLLOW_LINKS)) {
                return new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            LOGGER.warn("Exception in reading data from file, file name: '" + fileName + "'.", e);
        }

        return null;
    }
}
