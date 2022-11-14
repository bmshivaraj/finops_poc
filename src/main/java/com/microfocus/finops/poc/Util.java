package com.microfocus.finops.poc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.nio.file.Files;

public class Util {

    public static int lineCount(File file) throws IOException {

        LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(file));
        lineNumberReader.skip(Long.MAX_VALUE);
        int lineCount = lineNumberReader.getLineNumber();
        lineNumberReader.close();

        return lineCount;
    }



    public static int jsonRecordCount(File jsonFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(new String(Files.readAllBytes(jsonFile.toPath())));

            return ((ArrayNode)jsonNode).size();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
