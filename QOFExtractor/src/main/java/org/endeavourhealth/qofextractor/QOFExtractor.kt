package org.endeavourhealth.qofextractor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.logic.importers.QOFImportEngine;
import org.endeavourhealth.imapi.model.qof.QOFDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class QOFExtractor {
  private static final Logger LOG = LoggerFactory.getLogger(QOFExtractor.class);

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      LOG.error("You must provide a path to the folder containing the QOF documents!");
      System.exit(1);
    }

    File[] files = new File(args[0]).listFiles((dir, name) -> name.toLowerCase().endsWith(".docx"));

    if (files == null || files.length == 0) {
      LOG.error("No .docx files found in the specified folder!");
      System.exit(1);
    }

    for (File file : files) {
      QOFDocument qofDoc = QOFImportEngine.INSTANCE.processFile(file);

      String filePath = file.getParentFile().getAbsolutePath();
      String fileName = file.getName().replace(".docx", ".json");
      generateOutput(qofDoc, filePath + "/" + fileName);
    }
  }
  private static void generateOutput(QOFDocument qofDoc, String fileName) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    FileWriter file = new FileWriter(fileName);
    mapper.writerWithDefaultPrettyPrinter().writeValue(file, qofDoc);
    file.close();
  }

}