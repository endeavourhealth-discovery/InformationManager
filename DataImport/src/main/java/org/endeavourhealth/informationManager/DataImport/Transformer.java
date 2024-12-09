package org.endeavourhealth.informationManager.DataImport;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.informationManager.DataImport.fileHandlers.EmisCsvHandler;
import org.endeavourhealth.informationManager.DataImport.fileHandlers.FileHandler;

public class Transformer {
  public Transformer() {
  }

  public void transform(String inputFile, String outputFile) throws Exception {
    try (FileHandler fis = getFileHandler(inputFile)) {
      JsonNode obj = fis.getNextObject();
      while (obj != null) {
        System.out.println(obj.toPrettyString());

        obj = fis.getNextObject();
      }
    }
  }

  public FileHandler getFileHandler(String inputFile) throws Exception {
    return new EmisCsvHandler(inputFile);
  }

}
