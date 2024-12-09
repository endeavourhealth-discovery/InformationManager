package org.endeavourhealth.informationManager.DataImport.fileHandlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.schibsted.spt.data.jslt.FunctionUtils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.List;

public class EmisCsvHandler extends FileHandler {
  private List<String> columns;
  private BufferedReader reader;
  private String filename;

  public EmisCsvHandler(String filename) throws IOException, ClassNotFoundException {
    this.filename = filename;

    functions.add(FunctionUtils.wrapStaticMethod("getEmisLocationType", "org.endeavourhealth.informationManager.DataImport.fileHandlers.EmisCsvHandler", "getEmisLocationType"));

    openFile(filename);
    readColumns();
  }

  public static String getEmisLocationType(String locationTypeName) {
    return "emis:" + locationTypeName.toUpperCase().replace(" ", "_");
  }

  @Override
  String getTransform() throws IOException, URISyntaxException {
    if (filename.contains("_Admin_Location_"))
      return getResourceAsString("EmisAdminLocation.jslt");
    else
      throw new InputMismatchException("Unknown EMIS file type [" + filename + "]");
  }

  public JsonNode getNextObject() throws IOException, URISyntaxException {
    String data = reader.readLine();
    JsonNode json = csvToJson(columns, data);
    return transform(json);
  }

  private void openFile(String filename) throws FileNotFoundException {
    reader = new BufferedReader(new FileReader(filename));
  }

  private void readColumns() throws IOException {
    String data = reader.readLine();
    if (data == null)
      throw new IOException("No data found in file");

    columns = new ArrayList<>();
    for (String column : data.split(",")) {
      if (column.startsWith("\"") && column.endsWith("\""))
        column = column.substring(1, column.length() - 1);
      columns.add(column);
    }
  }

  @Override
  public void close() throws Exception {
    if (reader != null) {
      reader.close();
    }
  }
}
