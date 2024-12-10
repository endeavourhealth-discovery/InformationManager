package org.endeavourhealth.informationManager.DataImport.fileHandlers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.schibsted.spt.data.jslt.Expression;
import com.schibsted.spt.data.jslt.Function;
import com.schibsted.spt.data.jslt.FunctionUtils;
import com.schibsted.spt.data.jslt.Parser;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public abstract class FileHandler implements AutoCloseable {
  Collection<Function> functions = new ArrayList<>();

  protected FileHandler() throws ClassNotFoundException {
    functions.add(FunctionUtils.wrapStaticMethod("uuidToIri", "org.endeavourhealth.informationManager.DataImport.fileHandlers.FileHandler", "uuidToIri"));
    functions.add(FunctionUtils.wrapStaticMethod("newUUIDIri", "org.endeavourhealth.informationManager.DataImport.fileHandlers.FileHandler", "newUUIDIri"));
  }

  public static String newUUIDIri(String namespace) {
    String uuid = UUID.randomUUID().toString();
    return uuidToIri(uuid, namespace);
  }

  public static String uuidToIri(String uuid, String namespace) {
    return namespace + (uuid.replace("{", "").replace("}", ""));
  }

  public abstract JsonNode getNextObject() throws IOException, URISyntaxException;


  abstract String getTransform() throws IOException, URISyntaxException;

  JsonNode transform(JsonNode input) throws IOException, URISyntaxException {
    if (input == null)
      return null;

    Expression jslt = Parser.compileString(getTransform(), functions);
    return jslt.apply(input);
  }

  JsonNode csvToJson(List<String> columns, String data) {
    if (data == null)
      return null;

    String[] values = data.split(",", -1);
    ObjectNode result = JsonNodeFactory.instance.objectNode();

    ListIterator<String> iterator = columns.listIterator();
    while (iterator.hasNext()) {
      int index = iterator.nextIndex();
      String value = values[index];

      if (value.startsWith("\"") && value.endsWith("\""))
        value = value.substring(1, value.length() - 1);

      String column = iterator.next();
      if (value != null && !value.isEmpty())
        result.set(column, JsonNodeFactory.instance.textNode(value));
    }

    return result;
  }

  String getResourceAsString(String name) throws IOException, URISyntaxException {
    return Files.readString(Paths.get(getClass().getClassLoader().getResource(name).toURI()));
  }

}
