package org.endeavourhealth.informationmanager.utils.opensearch;

import org.endeavourhealth.imapi.dataaccess.OpenSearchSender;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.IOException;

public class Main {
  public static void main(String[] argv) throws IOException, InterruptedException {
    boolean update = false;
    String cache = null;
    for (String arg : argv)
      if (arg.equalsIgnoreCase("update"))
        update = true;
      else if (arg.toLowerCase().startsWith("cache=")) {
        cache = arg.split("=")[1];
      }
    new OpenSearchSender().execute(update, IM.GRAPH);
  }
}
