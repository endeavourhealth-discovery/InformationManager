package org.endeavourhealth.informationmanager.utils.opensearch;

import java.io.IOException;

public class Main {
  public static void main(String[] argv) throws IOException, InterruptedException {
    new OpenSearchSender().execute();
  }
}
