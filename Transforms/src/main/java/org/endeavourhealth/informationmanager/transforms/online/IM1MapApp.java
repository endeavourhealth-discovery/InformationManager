package org.endeavourhealth.informationmanager.transforms.online;

import org.endeavourhealth.imapi.vocabulary.SCHEME;
import org.endeavourhealth.informationmanager.transforms.models.TTImportByType;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;

public class IM1MapApp {


  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Insufficient parameters supplied:");
      System.err.println("<folder> ");
      System.exit(-1);
    }

    TTImportConfig cfg = new TTImportConfig();

    // Mandatory/ordered args
    cfg.setFolder(args[0]);
    TTImportByType importer = new Importer();
    importer.validateByType(SCHEME.IM1, cfg.getFolder());
    importer.importByType(SCHEME.IM1, cfg);
  }

}
