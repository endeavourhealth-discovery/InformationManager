package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.logic.exporters.SetExporter;
import org.endeavourhealth.imapi.vocabulary.IM;

public class ExporterApp {
  // TODO: Needs rework to use SetExporter to take advantage of latest changes (pre-expansion, subset predicate reversal, etc).
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("You need to provide a root path to send the data to, and set type");
      System.exit(-1);
    }
    String exportType = args[1].toLowerCase();
    SetExporter exporter = new SetExporter();
    switch (exportType) {
      case "conceptsets":
        // exporter.exportAll(args[0], IM.CONCEPT_SET);
        break;
      case "singleset":
        if (args.length != 3) {
          System.err.println("You need to provide a set IRI when exporting a single set");
          System.exit(-1);
        }

        // exporter.exportSingle(args[0], args[2]);
        break;
      default:
        throw new Exception("Unrecognised export type");
    }
  }
}
