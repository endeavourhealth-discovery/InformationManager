package org.endeavourhealth.informationmanager.transforms.online;

import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.TTImportByType;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Utility app for importing one or all of the various source files for the ontology initial population.
 */
public class ImportApp {
  private static final Logger LOG = LoggerFactory.getLogger(ImportApp.class);

  private static String testDirectory;

  public static String resourceFolder;

  public static String getTestDirectory() {
    return testDirectory;
  }


  public static void setTestDirectory(String testDirectory) {
    ImportApp.testDirectory = testDirectory;
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      LOG.error("Insufficient parameters supplied:");
      LOG.error("<source> <import type> <privacy={0 public 1 private publication 2 private authoring}> [secure|skiptct|skipsearch]");
      System.exit(-1);
    }

    TTImportConfig cfg = new TTImportConfig();

    // Mandatory/ordered args
    cfg.setFolder(args[0]);
    cfg.setImportType(args[1].toLowerCase());

    // Optional switch args
    if (args.length >= 3) {
      for (int i = 2; i < args.length; i++) {
        if (args[i].toLowerCase().contains("resources="))
          cfg.setResourceFolder(args[i].substring(args[i].lastIndexOf("=") + 1));
        switch (args[i].toLowerCase().split("=")[0]) {
          case "secure" -> cfg.setSecure(true);
          case "skiptct" -> cfg.setSkiptct(true);
          case "skipsearch" -> cfg.setSkipsearch(true);
          case "skiplucene" -> cfg.setSkiplucene(true);
          case "privacy" -> TTFilerFactory.setPrivacyLevel(Integer.parseInt(args[i].split("=")[1]));
          case "resources" -> ImportApp.resourceFolder = args[i].substring(args[i].lastIndexOf("=") + 1);
          case "entity" -> cfg.setSingleEntity(args[i].split("=")[1]);
          default -> {
            if (args[i].contains("test="))
              testDirectory = args[i].substring(args[i].lastIndexOf("=") + 1);
            else
              LOG.error("Unknown parameter {}", args[i]);
          }
        }
      }
    }
    importData(cfg);
  }

  private static void importData(TTImportConfig cfg) throws Exception {
    if ("tct".equals(cfg.getImportType())) {
      cfg.setSkipsearch(true);
    } else if ("search".equals(cfg.getImportType())) {
      cfg.setSkiptct(true);
    } else {
      IMPORT importType = IMPORT.from(cfg.getImportType());
      TTImportByType importer = new Importer().validateByType(importType, cfg.getFolder());
      importer.importByType(importType, cfg);
    }

    LOG.info("Finished - {}", new Date());
  }
}

