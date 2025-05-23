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
    TTImportByType importer = new Importer();
    switch (cfg.getImportType()) {
      case "qofquery", "qof" -> {
        importer = new Importer().validateByType(GRAPH.QOF, cfg.getFolder());
        importer.importByType(GRAPH.QOF, cfg);
      }
      case "corequery" -> {
        importer = new Importer().validateByType(GRAPH.QUERY, cfg.getFolder());
        importer.importByType(GRAPH.QUERY, cfg);
      }
      case "imv1" -> {
        importer = new Importer().validateByType(GRAPH.IM1, cfg.getFolder());
        importer.importByType(GRAPH.IM1, cfg);
      }
      case "bnf" -> {
        importer = new Importer().validateByType(GRAPH.BNF, cfg.getFolder());
        importer.importByType(GRAPH.BNF, cfg);
      }
      case "prsb" -> {
        importer = new Importer().validateByType(GRAPH.PRSB, cfg.getFolder());
        importer.importByType(GRAPH.PRSB, cfg);
      }
      case "core" -> {
        importer = new Importer().validateByType(GRAPH.DISCOVERY, cfg.getFolder());
        importer.importByType(GRAPH.DISCOVERY, cfg);
      }
      case "snomed" -> {
        importer = new Importer().validateByType(SNOMED.NAMESPACE, cfg.getFolder());
        importer.importByType(SNOMED.NAMESPACE, cfg);
      }
      case "emis" -> {
        importer = new Importer().validateByType(GRAPH.EMIS, cfg.getFolder());
        importer.importByType(GRAPH.EMIS, cfg);
      }
      case "cprd" -> {
        importer = new Importer().validateByType(GRAPH.CPRD_MED, cfg.getFolder());
        importer.importByType(GRAPH.CPRD_MED, cfg);
      }
      case "tpp", "ctv3" -> {
        importer = new Importer().validateByType(GRAPH.TPP, cfg.getFolder());
        importer.importByType(GRAPH.TPP, cfg);
      }
      case "opcs4" -> {
        importer = new Importer().validateByType(GRAPH.OPCS4, cfg.getFolder());
        importer.importByType(GRAPH.OPCS4, cfg);
      }
      case "icd10" -> {
        importer = new Importer().validateByType(GRAPH.ICD10, cfg.getFolder());
        importer.importByType(GRAPH.ICD10, cfg);
      }
      case "discoverymaps", "encounters" -> {
        importer = new Importer().validateByType(GRAPH.ENCOUNTERS, cfg.getFolder());
        importer.importByType(GRAPH.ENCOUNTERS, cfg);
      }
      case "read2", "vision" -> {
        importer = new Importer().validateByType(GRAPH.VISION, cfg.getFolder());
        importer.importByType(GRAPH.VISION, cfg);
      }
      case "kingsapex" -> {
        importer = new Importer().validateByType(GRAPH.KINGS_APEX, cfg.getFolder());
        importer.importByType(GRAPH.KINGS_APEX, cfg);
      }
      case "kingswinpath" -> {
        importer = new Importer().validateByType(GRAPH.KINGS_WINPATH, cfg.getFolder());
        importer.importByType(GRAPH.KINGS_WINPATH, cfg);
      }
      case "smartlifequery" -> {
        importer = new Importer().validateByType(GRAPH.SMARTLIFE, cfg.getFolder());
        importer.importByType(GRAPH.SMARTLIFE, cfg);
      }
      case "ceg" -> {
        importer = new Importer().validateByType(GRAPH.CEG, cfg.getFolder());
        importer.importByType(GRAPH.CEG, cfg);
      }
      case "barts" -> {
        importer = new Importer().validateByType(GRAPH.BARTS_CERNER, cfg.getFolder());
        importer.importByType(GRAPH.BARTS_CERNER, cfg);
      }
      case "ods" -> {
        importer = new Importer().validateByType(GRAPH.ODS, cfg.getFolder());
        importer.importByType(GRAPH.ODS, cfg);
      }
      case "nhstfc" -> {
        importer = new Importer().validateByType(GRAPH.NHS_TFC, cfg.getFolder());
        importer.importByType(GRAPH.NHS_TFC, cfg);
      }
      case "tct" -> cfg.setSkipsearch(true);
      case "search" -> cfg.setSkiptct(true);
      case "config" -> {
        importer = new Importer().validateByType(GRAPH.CONFIG, cfg.getFolder());
        importer.importByType(GRAPH.CONFIG, cfg);
      }
      case "deltas" -> {
        importer = new Importer().validateByType(GRAPH.DELTAS, cfg.getFolder());
        importer.importByType(GRAPH.DELTAS, cfg);
      }
      case "singlefile" -> {
        importer = new Importer().validateByType(TTIriRef.iri(IM.NAMESPACE + "SingleFileImporter"), cfg.getFolder());
        importer.importByType(TTIriRef.iri(IM.NAMESPACE + "SingleFileImporter"), cfg);
      }
      case "qcodegroups" -> {
        importer = new Importer().validateByType(QR.NAMESPACE, cfg.getFolder());
        importer.importByType(QR.NAMESPACE, cfg);
      }
      case "fhir" -> {
        importer = new Importer().validateByType(FHIR.GRAPH_FHIR, cfg.getFolder());
        importer.importByType(FHIR.GRAPH_FHIR, cfg);
      }
      default -> throw new IllegalArgumentException("Unknown import type");
    }

    LOG.info("Finished - {}", new Date());
  }
}

