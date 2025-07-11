package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CPRDImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(CPRDImport.class);

  private static final String[] obsCodes = {".*\\\\CPRD\\\\CPRDAurumMedical.txt"};
  private static final String[] drugCodes = {".*\\\\CPRD\\\\CPRDAurumProduct.txt"};

  private final TTManager manager = new TTManager();
  private TTDocument document;


  public CPRDImport() {
  }


  /**
   * Imports CPRD  identifiers codes and creates term code map to Snomed or local legacy entities
   *
   * @param config import configuration data
   * @throws Exception From document filer
   */


  public void importData(TTImportConfig config) throws ImportException {
    try {
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        importObsCodes(config.getFolder());
        document = manager.createDocument();
        document.addEntity(manager.createNamespaceEntity(Namespace.CPRD_MED, "CPRD medIds ",
          "CPRD clinical non product identifiers (including emis code ids)."));
        filer.fileDocument(document);

        document = manager.createDocument();
        document.addEntity(manager.createNamespaceEntity(Namespace.CPRD_PROD, "CPRD product ids",
          "internal identifiers to DMD VMPs and AMPs."));
        importDrugs(config.getFolder());
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }


  private void importDrugs(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, drugCodes[0]);
    int count = 0;
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine();
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        count++;
        if (count % 10000 == 0)
          LOG.info("Written {} drug concepts for " + Namespace.CPRD_PROD, count);
        TTEntity concept = new TTEntity();
        String drugId = (fields[0]);
        concept.setIri(Namespace.CPRD_PROD + "Product_" + drugId);
        concept.setName(fields[2]);
        concept.setCode(drugId);
        concept.setScheme(iri(Namespace.CPRD_PROD));
        concept.setStatus(iri(IM.ACTIVE));
        if (!fields[1].equals("")) {
          concept.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(Namespace.SNOMED + fields[1]));
        }
        document.addEntity(concept);
        line = reader.readLine();
      }
      LOG.info("Written {} entities for " + Namespace.CPRD_PROD, count);
    }

  }


  private void importObsCodes(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, obsCodes[0]);
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine();
      int count = 0;
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        count++;
        if (count % 10000 == 0)
          LOG.info("Written {} medical concepts for " + Namespace.CPRD_PROD, count);
        TTEntity concept = new TTEntity();
        String medId = (fields[0]);
        concept.setIri(Namespace.CPRD_PROD + "Medical_" + medId);
        concept.setName(fields[4]);
        concept.setCode(medId);
        concept.setScheme(iri(Namespace.CPRD_PROD));
        concept.setStatus(iri(IM.ACTIVE));
        concept.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(Namespace.SNOMED + fields[5]));
        TTNode termCode = new TTNode();
        termCode.set(iri(IM.CODE), TTLiteral.literal(fields[6]));
        concept.addObject(iri(IM.HAS_TERM_CODE), termCode);
        document.addEntity(concept);
        line = reader.readLine();
      }
      LOG.info("Written {} entities for " + Namespace.CPRD_PROD, count);
    }

  }


  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, obsCodes, drugCodes);
  }

  @Override
  public void close() throws Exception {
    manager.close();
  }
}

