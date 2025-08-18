package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class OPCS4Importer implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(OPCS4Importer.class);

  private static final String[] entities = {".*\\\\OPCS4\\\\.*\\\\OPCS4.* CodesAndTitles.*\\.txt"};
  private static final String[] chapters = {".*\\\\OPCS4\\\\OPCSChapters.txt"};
  private static final String[] maps = {".*\\\\CLINICAL\\\\.*\\\\SnomedCT_UKClinicalRF2_PRODUCTION_.*\\\\Snapshot\\\\Refset\\\\Map\\\\der2_iisssciRefset_ExtendedMapUKCLSnapshot_GB1000000_.*\\.txt"};

  private final TTIriRef opcscodes = TTIriRef.iri(Namespace.OPCS4 + "OPCS49Classification");
  private final Map<String, TTEntity> codeToEntity = new HashMap<>();
  private final Map<String, TTEntity> altCodeToEntity = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();

  private TTDocument document;
  private TTDocument mapDocument;
  private Set<String> snomedCodes;

  public void importData(TTImportConfig config) throws ImportException {
    LOG.info("Importing OPCS4.....");
    LOG.info("Checking Snomed codes first");
    try {
      snomedCodes = importMaps.getCodes(Namespace.SNOMED);
      try (TTManager manager = new TTManager()) {
        document = manager.createDocument();
        document.addEntity(manager.createNamespaceEntity(Namespace.OPCS4, "OPCS4 code scheme and graph", "OPCS4-9 official code scheme and graph"));
        importChapters(config.getFolder(), document);
        importEntities(config.getFolder(), document);

        mapDocument = manager.createDocument();
        importMaps(config.getFolder());
        //Important to file after maps set
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
          filer.fileDocument(document);
          filer.fileDocument(mapDocument);
        }
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  public TTDocument importMaps(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, maps[0]);
    ComplexMapImporter mapImport = new ComplexMapImporter();
    mapImport.importMap(file.toFile(), mapDocument, altCodeToEntity, "1126441000000105", snomedCodes);
    return document;
  }

  private void importChapters(String inFolder, TTDocument document) throws IOException {
    Path file = ImportUtils.findFileForId(inFolder, chapters[0]);
    TTEntity opcs = new TTEntity()
      .setIri(opcscodes.getIri())
      .addType(iri(IM.CONCEPT))
      .setName("OPCS 4-9 Classification")
      .setCode("OPCS49Classification")
      .setScheme(iri(Namespace.OPCS4))
      .setDescription("Classification of OPCS4 with chapter headings");
    opcs.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "CodeBasedTaxonomies"));
    document.addEntity(opcs);

    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        String chapter = fields[0];
        String term = fields[1];
        TTEntity c = new TTEntity();
        c.setIri(Namespace.OPCS4 + chapter)
          .setName(term + " (chapter " + chapter + ")")
          .setCode(chapter)
          .setScheme(iri(Namespace.OPCS4))
          .addType(iri(IM.CONCEPT))
          .set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(opcs.getIri())));
        codeToEntity.put(chapter, c);
        document.addEntity(c);
        line = reader.readLine();
      }
    }
    TTEntity c = new TTEntity()
      .setIri(Namespace.OPCS4 + "O")
      .setName("Overflow codes (chapter " + "O" + ")")
      .setCode("O")
      .addType(iri(IM.CONCEPT))
      .setScheme(iri(Namespace.OPCS4))
      .set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(opcs.getIri())));
    codeToEntity.put("O", c);
    document.addEntity(c);
  }

  private void importEntities(String folder, TTDocument document) throws IOException {

    Path file = ImportUtils.findFileForId(folder, entities[0]);

    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      String line = reader.readLine();

      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        if (count % 10000 == 0) {
          LOG.info("Processed {} records", count);
        }
        String[] fields = line.split("\t");
        String code = fields[0];
        TTEntity c = new TTEntity()
          .setCode(fields[0])
          .setScheme(iri(Namespace.OPCS4))
          .setIri(Namespace.OPCS4 + (fields[0].replace(".", "")))
          .addType(iri(IM.CONCEPT));
        if (code.contains(".")) {
          String qParent = code.substring(0, code.indexOf("."));
          TTEntity parent = codeToEntity.get(qParent);
          c.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(parent.getIri()));
        } else {
          String qParent = code.substring(0, 1);
          TTEntity parent = codeToEntity.get(qParent);
          c.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(parent.getIri()));
        }
        codeToEntity.put(fields[0], c);
        altCodeToEntity.put(fields[0].replace(".", ""), c);
        if (fields[1].length() > 250) {
          c.setName(fields[1].substring(0, 150));
        } else {
          c.setName(fields[1]);
        }

        document.addEntity(c);
        line = reader.readLine();
      }
      LOG.info("Imported {} records", count);
    }
  }

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, entities, chapters, maps);
  }

  @Override
  public void close() throws Exception {
    if (snomedCodes != null) snomedCodes.clear();
    codeToEntity.clear();
    altCodeToEntity.clear();

    importMaps.close();
  }
}
