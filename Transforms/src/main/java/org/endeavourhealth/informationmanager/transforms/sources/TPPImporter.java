package org.endeavourhealth.informationmanager.transforms.sources;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvValidationException;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.common.ZipUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


/**
 * Creates the term code entity map for TPP codes
 * Creates new entities for TPP local codes that are unmapped
 */
public class TPPImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(TPPImporter.class);

  private static final String[] concepts = {".*\\\\TPP\\\\Concept.v3"};
  private static final String[] dcf = {".*\\\\TPP\\\\Dcf.v3"};
  private static final String[] descriptions = {".*\\\\TPP\\\\Descrip.v3"};
  private static final String[] terms = {".*\\\\TPP\\\\Terms.v3"};
  private static final String[] hierarchies = {".*\\\\TPP\\\\V3hier.v3"};
  private static final String[] nhsMap = {".*\\\\TPP\\\\CTV3SCTMAP.txt"};
  private static final String[] localCodeMap = {".*\\\\TPP\\\\TPP-Local-Snomed.txt"};
  private static final String[] vaccineMaps = {".*\\\\TPP\\\\VaccineMaps.json"};
  private static final String[] tppCtv3Lookup = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_lookup_2.zip"};
  private static final String[] tppCtv3ToSnomed = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_to_snomed.zip"};
  private Map<String, Set<String>> emisToSnomed;
  private TTDocument document;
  private TTDocument vDocument;
  private static final Map<String, TTEntity> codeToEntity = new HashMap<>();
  private static final Map<String, String> termCodes = new HashMap<>();
  private ImportMaps importMaps = new ImportMaps();


  public void importData(TTImportConfig config) throws Exception {


    LOG.info("Looking for Snomed codes");
    try (TTManager manager = new TTManager()) {

      document = manager.createDocument(GRAPH.TPP);
      document.addEntity(manager.createGraph(GRAPH.TPP, "TPP (including CTV3) codes",
        "The TPP local code scheme and graph including CTV3 and TPP local codes"));

      //Gets the emis read 2 codes from the IM to use as look up as some are missing
      // importEmis();
      importEMISMaps();

      addTPPTopLevel();
      inportTPPConcepts(config.getFolder());
      importTPPTerms(config.getFolder());
      importTPPDescriptions(config.getFolder());
      importTPPDcf(config.getFolder());
      importLocals(config.getFolder());

      importCV3Hierarchy(config.getFolder());

      //Imports the tpp terms from the tpp look up table
      importTppCtv3ToSnomed(config.getFolder());
      importnhsMaps(config.getFolder());
      addEmisMaps();
      addDiscoveryMaps();
      importVaccineMaps(manager, config.getFolder());
      setOrphanC0des();
      importTppLocalMaps(config.getFolder());

      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
        filer.fileDocument(document);
      }


    }
  }

  private void setOrphanC0des() {
    for (Map.Entry<String, TTEntity> entry : codeToEntity.entrySet()) {
      String code = entry.getKey();
      TTEntity tppEntity = entry.getValue();
      if (tppEntity.get(iri(IM.MATCHED_TO)) == null) {
        if (tppEntity.get(iri(IM.IS_CHILD_OF)) == null)
          tppEntity.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(GRAPH.TPP + "TPPOrphanCodes"));
      }
    }
  }

  private void addDiscoveryMaps() {
    TTEntity entity = new TTEntity()
      .setIri(GRAPH.TPP + "Y2a0e")
      .setCrud(iri(IM.ADD_QUADS))
      .set(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + "1156257007"));
    document.addEntity(entity);
    entity = new TTEntity()
      .setIri(GRAPH.TPP + "Y29ea")
      .setCrud(iri(IM.ADD_QUADS))
      .set(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + "1324671000000103"));
    document.addEntity(entity);
  }

  private void importVaccineMaps(TTManager manager, String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, vaccineMaps[0]);
    vDocument = manager.loadDocument(file.toFile());
    for (TTEntity vaccine : vDocument.getEntities()) {
      String iri = vaccine.getIri();
      String code = iri.substring(iri.lastIndexOf("#") + 1);
      TTEntity entity = codeToEntity.get(code);
      if (entity == null) {
        entity = new TTEntity()
          .setIri(vaccine.getIri())
          .setCrud(iri(IM.ADD_QUADS))
          .setCode(code)
          .setScheme(iri(GRAPH.TPP))
          .set(iri(IM.MATCHED_TO), vaccine.get(iri(IM.MATCHED_TO)));
        document.addEntity(entity);

      }
      for (TTValue match : vaccine.get(iri(IM.MATCHED_TO)).getElements()) {
        entity.addObject(iri(IM.MATCHED_TO), match);
      }
    }

  }

  private void addEmisMaps() {
    for (TTEntity entity : document.getEntities()) {
      String code = entity.getCode();
      if (code != null) {
        if (!code.startsWith(".")) {
          String scode = code.replace(".", "");
          if (emisToSnomed.get(scode) != null) {
            for (String snomed : emisToSnomed.get(scode)) {
              if (!alreadyMapped(entity, snomed.split("#")[1])) {
                entity.addObject(iri(IM.MATCHED_TO), iri(snomed));
              }
            }
          }
        }
      }
    }
  }


  private void importEMISMaps() throws SQLException, TTFilerException, IOException {
    LOG.info("Getting EMIS maps");
    emisToSnomed = importMaps.importEmisToSnomed();
  }

  private void importLocals(String folder) throws IOException, CsvValidationException {
    Path zip = ImportUtils.findFileForId(folder, tppCtv3Lookup[0]);
    File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
    LOG.info("Importing TPP Ctv3 local codes");
    try (CSVReader reader = (new CSVReaderBuilder(new FileReader(file))
      .withCSVParser(new CSVParserBuilder()
        .withSeparator('\t')
        .build())
      .build())) {
      reader.readNext();
      String[] fields;
      int count = 0;
      while ((fields = reader.readNext()) != null) {
        count++;
        if (count % 50000 == 0) {
          LOG.info("Imported {} codes", count);
        }
        String code = fields[0].replace("\"", "");
        String term = fields[1];
        TTEntity tpp = codeToEntity.get(code);
        if (tpp == null) {
          tpp = new TTEntity().setIri(GRAPH.TPP + code.replace(".", "_"));
          tpp.setCode(code);
          tpp.setName(term);
          tpp.addType(iri(IM.CONCEPT));
          codeToEntity.put(code, tpp);
          document.addEntity(tpp);

        }
      }
      LOG.info("Process ended with {}", count);
    }
  }

  private void importnhsMaps(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, nhsMap[0]);
    LOG.info("Retrieving terms from tpp_TPP+lookup2");
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine(); // NOSONAR - Skip header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        count++;
        if (count % 50000 == 0) {
          LOG.info("{} nhs ctv3 maps imported", count);
        }
        String code = fields[0];
        String snomed = fields[2];
        if (!snomed.equals("_DRUG")) {
          TTEntity tpp = codeToEntity.get(code);
          if (tpp != null) {
            if (!alreadyMapped(tpp, snomed))
              tpp.addObject(iri(IM.MATCHED_TO), iri(SNOMED.NAMESPACE + snomed));
          }
        }

        line = reader.readLine();
      }
      LOG.info("Process ended with {} entities created", count);
    }


  }

  private boolean alreadyMapped(TTEntity tpp, String snomed) {
    if (tpp.get(iri(IM.MATCHED_TO)) == null)
      return false;
    for (TTValue superClass : tpp.get(iri(IM.MATCHED_TO)).iterator()) {
      if (superClass.asIriRef().getIri().split("#")[1].equals(snomed))
        return true;
    }
    return false;
  }

  private void importCV3Hierarchy(String path) throws IOException {
    for (String hierFile : hierarchies) {
      Path file = ImportUtils.findFilesForId(path, hierFile).get(0);
      LOG.info("Processing  hierarchy in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        String line = reader.readLine();
        int count = 0;
        while (line != null && !line.isEmpty()) {
          count++;
          String[] fields = line.split("\\|");
          String child = fields[0];
          String parent = fields[1];
          TTEntity tpp = codeToEntity.get(child);
          if (tpp != null) {
            if (!parent.startsWith(".")) {
              TTManager.addChildOf(tpp, iri(GRAPH.TPP + parent.replace(".", "_")));
            } else {
              TTManager.addChildOf(tpp, iri(GRAPH.TPP + "TPPCodes"));
            }
          }
          line = reader.readLine();
        }
        LOG.info("Imported {} hierarchy nodes", count);
      }
    }

  }

  private void importTPPTerms(String path) throws IOException {
    int i = 0;
    for (String termFile : terms) {
      Path file = ImportUtils.findFilesForId(path, termFile).get(0);
      LOG.info("Processing terms in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine();  // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\\|");
          String termCode = fields[0];
          String term = fields[2].replace("\t", "");
          if (fields.length > 3 && !fields[3].equals(""))
            term = fields[3].replace("\t", "");
          termCodes.put(termCode, term);
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} term codes", i);
  }

  private void importTPPDescriptions(String path) throws IOException {
    int i = 0;
    for (String conceptFile : descriptions) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing  descriptions in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\\|");
          String concept = fields[0];
          String termCode = fields[1];
          String termType = fields[2];
          String term = termCodes.get(termCode);
          if (term != null) {
            TTEntity tpp = codeToEntity.get(concept);
            if (tpp != null) {
              TTManager.addTermCode(tpp, term, termCode);
              if (termType.equals("P"))
                tpp.setName(term);
            }
          }
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} term codes", i);
  }

  private void importTPPDcf(String path) throws IOException {
    int i = 0;
    for (String conceptFile : dcf) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing replacememnts in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\\|");
          String concept1 = fields[1];
          String termCode = fields[0];
          if (termCode.equals("Yag2I"))
            LOG.info("term code");
          String concept2 = fields[2];
          String term = termCodes.get(termCode);
          if (term != null) {
            TTEntity tpp = codeToEntity.get(concept1);
            if (tpp != null) {
              TTManager.addTermCode(tpp, term, termCode);
              if (tpp.getName() == null)
                tpp.setName(term);
            }
            TTEntity tpp1 = codeToEntity.get(concept2);
            if (tpp1 != null) {
              TTManager.addTermCode(tpp1, term, termCode);
              if (tpp1.getName() == null)
                tpp1.setName(term);
            }
          }
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} term codes", i);
  }

  private void inportTPPConcepts(String path) throws IOException {
    int i = 0;
    LOG.info("importing concepts..");
    for (String conceptFile : concepts) {
      Path file = ImportUtils.findFilesForId(path, conceptFile).get(0);
      LOG.info("Processing concepts in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          String[] fields = line.split("\\|");
          String code = fields[0];
          if (!Character.isLowerCase(code.charAt(0))) {
            String iri = GRAPH.TPP +
              (code.replace(".", "_"));
            TTEntity tpp = new TTEntity().setIri(iri);
            tpp.setCode(code);
            tpp.setScheme(iri(GRAPH.TPP));
            tpp.setStatus(iri(IM.ACTIVE));
            tpp.addType(iri(IM.CONCEPT));
            if (code.startsWith("."))
              tpp.setStatus(iri(IM.INACTIVE));
            codeToEntity.put(code, tpp);
            document.addEntity(tpp);
          }
          i++;
          line = reader.readLine();
        }
      }
    }
    LOG.info("Imported {} concepts", i);
  }

  private void addTPPTopLevel() {
    TTEntity c = new TTEntity().setIri(GRAPH.TPP + "TPPCodes")
      .addType(iri(IM.CONCEPT))
      .setName("TPP (CTV3) and TPP local codes")
      .setScheme(iri(GRAPH.TPP))
      .setCode("TPPCodes");
    c.set(iri(IM.IS_CONTAINED_IN), new TTArray());
    c.get(iri(IM.IS_CONTAINED_IN)).add(TTIriRef.iri(IM.NAMESPACE + "CodeBasedTaxonomies"));
    document.addEntity(c);
    c = new TTEntity().setIri(GRAPH.TPP + "TPPOrphanCodes")
      .set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(GRAPH.TPP + "TPPCodes")))
      .setName("TPP unmatched orphan codes")
      .addType(iri(IM.CONCEPT))
      .setDescription("TPP orphan codes whose parent is unknown and are not matched to UK Snomed-CT")
      .setScheme(iri(GRAPH.TPP));
    document.addEntity(c);
  }

  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, concepts, descriptions, dcf, terms, hierarchies, tppCtv3Lookup, tppCtv3ToSnomed, nhsMap, vaccineMaps, localCodeMap);
  }


  private void importTppCtv3ToSnomed(String folder) throws IOException, CsvValidationException {
    Path zip = ImportUtils.findFileForId(folder, tppCtv3ToSnomed[0]);
    File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
    LOG.info("Importing TPP Ctv3 to Snomed");
    try (CSVReader reader = (new CSVReaderBuilder(new FileReader(file))
      .withCSVParser(new CSVParserBuilder()
        .withSeparator('\t')
        .build())
      .build())) {
      reader.readNext();
      String[] fields;
      int count = 0;
      while ((fields = reader.readNext()) != null) {
        count++;
        if (count % 50000 == 0) {
          LOG.info("{} ctv3 maps imported ", count);
        }
        String code = fields[0];
        String snomed = fields[1];
        TTEntity tpp = codeToEntity.get(code);
        if (tpp == null) {
          tpp = new TTEntity().setIri(GRAPH.TPP + code.replace(".", "_"));
          tpp.setCode(code);
          tpp.setScheme(iri(GRAPH.TPP));
          tpp.setName("TPP local code. name unknown");
          tpp.addType(iri(IM.CONCEPT));
          codeToEntity.put(code, tpp);
          document.addEntity(tpp);

        }
        if (!alreadyMapped(tpp, snomed)) {
          tpp.addObject(iri(IM.MATCHED_TO), iri(SNOMED.NAMESPACE + snomed));
        }
      }
      LOG.info("Process ended with {}", count);
    }
  }


  private void importTppLocalMaps(String folder) throws IOException, CsvValidationException {
    Path file = ImportUtils.findFileForId(folder, localCodeMap[0]);
    LOG.info("Importing TPP Local Code map");
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        if (count % 50000 == 0) {
          LOG.info("{} Local maps imported ", count);
        }
        String[] fields = line.split("\t");
        String code = fields[0].split("#")[1];
        String name = fields[1];
        if (name == null) {
          name = "TPP Local code name unknown";
        }
        String snomed = fields[2];
        TTEntity tpp = codeToEntity.get(code);
        if (tpp == null) {
          tpp = new TTEntity().setIri(GRAPH.TPP + code.replace(".", "_"));
          tpp.setCode(code);
          tpp.setScheme(iri(GRAPH.TPP));
          tpp.setName(name);
          tpp.addType(iri(IM.CONCEPT));
          codeToEntity.put(code, tpp);
          document.addEntity(tpp);

        }
        if (!alreadyMapped(tpp, snomed.split("#")[1])) {
          tpp.addObject(iri(IM.MATCHED_TO), iri(snomed));
        }
        line = reader.readLine();
      }
      LOG.info("Process ended with {}", count);
    }
  }


  public String[] readQuotedCSVLine(String line) {
    String[] fields = line.split(",");
    if (fields.length > 3) {
      for (int i = 2; i < fields.length - 1; i++) {
        fields[1] = fields[1].concat(",").concat(fields[i]);
      }
    }
    return fields;
  }

  @Override
  public void close() throws Exception {
    if (emisToSnomed != null) emisToSnomed.clear();
    codeToEntity.clear();
    termCodes.clear();

    importMaps.close();
  }
}
