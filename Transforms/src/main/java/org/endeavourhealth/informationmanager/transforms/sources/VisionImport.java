package org.endeavourhealth.informationmanager.transforms.sources;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.tripletree.*;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class VisionImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(VisionImport.class);
  private static final String[] r2Terms = {".*\\\\READ\\\\Term.csv"};
  private static final String[] r2Desc = {".*\\\\READ\\\\DESC.csv"};
  private static final String[] visionRead2Code = {".*\\\\TPP_Vision_Maps\\\\vision_read2_code.csv"};
  private static final String[] visionRead2toSnomed = {".*\\\\TPP_Vision_Maps\\\\vision_read2_to_snomed_map.csv"};

  private final Map<String, TTEntity> codeToConcept = new HashMap<>();
  private final Map<String, TTEntity> r2TermIdMap = new HashMap<>();
  private final Set<String> preferredId = new HashSet<>();
  private final ImportMaps importMaps = new ImportMaps();
  private Set<String> snomedCodes;
  private TTDocument document;
  private Map<String, TTEntity> emisRead;

  @Override
  public void importData(TTImportConfig config) throws ImportException {

    LOG.info("importing vision codes");
    LOG.info("retrieving snomed codes from IM");
    try (TTManager manager = new TTManager()) {
      snomedCodes = importMaps.getCodes(Namespace.SNOMED, Graph.IM);
      document = manager.createDocument();
      document.addEntity(manager.createNamespaceEntity(Namespace.VISION, "Vision (including Read) codes",
        "The Vision local code scheme and graph including Read 2 and Vision local codes"));

      importEmis();
      importR2Desc(config.getFolder());
      importR2Terms(config.getFolder());
      importVisionCodes(config.getFolder());
      addMoreReadCodes();
      createHierarchy();
      addVisionMaps(config.getFolder());
      addMissingMaps();
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }

    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  public Boolean isEMIS(String s) {
    if (s.length() > 5)
      return true;
    else if (s.contains("DRG") || s.contains("SHAPT") || s.contains("EMIS"))
      return true;
    else
      return false;
  }

  public String[] readQuotedCSVLine(BufferedReader reader, String line) throws IOException {
    if (line.split(",").length < 5) {
      do {
        String nextLine = reader.readLine();
        line = line.concat("\n").concat(nextLine);
      } while (line.split(",").length < 5);
    }
    String[] fields = line.split(",");
    if (fields.length > 5) {
      for (int i = 2; i < fields.length - 3; i++) {
        fields[1] = fields[1].concat(",").concat(fields[i]);
      }
    }
    return fields;
  }

  public boolean isSnomed(String s) {
    return snomedCodes.contains(s);
  }

  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, r2Terms, r2Desc, visionRead2Code, visionRead2toSnomed);
  }

  @Override
  public void close() throws Exception {
    if (snomedCodes != null) snomedCodes.clear();
    if (emisRead != null) emisRead.clear();
    codeToConcept.clear();
    r2TermIdMap.clear();
    preferredId.clear();

    importMaps.close();

  }

  private void addMoreReadCodes() throws TTFilerException {
    for (Map.Entry<String, TTEntity> entry : emisRead.entrySet()) {
      String code = entry.getKey();
      if (codeToConcept.get(code) == null)
        document.addEntity(entry.getValue());
      else {
        TTEntity vision = codeToConcept.get(code);
        TTEntity read = entry.getValue();
        if (read.get(iri(IM.MATCHED_TO)) != null) {
          if (vision.get(iri(IM.MATCHED_TO)) == null) {
            vision.set(iri(IM.MATCHED_TO), read.get(iri(IM.MATCHED_TO)));
          } else {
            for (TTValue snoExtra : read.get(iri(IM.MATCHED_TO)).iterator()) {
              if (!vision.get(iri(IM.MATCHED_TO)).contains(snoExtra)) {
                vision.get(iri(IM.MATCHED_TO)).add(snoExtra);
              }
            }
          }
        }
      }
    }
  }

  private void addMissingMaps() {
    for (Map.Entry<String, TTEntity> entry : codeToConcept.entrySet()) {
      String code = entry.getKey();
      TTEntity vision = entry.getValue();
      if (vision.get(iri(IM.MATCHED_TO)) == null) {
        if (emisRead.get(code) != null) {
          vision.addObject(iri(IM.MATCHED_TO), emisRead.get(code).get(iri(IM.MATCHED_TO)).asIriRef());
        }
      }

    }
  }

  private void importR2Terms(String folder) throws IOException, CsvValidationException {

    Path file = ImportUtils.findFileForId(folder, r2Terms[0]);
    LOG.info("Importing official R2 terms as vision");

    try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
      reader.readNext();
      int count = 0;
      String[] fields;
      while ((fields = reader.readNext()) != null) {
        count++;
        if ("C".equals(fields[1])) {
          String termid = fields[0];
          String term = fields[3];
          TTEntity readConcept = r2TermIdMap.get(termid);
          if (readConcept != null) {
            if (preferredId.contains(termid))
              readConcept.setName(term);
            else {
              TTManager.addTermCode(readConcept, term, null);
            }
          }
        }
      }
      LOG.info("Process ended with {} read 2 terms", count);
    }
  }

  private void importEmis() throws IOException {
    LOG.info("Importing EMIS/Read from IM for look up....");
    emisRead = importMaps.getEMISReadAsVision(Graph.IM);

  }

  private void importR2Desc(String folder) throws IOException {

    Path file = ImportUtils.findFileForId(folder, r2Desc[0]);
    LOG.info("Importing R2 entities");

    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine(); // NOSONAR - Skip header
      String line = reader.readLine();

      int count = 0;
      while (line != null && !line.isEmpty()) {

        String[] fields = line.split(",");
        if ("C".equals(fields[6])) {
          String code = fields[0];
          if (!code.startsWith(".")) {
            if (!Character.isLowerCase(code.charAt(0))) {
              TTEntity readConcept = codeToConcept.get(code);
              if (readConcept == null) {
                String lname = code.replaceAll("[.&/'| ()^]", "_");
                lname = lname.replace("[", "_").replace("]", "_");
                readConcept = new TTEntity()
                  .setIri(Namespace.VISION + lname)
                  .setCode(code)
                  .setStatus(iri(IM.ACTIVE))
                  .setScheme(iri(Namespace.VISION))
                  .addType(iri(IM.CONCEPT));
                document.addEntity(readConcept);
                codeToConcept.put(code, readConcept);
              }
              String termId = fields[1];
              String preferred = fields[2];
              if (preferred.equals("P"))
                preferredId.add(termId);
              r2TermIdMap.put(termId, readConcept);
            }
            count++;
            if (count % 50000 == 0) {
              LOG.info("Processed {} read code termid links", count);
            }
          }
        }
        line = reader.readLine();
      }
      LOG.info("Process ended with {} read code term id links", count);
    }
  }

  private void createHierarchy() {
    LOG.info("Creating child parent hierarchy");
    TTEntity vision = new TTEntity()
      .setIri(Namespace.VISION + "VisionCodes")
      .setName("Vision read 2 and localcodes")
      .addType(iri(IM.CONCEPT))
      .setCode("VisionCodes")
      .setScheme(iri(Namespace.VISION))
      .setDescription("Vision and read 2 codes mapped to core");
    vision.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "CodeBasedTaxonomies"));
    document.addEntity(vision);
    for (TTEntity entity : document.getEntities()) {
      String shortCode = entity.getCode();
      if (shortCode != null) {
        if (shortCode.contains("."))
          shortCode = shortCode.substring(0, shortCode.indexOf("."));
        if (shortCode.length() == 1)
          entity.set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(vision.getIri())));
        else {
          StringBuilder parent = new StringBuilder(shortCode.substring(0, shortCode.length() - 1));
          while (parent.length() < 5) {
            parent.append("_");
          }
          entity.set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(Namespace.VISION.toString() + parent)));
        }
      }
    }
  }

  private void importVisionCodes(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, visionRead2Code[0]);
    LOG.info("Retrieving terms from vision read+lookup2");
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine(); // NOSONAR - Skip header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        String[] fields = readQuotedCSVLine(reader, line);
        count++;
        if (count % 50000 == 0) {
          LOG.info("{} codes imported ", count);
        }
        String code = fields[0];
        String term = fields[1];
        code = code.replace("\"", "");
        term = term.substring(1, term.length() - 1);
        String lname = code.replaceAll("[.&/'| ()^]", "_");
        lname = lname.replace("[", "_").replace("]", "_");
        if (!code.startsWith(".") && !Character.isLowerCase(code.charAt(0)) && codeToConcept.get(code) == null) {
          TTEntity c = new TTEntity();
          c.setIri(Namespace.VISION + lname);
          c.addType(iri(IM.CONCEPT));
          c.setName(term);
          c.setCode(code);
          c.setScheme(iri(Namespace.VISION));
          document.addEntity(c);
          codeToConcept.put(code, c);
        }
        line = reader.readLine();
      }
      LOG.info("Process ended with {} additional Vision read like codes created", count);
    }
  }

  private void addVisionMaps(String folder) throws IOException {
    Path file = ImportUtils.findFileForId(folder, visionRead2toSnomed[0]);
    LOG.info("Retrieving Vision snomed maps");
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      reader.readLine(); // NOSONAR - Skip header
      String line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split(",");
        count++;
        if (count % 50000 == 0) {
          LOG.info("{} maps added ", count);
        }
        String code = fields[0];
        String snomed = fields[1];
        TTEntity vision = codeToConcept.get(code);
        if (vision != null) {
          if (isSnomed(snomed)) {
            vision.addObject(iri(IM.MATCHED_TO), iri(Namespace.SNOMED + snomed));
          }
        }
        line = reader.readLine();
      }
      LOG.info("Process ended with {}", count);
    }
  }
}
