package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.ZipUtils;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class EMISImport implements TTImport {
  public static final String EMIS = "http://endhealth.info/emis#";
  private static final Logger LOG = LoggerFactory.getLogger(EMISImport.class);
  private static final String[] emisCodes = {".*\\\\EMIS\\\\emis_codes.zip"};
  private static final String[] allergies = {".*\\\\EMIS\\\\Allergies.json"};
  private static final String[] drugIds = {".*\\\\EMIS\\\\EMISDrugs.zip"};
  private static final String[] localCodeMaps = {".*\\\\EMIS\\\\LocalCodeMaps.txt"};
  private final Map<String, TTEntity> codeIdToEntity = new HashMap<>();
  private final Map<String, TTEntity> oldCodeToEntity = new HashMap<>();
  private final Map<String, TTEntity> conceptIdToEntity = new HashMap<>();
  private final Map<String, TTEntity> snomedToEmis = new HashMap<>();
  private final Map<String, TTEntity> termToEmis = new HashMap<>();
  private final Map<String, List<String>> parentMap = new HashMap<>();
  private final Map<String, String> remaps = new HashMap<>();
  private final Map<String, String> alternateParents = new HashMap<>();
  private final TTManager manager = new TTManager();
  List<String> emisNs = Arrays.asList("1000006", "1000033", "1000034", "1000035", "1000027");
  private TTDocument document;


  public EMISImport() {
  }

  public static void populateRemaps(Map<String, String> remaps) {
    remaps.put("65O2", "116813009");
    remaps.put("65O3", "268504008");
    remaps.put("65O4", "271498007");
    remaps.put("65O5", "384702009");
    remaps.put("65OZ", "709562004");
  }

  /**
   * Imports EMIS , Read and EMIS codes and creates term code map to Snomed or local legacy entities
   * Requires vision maps to be populated
   *
   * @param config import configuration data
   * @throws Exception From document filer
   */


  public void importData(TTImportConfig config) throws ImportException {
    try {
      LOG.info("Retrieving filed snomed codes");
      document = manager.createDocument();
      document.addEntity(manager.createScheme(SCHEME.EMIS, "EMIS codes",
        "The EMIS code scheme including codes directly matched to UK Snomed-CT, and EMIS unmatched local codes."));

      checkAndUnzip(config.getFolder());

      LOG.info("importing emis code file");
      populateRemaps(remaps);
      populateAlternateParents();
      addEMISTopLevel();
      importEMISCodes(config.getFolder());
      importDrugs(config.getFolder());
      importLocalCodeMaps(config.getFolder());
      manager.createIndex();

      allergyMaps(config.getFolder());
      setEmisHierarchy();
      //addExtraMatches();
      supplementary();
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  public void populateAlternateParents() {
    alternateParents.put("1994021000006104", "271649006");
  }

  public boolean isBlackList(String code) {
    String[] blacklist = {"373873005"};
    return Arrays.asList(blacklist).contains(code);

  }

  public Boolean isSnomed(String s) {

    if (getNameSpace(s).equals(""))
      return true;
    return !emisNs.contains(getNameSpace(s));
  }

  public String getNameSpace(String s) {
    if (s.length() > 10)
      return s.substring(s.length() - 10, s.length() - 3);
    else
      return "";
  }

  public String getEmisCode(String code, String term) {

    int index = code.indexOf(".");
    if (index != -1) {
      code = code.substring(0, index);
    }
    if ("00".equals(term.substring(0, 2))) {
      return code;
    } else if (term.startsWith("1")) {
      return code + "-" + term.charAt(1);
    } else {
      return code + "-" + term.substring(0, 2);
    }
  }

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, emisCodes, allergies, localCodeMaps);
  }

  @Override
  public void close() throws Exception {
    conceptIdToEntity.clear();
    codeIdToEntity.clear();
    snomedToEmis.clear();
    termToEmis.clear();
    parentMap.clear();
    remaps.clear();
    manager.close();
  }

  private void checkAndUnzip(String folder) throws IOException {
    List<String> zippedFiles = Arrays.asList("emis_codes", "EMISDrugs");
    LOG.info("Checking for EMIS file updates");

    for (String filename : zippedFiles) {
      Path zipFile = ImportUtils.findFileForId(folder, ".*\\\\EMIS\\\\" + filename + ".zip");
      FileTime zipDate = getFileTimestamp(zipFile);
      LOG.info("{} - {}", zipFile, zipDate);

      Path txtFile = null;
      FileTime txtDate = null;
      try {
        txtFile = ImportUtils.findFileForId(folder, ".*\\\\EMIS\\\\" + filename + ".txt");
        txtDate = getFileTimestamp(txtFile);
        LOG.info("{} - {}", txtFile, txtDate);
      } catch (Exception e) {
        LOG.info("No text file found");
      }

      if (null == txtFile || null == txtDate || zipDate.compareTo(txtDate) > 0) {
        LOG.info("Extracting updated {}...", zipFile);
        ImportUtils.unzipArchive(zipFile.toString(), zipFile.getParent().toString());
      }
    }
  }

  private FileTime getFileTimestamp(Path file) throws IOException {
    BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
    return attr.lastModifiedTime();
  }

  private void importLocalCodeMaps(String folder) throws IOException {
    LOG.info("Adding local code maps");
    Path file = ImportUtils.findFileForId(folder, localCodeMaps[0]);
    Set<String> activeConcepts = new HashSet<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
      String line = reader.readLine();
      line = reader.readLine();
      int count = 0;
      while (line != null && !line.isEmpty()) {
        count++;
        String[] fields = line.split("\t");
        String emisCodeId = fields[0];
        String snomedCode = fields[1];
        String status = fields[2];
        String descid = "";
        String name = "";
        if (fields.length > 4) {
          descid = fields[3];
          name = fields[4];
        }
        if (status.equals(IM.INACTIVE) && activeConcepts.contains(emisCodeId))
          continue;

        TTEntity emisEntity = codeIdToEntity.get(emisCodeId);
        emisEntity.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + snomedCode));
        if (status.equals(IM.ACTIVE))
          activeConcepts.add(emisCodeId);
        if (!descid.equals("")) {
          setDescriptionId(descid, name, emisEntity);
        }
        line = reader.readLine();
      }
    }
  }

  private void setDescriptionId(String descid, String name, TTEntity emisEntity) {
    if (notFoundValue(emisEntity, iri(IM.HAS_TERM_CODE), iri(IM.CODE), descid)) {
      TTNode termCode = new TTNode();
      termCode.set(iri(IM.CODE), TTLiteral.literal(descid));
      termCode.set(iri(RDFS.LABEL), TTLiteral.literal(name));
      emisEntity.addObject(iri(IM.HAS_TERM_CODE), termCode);
    }
  }

  private void importDrugs(String folder) throws IOException {
    Path zip = ImportUtils.findFileForId(folder, drugIds[0]);
    File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      reader.readLine();
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        String codeId = fields[0];
        String snomed = fields[1];
        String term = fields[2];
        TTEntity emisConcept = codeIdToEntity.get(codeId);
        if (emisConcept == null) {
          emisConcept = new TTEntity()
            .setIri(EMIS + codeId)
            .setScheme(TTIriRef.iri(EMIS))
            .setName(term)
            .addType(TTIriRef.iri(IM.CONCEPT))
            .set(iri(IM.CODE_ID), TTLiteral.literal(codeId));
          TTEntity mainConcept = termToEmis.get(term);
          String code = codeId;
          if (mainConcept != null)
            code = mainConcept.getCode();
          emisConcept.setCode(code);
          document.addEntity(emisConcept);
        }
        if (emisConcept.getStatus() == null) {
          emisConcept.setStatus(snomed.equals("NULL") ? iri(IM.INACTIVE) : iri(IM.ACTIVE));
        }
        if (notFoundValue(emisConcept, iri(IM.HAS_TERM_CODE), iri(IM.CODE), codeId))
          TTManager.addTermCode(emisConcept, term, codeId);
        if (!snomed.equals("NULL")) {
          emisConcept.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + snomed));
        }
        line = reader.readLine();
      }
    }
  }

  private void supplementary() {
    addSub("EMISNQDT1", "310551000000106");
    addSub("EMISNQHA21", "428975001");
    addSub("TRISHE2", "16584000");
    addSub("EMISNQRO5", "415354003");
    addSub("EMISNQ1S1", "414259000");
    addSub("EMISNQ2N1", "415507003");
    addSub("EMISNQ3R1", "415712004");
  }

  private void addSub(String child, String parent) {
    TTEntity childEntity = oldCodeToEntity.get(child);
    childEntity.addObject(iri(IM.MATCHED_TO), iri(SNOMED.NAMESPACE + parent));
  }

  private void allergyMaps(String folder) throws IOException {
    Path path = ImportUtils.findFileForId(folder, allergies[0]);
    try (TTManager allMgr = new TTManager()) {
      TTDocument allDoc = allMgr.loadDocument(path.toFile());
      for (TTEntity all : allDoc.getEntities()) {
        String oldCode = all.getIri().substring(all.getIri().lastIndexOf("#") + 1);
        oldCode = oldCode.replaceAll("_", ".");
        TTEntity emisEntity = oldCodeToEntity.get(oldCode);
        if (all.get(iri(IM.MATCHED_TO)) != null) {
          for (TTValue superClass : all.get(iri(IM.MATCHED_TO)).getElements()) {
            emisEntity.addObject(iri(IM.MATCHED_TO), superClass);
          }
        } else if (all.get(iri(IM.ROLE_GROUP)) != null)
          emisEntity.set(iri(IM.ROLE_GROUP), all.get(iri(IM.ROLE_GROUP)));
        if (all.get(iri(RDFS.SUBCLASS_OF)) != null) {
          for (TTValue sup : all.get(iri(RDFS.SUBCLASS_OF)).getElements()) {
            emisEntity.addObject(iri(RDFS.SUBCLASS_OF), sup.asIriRef());
          }
        }
      }
    }
  }

  private void setEmisHierarchy() {
    LOG.info("Creating local code subclasses of core");
    for (Map.Entry<String, List<String>> entry : parentMap.entrySet()) {
      String child = entry.getKey();
      TTEntity childEntity = codeIdToEntity.get(child);
      if (childEntity.get(iri(IM.MATCHED_TO)) == null) {
        if (alternateParents.get(childEntity.getCode()) != null) {
          childEntity.addObject(iri(IM.LOCAL_SUBCLASS_OF), TTIriRef.iri(SNOMED.NAMESPACE + alternateParents.get(childEntity.getCode())));
        } else {
          Set<String> coreParents = new HashSet<>();
          getCoreParents(child, coreParents);
          if (!coreParents.isEmpty()) {
            for (String parent : coreParents) {
              childEntity.addObject(iri(IM.LOCAL_SUBCLASS_OF), TTIriRef.iri(parent));
            }
          }
        }
      }
    }
  }

  private void getCoreParents(String child, Set<String> coreParents) {
    if (parentMap.get(child) != null) {
      for (String parentId : parentMap.get(child)) {
        TTEntity parentEntity = codeIdToEntity.get(parentId);
        if (parentEntity.get(iri(IM.MATCHED_TO)) != null) {
          for (TTValue parent : parentEntity.get(iri(IM.MATCHED_TO)).getElements()) {
            coreParents.add(parent.asIriRef().getIri());
          }
        } else {
          String parent = parentEntity.get(iri(IM.CODE_ID)).asLiteral().getValue();
          getCoreParents(parent, coreParents);
        }
      }
    }
  }

  private void addEMISTopLevel() {
    TTEntity c = new TTEntity().setIri(EMIS + "EMISOrphanCodes")
      .set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(EMIS + "1669671000006112")))
      .setName("EMIS unmatched orphan codes")
      .addType(iri(IM.CONCEPT))
      .setDescription("EMIS orphan codes that have no parent and are not matched to UK Snomed-CT." +
        " Each has a code id and an original text code and an EMIS Snomed concept id but no parent code")
      .setScheme(iri(SCHEME.EMIS));
    document.addEntity(c);
    document.addEntity(c);
  }

  private void importEMISCodes(String folder) throws IOException {
    Path zip = ImportUtils.findFileForId(folder, emisCodes[0]);
    File file = ZipUtils.unzipFile(zip.getFileName().toString(), zip.getParent().toString());
    //place holder for unlinked emis codes betlow the emis root code
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      reader.readLine();
      int count = 0;
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        String[] fields = line.split("\t");
        count++;
        if (count % 100000 == 0)
          LOG.info("Imported {} emis codes for " + SCHEME.EMIS, count);

        EmisCode ec = new EmisCode();
        ec.setCodeId(fields[0]);
        if (fields[0].equals("29711000033114"))
          System.out.println("here");
        ec.setTerm(fields[2]);
        ec.setCode(fields[3]);
        ec.setConceptId(fields[4]);
        if (isBlackList(fields[4])) {
          ec.setConceptId(fields[3].replaceAll("\\^", "").replaceAll("-", "_"));
          // LOG.warn(ec.getConceptId());
        }

        ec.setDescid(fields[5]);
        ec.setSnomedDescripton(fields[6]);
        if (fields.length == 14)
          if (!fields[13].equals(""))
            ec.setParentId(fields[13]);
        addConcept(ec);
        line = reader.readLine();
      }
      LOG.info("{} codes imported", count);
    }

  }

  private void addConcept(EmisCode ec) {
    String codeId = ec.getCodeId();
    if (codeId.equals("192931000006112")) return;
    String term = ec.getTerm();
    String code = ec.getCode();
    String conceptId = ec.getConceptId();
    String descid = ec.getDescid();
    String parentId = ec.getParentId();
    if (parentId != null)
      if (parentId.equals("") | parentId.equals("NULL"))
        parentId = null;
    if (descid.equals("") | descid.equals("NULL"))
      descid = null;


    String name = (term.length() <= 250)
      ? term
      : (term.substring(0, 200) + "...");
    TTEntity emisConcept = codeIdToEntity.get(codeId);
    if (emisConcept == null) {
      if (remaps.get(code) != null)
        conceptId = remaps.get(code);
      emisConcept = new TTEntity()
        .setIri(SCHEME.EMIS + codeId)
        .setCode(ec.conceptId)
        .set(TTIriRef.iri(IM.ALTERNATIVE_CODE), TTLiteral.literal(code))
        .addType(iri(IM.CONCEPT))
        .setScheme(iri(SCHEME.EMIS));
      emisConcept
        .setName(name);

      codeIdToEntity.put(codeId, emisConcept);
      document.addEntity(emisConcept);
    }
    emisConcept.addObject(iri(IM.CODE_ID), codeId);
    setDescriptionId(descid, name, emisConcept);

    oldCodeToEntity.put(code, emisConcept);
    termToEmis.put(term, emisConcept);
    if (isSnomed(conceptId)) {
      if (!isBlackList(conceptId)) {
        emisConcept.setStatus(iri(IM.INACTIVE));
        emisConcept.setName(name + " (emis code id)");
        snomedToEmis.put(conceptId, emisConcept);
        if (notFound(emisConcept, iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + conceptId)))
          emisConcept.addObject(iri(IM.MATCHED_TO), TTIriRef.iri(SNOMED.NAMESPACE + conceptId));
      }
    }
    if (code.equals("EMISNHH2")) {
      emisConcept.set(iri(IM.IS_CONTAINED_IN), new TTArray()
        .add(iri(IM.NAMESPACE + "CodeBasedTaxonomies")));
    } else {
      if (parentId == null && emisConcept.get(iri(IM.MATCHED_TO)) == null) {
        emisConcept.set(iri(IM.IS_CHILD_OF), new TTArray().add(iri(EMIS + "EMISOrphanCodes")));
      } else if (parentId != null) {
        emisConcept.addObject(iri(IM.IS_CHILD_OF), TTIriRef.iri(EMIS + parentId));
        parentMap.computeIfAbsent(codeId, k -> new ArrayList<>());
        parentMap.get(codeId).add(parentId);
      }
    }
  }

  private boolean notFound(TTNode node, TTIriRef predicate, TTValue value) {
    if (node.get(predicate) == null)
      return true;
    return !node.get(predicate).getElements().contains(value);
  }

  private boolean notFoundValue(TTNode node, TTIriRef predicate, TTIriRef subPredicate, String code) {
    if (node.get(predicate) == null)
      return true;
    for (TTValue subNode : node.get(predicate).getElements()) {
      if (subNode.asNode().get(subPredicate) == null)
        return true;
      else {
        for (TTValue already : subNode.asNode().get(subPredicate).getElements()) {
          if (already.asLiteral().equals(TTLiteral.literal(code)))
            return false;
        }
      }
    }
    return true;
  }
}
