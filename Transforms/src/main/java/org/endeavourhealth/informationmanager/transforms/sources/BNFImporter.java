package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class BNFImporter implements TTImport {

  private static final Logger LOG = LoggerFactory.getLogger(BNFImporter.class);

  private TTDocument document;
  private TTManager manager;
  private final Map<String, Set<String>> bnfCodeToSnomed = new HashMap<>();
  private final Map<String, TTEntity> codeToEntity = new HashMap<>();
  private final String topFolder = BNF.NAMESPACE + "BNFValueSets";
  private Map<String, Set<String>> children = new HashMap<>();

  public static final String[] bnf_maps = {
    ".*\\\\BNFMaps\\\\BNF Snomed Mapping data.*\\.txt"
  };

  public static final String[] bnf_codes = {
    ".*\\\\BNFMaps\\\\.*_BNF_Code_Information.csv"
  };

  @Override
  public void importData(TTImportConfig config) throws Exception {
    manager = new TTManager();
    document = manager.createDocument(BNF.NAMESPACE);
    topFolder();
    importMaps(config.getFolder());
    importCodes(config.getFolder());
    flattenSets();
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
      filer.fileDocument(document);
    }
  }

  private void flattenSets() {
    List<TTEntity> toRemove = new ArrayList<>();
    for (TTEntity set : document.getEntities()) {
      if (!toRemove.contains(set)) {
        if (set.isType(iri(IM.CONCEPT_SET))) {
          flattenParent(set, toRemove);
        }
      }
    }
    for (TTEntity remove : toRemove) {
      document.getEntities().remove(remove);
    }
  }

  private void flattenParent(TTEntity set, List<TTEntity> toRemove) {
    TTEntity parent = getParent(set);
    if (!set.getIri().equals(parent.getIri())) {
      toRemove.add(set);
      if (set.get(iri(IM.ROLE_GROUP)) != null) {
        parent.set(iri(IM.ROLE_GROUP), set.get(iri(IM.ROLE_GROUP)));
        String query = set.get(iri(IM.DEFINITION)).asLiteral().getValue();
        query = query.replace(set.getIri(), parent.getIri());
        parent.set(iri(IM.DEFINITION), TTLiteral.literal(query));
        parent.setType(new TTArray().add(iri(IM.CONCEPT_SET)));
      }
    }
  }

  private TTEntity getParent(TTEntity set) {
    if (!set.getIri().endsWith("0"))
      return set;
    TTIriRef parent;
    if (set.get(iri(IM.IS_CONTAINED_IN)) != null) {
      parent = set.get(iri(IM.IS_CONTAINED_IN)).get(0).asIriRef();
    } else
      parent = set.get(iri(IM.IS_SUBSET_OF)).get(0).asIriRef();
    TTEntity parentEntity = manager.getEntity(parent.getIri());
    if (children.get(parentEntity.getIri()).size() == 1) {
      return getParent(parentEntity);
    }
    return set;
  }


  private void topFolder() {
    TTEntity entity = new TTEntity()
      .setIri(topFolder)
      .addType(iri(IM.FOLDER))
      .setName("BNF based value set library")
      .setStatus(iri(IM.ACTIVE))
      .setDescription("A library of value sets generated from BNF codes and NHS BNF snomed maps");
    entity.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "QueryConceptSets"));
    document.addEntity(entity);
  }

  private void importCodes(String path) throws CsvValidationException, IOException {
    int i = 0;
    for (String map : bnf_codes) {
      Path file = ImportUtils.findFilesForId(path, map).get(0);
      LOG.info("Processing bnf codes in {}", file.getFileName().toString());
      try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
        reader.readNext(); // NOSONAR - Skip header
        String[] fields;
        while ((fields = reader.readNext()) != null) {
          processCodeLine(fields);
        }
      }
    }

  }


  private void importMaps(String path) throws IOException {
    int i = 0;
    for (String map : bnf_maps) {
      Path file = ImportUtils.findFilesForId(path, map).get(0);
      LOG.info("Processing bnf to snomed maps in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine(); // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processMapLine(line);
          i++;
          line = reader.readLine();
        }
      }
    }
  }

  private void processMapLine(String line) {
    String[] fields = line.split("\t");
    if (fields.length > 8)  // remove ampps
      return;
    String bnfCode = fields[2];
    String bnfName = fields[3];
    String snomed = fields[4];
    if (bnfName.equals("") && (bnfCode.equals("")))
      return;
    if (!bnfCode.equals(""))
      bnfCodeToSnomed.computeIfAbsent(bnfCode, s -> new HashSet<>()).add(snomed);
    else {
      String pseudoCode = "bnf_" + bnfName.replace(" ", "");
      bnfCodeToSnomed.computeIfAbsent(pseudoCode, s -> new HashSet<>()).add(snomed);
    }


  }

  private void processCodeLine(String[] fields) throws JsonProcessingException {
    String chapter = fields[0];
    String chapterCode = fields[1];
    String section = fields[2];
    String sectionCode = fields[3];
    String paragraph = fields[4];
    String paragraphCode = fields[5];
    String subparagraph = fields[6];
    String subparagraphCode = fields[7];
    String presentation = fields[12];
    String presentationCode = fields[13];
    int chapterDot = Integer.parseInt(chapterCode);
    int chapLength = chapterCode.length();
    String sectionDot = chapterDot + "." + Integer.parseInt(sectionCode.substring(chapLength));
    int sectionLengh = sectionCode.length();
    String paraDot = sectionDot + "." + Integer.parseInt(paragraphCode.substring(sectionLengh));
    int paraLength = paragraphCode.length();
    String subParaDot = paraDot + "." + Integer.parseInt(subparagraphCode.substring(paraLength));
    String workingCode = presentationCode;
    if (bnfCodeToSnomed.get(workingCode) == null) {
      workingCode = "bnf_" + presentation.replace(" ", "");
    }
    if (bnfCodeToSnomed.get(workingCode) == null) {
      return;
    }
    if (codeToEntity.get(chapterCode) == null) {
      setNewEntity(chapterCode, chapterDot + " " + chapter + " (BNF based value sets)", IM.FOLDER, topFolder, null);
    }
    if (codeToEntity.get(sectionCode) == null) {
      setNewEntity(sectionCode, sectionDot + " " + section + " (BNF based value sets)", IM.FOLDER, BNF.NAMESPACE + "BNF_" + chapterCode, null);
    }
    if (codeToEntity.get(paragraphCode) == null) {
      setNewEntity(paragraphCode, paraDot + " " + paragraph + " (BNF based value sets)", IM.CONCEPT_SET, BNF.NAMESPACE + "BNF_" + sectionCode, null);
    }
    if (codeToEntity.get(subparagraphCode) == null) {
      setNewEntity(subparagraphCode, subParaDot + " " + subparagraph + " (BNF based value sets)", IM.CONCEPT_SET, null, BNF.NAMESPACE + "BNF_" + paragraphCode);
    }
    Set<String> snomeds = bnfCodeToSnomed.get(workingCode);
    if (snomeds != null)
      for (String snomed : snomeds) {
        TTEntity set = codeToEntity.get(subparagraphCode);
        if (set.get(iri(IM.ROLE_GROUP)) == null) {
          set.set(iri(IM.ROLE_GROUP), new TTNode());
        }
        set.get(iri(IM.ROLE_GROUP)).asNode().addObject(iri(IM.HAS_MEMBER_PARENT), iri(SNOMED.NAMESPACE + snomed));
      }
  }


  private void setNewEntity(String code, String name, String type, String parent, String superset) throws JsonProcessingException {
    TTEntity entity = new TTEntity()
      .setIri(BNF.NAMESPACE + "BNF_" + code)
      .addType(iri(type))
      .setScheme(iri(BNF.NAMESPACE))
      .setName(name);
    if (parent != null) {
      entity.addObject(iri(IM.IS_CONTAINED_IN), iri(parent));
      if (code.matches("\\d+")) {
        String parentOrder = parent.split("#")[1];
        Integer order;
        if (parentOrder.matches("\\d+")) {
          order = Integer.parseInt(code) - Integer.parseInt(parentOrder);
        } else
          order = Integer.parseInt(code);
        entity.set(iri(SHACL.ORDER), TTLiteral.literal(order));
      }
      children.computeIfAbsent(parent, p -> new HashSet<>()).add(entity.getIri());
    }
    if (superset != null) {
      entity.addObject(iri(IM.IS_SUBSET_OF), iri(superset));
      children.computeIfAbsent(superset, s -> new HashSet<>()).add(entity.getIri());
    }
    if (type.equals(IM.CONCEPT_SET)) {
      entity.set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
        .match(m -> m
          .addInstanceOf(new Node()
            .setDescendantsOrSelfOf(true))
          .where(w -> w
            .setInverse(true)
            .setAnyRoleGroup(true)
            .setIri(IM.HAS_MEMBER_PARENT)
            .setName("that have member parents in")
            .is(i -> i.setIri(entity.getIri()))))));
    }
    document.addEntity(entity);
    codeToEntity.put(code, entity);
  }


  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    ImportUtils.validateFiles(inFolder, bnf_maps, bnf_codes);
  }

  @Override
  public void close() throws Exception {

  }
}
