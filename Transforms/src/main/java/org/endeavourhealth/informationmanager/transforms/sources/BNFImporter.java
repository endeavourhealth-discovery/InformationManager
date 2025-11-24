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
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class BNFImporter implements TTImport {

  private static final Logger LOG = LoggerFactory.getLogger(BNFImporter.class);

  private TTDocument document;
  private TTManager manager;
  private final Map<String, Set<String>> bnfCodeToSnomed = new HashMap<>();
  private final Map<String, TTEntity> codeToEntity = new HashMap<>();
  private final Map<String, Set<String>> setToSnomed = new HashMap<>();
  private final ImportMaps importMaps = new ImportMaps();
  private final Map<String,TTEntity> vmpMap=new HashMap<>();

  private final String topFolder = Namespace.BNF + "BNFValueSets";
  private final Map<String, Set<String>> children = new HashMap<>();

  public static final String[] bnf_maps = {
    ".*\\\\BNFMaps\\\\BNF Snomed Mapping data.*\\.txt"
  };

  public static final String[] bnf_codes = {
    ".*\\\\BNFMaps\\\\.*_BNF_Code_Information.csv"
  };

  public static final String[] bnf_amp = {
    ".*\\\\BNF\\\\.*\\\\f_bnf_vmp.csv"
  };

  public static final String[] dmd_amp = {
    ".*\\\\DMD\\\\.*\\\\f_amp_AmpType.csv"
  };


  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try {
      manager = new TTManager();
      document = manager.createDocument();
      topFolder();
      importDMD(config.getFolder());
      importBNF_AMP(config.getFolder());
      importMaps(config.getFolder());
      importCodes(config.getFolder());
      setMembers();
      flattenSets();
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
        filer.fileDocument(document);
      }

    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(),ex);
    }
  }

  private void importBNF_AMP(String folder) {
  }

  private void importDMD(String path) throws CsvValidationException, IOException {
    int i = 0;
    for (String map : dmd_amp) {
      Path file = ImportUtils.findFilesForId(path, map).get(0);
      LOG.info("Processing dmd to amp codes in {}", file.getFileName().toString());
      try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
        reader.readNext(); // NOSONAR - Skip header
        String[] fields;
        while ((fields = reader.readNext()) != null) {
          i++;
          if (i%10000==0){
            LOG.info("Processed {} codes",i);
          }
        }
      }
    }

  }


  private void setMembers() {
    LOG.info("Assigning instances to set definition match clause");
    int i=0;
    for (TTEntity entity: document.getEntities()){
      String setIri= entity.getIri();
      if (setToSnomed.get(setIri)!=null){
        i++;
        for (String snomed:setToSnomed.get(setIri)){
          entity.addObject(iri(IM.ENTAILED_MEMBER),new TTNode()
            .set(iri(IM.IS),iri(Namespace.SNOMED+snomed))
            .set(iri(IM.ENTAILMENT),iri(IM.DESCENDANTS_OR_SELF_OF)));
        }
      }
    }
    LOG.info("{} sets defined",i);

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
      if (set.get(iri(IM.ENTAILED_MEMBER)) != null) {
        parent.setType(new TTArray().add(iri(IM.CONCEPT_SET)));
        parent.set(iri(IM.ENTAILED_MEMBER),set.get(iri(IM.ENTAILED_MEMBER)));
        set.getPredicateMap().remove(iri(IM.ENTAILED_MEMBER));
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
      .setScheme(Namespace.IM.asIri())
      .setName("BNF based value set library")
      .setStatus(iri(IM.ACTIVE))
      .setDescription("A library of value sets generated from BNF codes and NHS BNF snomed maps");
    entity.addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "QueryConceptSets"));
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
          i++;
          processCodeLine(fields);
          if (i%10000==0){
            LOG.info("Processed {} codes",i);
          }
        }
      }
    }

  }


  private void importMaps(String path) throws IOException {
    for (String map : bnf_maps) {
      Path file = ImportUtils.findFilesForId(path, map).get(0);
      LOG.info("Processing bnf to snomed maps in {}", file.getFileName().toString());
      try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
        reader.readLine(); // NOSONAR - Skip header
        String line = reader.readLine();
        while (line != null && !line.isEmpty()) {
          processMapLine(line);
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
    if (snomed.contains(" "))
      LOG.error("bad snomed [" + snomed + "]");
    else {
      if (bnfName.equals("") && (bnfCode.equals("")))
        return;
      if (!bnfCode.equals(""))
        bnfCodeToSnomed.computeIfAbsent(bnfCode, s -> new HashSet<>()).add(snomed);
      else {
        String pseudoCode = "bnf_" + bnfName.replace(" ", "");
        bnfCodeToSnomed.computeIfAbsent(pseudoCode, s -> new HashSet<>()).add(snomed);
      }
    }

  }

  private void processCodeLine(String[] fields) {
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
      setNewEntity(chapterCode, chapterDot + " " + chapter + " (BNF based set)", IM.FOLDER.toString(), topFolder, null);
    }
    if (codeToEntity.get(sectionCode) == null) {
      setNewEntity(sectionCode, sectionDot + " " + section + " (BNF based set)", IM.FOLDER.toString(), Namespace.BNF + "BNF_" + chapterCode, null);
    }
    if (codeToEntity.get(paragraphCode) == null) {
      setNewEntity(paragraphCode, paraDot + " " + paragraph + " (BNF based set)", IM.CONCEPT_SET.toString(), Namespace.BNF + "BNF_" + sectionCode, null);
    }
    if (codeToEntity.get(subparagraphCode) == null) {
      setNewEntity(subparagraphCode, subParaDot + " " + subparagraph + " (BNF based set)", IM.CONCEPT_SET.toString(), null, Namespace.BNF + "BNF_" + paragraphCode);
    }
    Set<String> snomeds = bnfCodeToSnomed.get(workingCode);
    if (snomeds != null) {
      String setIri= codeToEntity.get(subparagraphCode).getIri();
      for (String snomed : snomeds) {
        setToSnomed.computeIfAbsent(setIri,s-> new HashSet<>()).add(snomed);
      }

    }



  }


  private void setNewEntity(String code, String name, String type, String parent, String superset) {
    TTEntity entity = new TTEntity()
      .setIri(Namespace.BNF + "BNF_" + code)
      .addType(iri(type))
      .setScheme(Namespace.BNF.asIri())
      .setName(name);
    if (parent != null) {
      entity.addObject(iri(IM.IS_CONTAINED_IN), iri(parent));
      if (code.matches("\\d+")) {
        String parentOrder = parent.split("#")[1];
        int order;
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

    document.addEntity(entity);
    codeToEntity.put(code, entity);
    mapEmisCode(code);
  }

  private void mapEmisCode(String code) {
      String emisCode=String.valueOf(Integer.parseInt(code.substring(0, 2)));
      if (code.length()>2){
        if (code.length()==3 )emisCode=emisCode+"."+Integer.parseInt(code.substring(2, 3));
        else emisCode= emisCode+"."+ Integer.parseInt(code.substring(2, 4));
        if (code.length()>4){
          if (code.length()==5) emisCode=emisCode+"."+Integer.parseInt(code.substring(4, 5));
          else emisCode=emisCode+"."+Integer.parseInt(code.substring(4,6));
          if (code.length()>6){
            if (code.length()==7) emisCode=emisCode+"."+Integer.parseInt(code.substring(6, 7));
            else emisCode=emisCode+"."+Integer.parseInt(code.substring(6,8));
          }
        }
      }
      emisCode="DRGG"+emisCode;
      String emisIri= importMaps.getIriFromLegacyCode(Namespace.EMIS.toString(),emisCode);
      if (emisIri!=null){
        TTEntity emisEntity= new TTEntity()
          .setIri(emisIri)
          .setCrud(iri(IM.ADD_QUADS));
        emisEntity.set(IM.MATCHED_TO,iri(Namespace.BNF+"BNF_"+code));
        document.addEntity(emisEntity);
      }
  }


  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    ImportUtils.validateFiles(inFolder, bnf_maps, bnf_codes,bnf_amp,dmd_amp);
  }

  @Override
  public void close() throws Exception {

  }
}
