package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.XlsxExpander;
import org.endeavourhealth.informationmanager.transforms.ZipUtils;
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

public class QOFRefSetImport implements TTImport {

  private static final String[] qofRefSets = {".*\\\\QOF\\\\Static_Expanded_cluster_lists_Ruleset-level_adhoc_.*\\.zip"};
  private static final String[] expandedRefSets = {".*\\\\QOF\\\\RefSets.*\\.txt"};



  public static final String PCDFolder = Namespace.IM + "PCDClusters";
  private static final Logger LOG = LoggerFactory.getLogger(QOFRefSetImport.class);
  private Map<String, TTEntity> qofMap;
  private TTDocument document;
  private int setCount=0;




  public void importData(TTImportConfig config) throws ImportException {
    try {
      Path zip = ImportUtils.findFileForId(config.getFolder(), qofRefSets[0]);
      ZipUtils.unzipFiles(zip.getFileName().toString(), zip.getParent().toString(),zip.getParent().toString()+"/RefSets/");
      XlsxExpander excelExpander= new XlsxExpander();
      excelExpander.exportXlsToTxt(config.getFolder() + "/QOF/RefSets/");
    } catch (IOException e) {
      throw new ImportException(e.getMessage());
    }
    try (TTManager manager = new TTManager()) {
      document= manager.createDocument();
      qofMap = new HashMap<>();
      document = new TTDocument();
      processSets(config.getFolder());
      LOG.info("Imported {} qof refsets", setCount);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
          filer.fileDocument(document);
        }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }
  private void importExpandedRefsetFiles(String path) throws IOException {
    int i = 0;
    LOG.info("Importing expanded refsets");
    for (String refsetFile :expandedRefSets) {
      List<Path> paths = ImportUtils.findFilesForId(path, refsetFile);
      for (Path file :paths) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
          String line = reader.readLine();
          while (line != null && !line.isEmpty()) {
            processExpandedRefsetLine(line);
            i++;
            line = reader.readLine();
          }

        }
      }
    }
    LOG.info("Imported {} refset", i);
  }
  private void processExpandedRefsetLine(String line) {
    String[] fields = line.split("\t");
    String setIri=Namespace.SNOMED+(fields[6].replace("^",""));
    TTEntity c = qofMap.get(setIri);
    if (c==null) {
      setCount++;
      LOG.info("Creating refset {}", fields[2]);
      TTEntity set = new TTEntity()
        .set(iri(IM.ALTERNATIVE_CODE), TTLiteral.literal(fields[1]))
        .setIri(setIri)
        .setName(fields[2])
        .setScheme(Namespace.SNOMED.asIri())
        .setCrud(iri(IM.UPDATE_PREDICATES))
        .setType(new TTArray().add(iri(IM.CONCEPT_SET)));
      document.addEntity(set);
      c = new TTEntity()
        .setIri(setIri)
          .setCrud((iri(IM.ADD_QUADS)))
        .addObject(iri(IM.IS_CONTAINED_IN), iri(PCDFolder));
      TTManager.addTermCode(c, fields[1], fields[1], iri(IM.ACTIVE));
      document.addEntity(c);
      qofMap.put(setIri, c);
    }
    c.addObject(iri(IM.HAS_MEMBER), iri(Namespace.SNOMED + fields[3]));
  }



  @Override
  public void validateFiles(String inFolder) throws TTFilerException {

    ImportUtils.validateFiles(inFolder, qofRefSets);

  }

  private void processSets(String path) throws IOException {
    TTEntity clusters = new TTEntity()
      .setIri(PCDFolder)
      .setName("Primary Care Code clusters")
      .setDescription("PCD portal  code cluster, reference sets , which are a subset of the Snomed-CT reference sets. The content of these are sourced from the UK Snomed-CT releases.")
      .setScheme(Namespace.SNOMED.asIri())
      .addType(iri(IM.FOLDER));
    clusters.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
    clusters
      .addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "QueryConceptSets"));
    document.addEntity(clusters);
    importExpandedRefsetFiles(path);
  }









  @Override
  public void close() throws Exception {

  }
}

