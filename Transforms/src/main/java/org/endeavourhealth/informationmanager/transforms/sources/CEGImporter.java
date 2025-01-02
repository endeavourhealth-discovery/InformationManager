package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.Properties;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CEGImporter implements TTImport {

  private static final String[] queries = {".*\\\\CEGQuery"};
  private static final String[] annotations = {".*\\\\QueryAnnotations.properties"};
  private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
  private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
  private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
  private TTDocument document;
  private String mainFolder;
  private String setFolder;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try (CEGEthnicityImport ethnicityImport = new CEGEthnicityImport()) {
      ethnicityImport.importData(config);
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }
    /* Not longer active
    try (
      TTManager manager = new TTManager()) {
      document = manager.createDocument(GRAPH.CEG);
      createGraph();
      createOrg();
      createFolders();
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
        filer.fileDocument(document);
      }
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }
    try {
      loadAndConvert(config.getFolder());
    }
    catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }

     */

  }


  private void createGraph(){
    TTEntity graph = new TTEntity()
      .setIri(GRAPH.CEG)
      .setName("CEG (QMUL) graph")
      .setDescription("CEG library of value sets, queries and profiles")
      .addType(iri(IM.GRAPH));
    graph.addObject(iri(RDFS.SUBCLASS_OF), iri(IM.GRAPH));
    document.addEntity(graph);
  }


  private void createFolders() {
    TTEntity folder = new TTEntity()
      .setIri(GRAPH.CEG + "Q_CEGQueries")
      .setName("QMUL CEG query library")
      .addType(iri(IM.FOLDER))
      .set(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "Q_Queries"));
    folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
    document.addEntity(folder);
    mainFolder= folder.getIri();
    folder = new TTEntity()
      .setIri(GRAPH.CEG + "CSET_CEGConceptSets")
      .setName("QMUL CEG value set library")
      .addType(iri(IM.FOLDER))
      .set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "QueryConceptSets"));
    folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
    document.addEntity(folder);
    setFolder= folder.getIri();

  }

  private void createOrg() {
    TTEntity owner = new TTEntity()
      .setIri("http://org.endhealth.info/im#QMUL_CEG")
      .addType(TTIriRef.iri(IM.NAMESPACE + "Organisation"))
      .setName("Clinical Effectiveness Group of Queen Mary University of London - CEG")
      .setDescription("The Clinical effectiveness group being a special division of Queen Mary University of London," +
        "deliverying improvements in clinical outcomes for the population of UK");
    document.addEntity(owner);
  }

  public void loadAndConvert(String folder) throws Exception {
    Properties dataMap = new Properties();
    try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, dataMapFile[0]).toFile()))) {
      dataMap.load(reader);
    }

    Properties labels = new Properties();
    try (FileReader reader = new FileReader((ImportUtils.findFileForId(folder, annotations[0]).toFile()))) {
      labels.load(reader);
    }
    Path directory = ImportUtils.findFileForId(folder, queries[0]);
    try (TTManager manager= new TTManager()) {
      EQDImporter eqdImporter = new EQDImporter(manager,dataMap,mainFolder,setFolder);
      eqdImporter.importEqds(GRAPH.CEG, directory);
    }
  }




  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    ImportUtils.validateFiles(inFolder, queries, annotations, duplicates, lookups);
  }


  @Override
  public void close() throws Exception {

  }
}
