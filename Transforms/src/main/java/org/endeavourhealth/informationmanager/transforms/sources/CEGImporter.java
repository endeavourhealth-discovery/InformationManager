package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.library.model.tripletree.TTDocument;
import org.endeavourhealth.library.model.tripletree.TTEntity;
import org.endeavourhealth.library.model.tripletree.TTIriRef;
import org.endeavourhealth.library.transforms.TTManager;
import org.endeavourhealth.library.vocabulary.GRAPH;
import org.endeavourhealth.library.vocabulary.IM;
import org.endeavourhealth.library.vocabulary.NAMESPACE;

import static org.endeavourhealth.library.model.tripletree.TTIriRef.iri;

public class CEGImporter implements TTImport {

  private static final String[] queries = {".*\\\\CEGQuery"};
  private static final String[] dataMapFile = {".*\\\\EQD\\\\EqdDataMap.properties"};
  private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
  private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
  private static final String[] autoNamedSets = {".*\\\\EQD\\\\AutoNamedSets.txt"};
  private static final String[] autoNamedClauses = {".*\\\\EQD\\\\AutoNamedClauses.txt"};
  private String mainFolder;
  private String setFolder;
  private TTImportConfig config;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    this.config= config;

    try (
      TTManager manager = new TTManager()) {
      TTDocument document = manager.createDocument();
      document.addEntity(manager.createNamespaceEntity(NAMESPACE.CEG,"CEG (QMUL) scheme","CEG library of value sets, queries and profiles"));
      createOrg(document);
      createFolders(document);
      EQDImporter eqdImporter = new EQDImporter(false);
      eqdImporter.loadAndConvert(config,manager,queries[0], NAMESPACE.CEG, dataMapFile[0],"criteriaMaps.properties",mainFolder,setFolder,autoNamedSets[0],
        autoNamedClauses[0]);
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
        filer.fileDocument(document);
      } catch (Exception e) {
        throw new ImportException(e.getMessage(), e);
      }
    } catch (Exception e){
      throw new ImportException(e.getMessage(),e);
    }
  }

  private void createFolders(TTDocument document) {
    TTEntity folder = new TTEntity()
      .setIri(NAMESPACE.CEG + "Q_CEGQueries")
      .setName("QMUL CEG query library")
      .addType(iri(IM.FOLDER))
      .set(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM + "Q_Queries"));
    folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
    document.addEntity(folder);
    mainFolder= folder.getIri();
    folder = new TTEntity()
      .setIri(NAMESPACE.CEG + "CSET_CEGConceptSets")
      .setName("QMUL CEG value set library")
      .addType(iri(IM.FOLDER))
      .set(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM + "QueryConceptSets"));
    folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
    document.addEntity(folder);
    setFolder= folder.getIri();

  }

  private void createOrg(TTDocument document) {
    TTEntity owner = new TTEntity()
      .setIri("http://org.endhealth.info/im#QMUL_CEG")
      .addType(iri(NAMESPACE.IM + "Organisation"))
      .setName("Clinical Effectiveness Group of Queen Mary University of London - CEG")
      .setDescription("The Clinical effectiveness group being a special division of Queen Mary University of London," +
        "deliverying improvements in clinical outcomes for the population of UK");
    document.addEntity(owner);
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    ImportUtils.validateFiles(inFolder, queries, duplicates, lookups);
  }

  @Override
  public void close() throws Exception {

  }
}
