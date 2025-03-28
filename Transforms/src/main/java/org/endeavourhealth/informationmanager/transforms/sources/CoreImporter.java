package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.logic.reasoner.Reasoner;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.endeavourhealth.imapi.model.tripletree.TTVariable.iri;

public class CoreImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(CoreImporter.class);
  private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};

  private static final String[] coreEntities = {
    ".*\\\\SemanticWeb\\\\RDFOntology.json",
    ".*\\\\SemanticWeb\\\\RDFSOntology.json",
    ".*\\\\SemanticWeb\\\\OWLOntology.json",
    ".*\\\\SemanticWeb\\\\SHACLOntology.json",
    ".*\\\\DiscoveryCore\\\\CoreOntology.json",
    ".*\\\\DiscoveryCore\\\\CoreOntology-more-inferred.json",
    ".*\\\\DiscoveryCore\\\\StatsReports.json",
    ".*\\\\DiscoveryCore\\\\QueryHelpers.json",
    ".*\\\\DiscoveryCore\\\\Sets.json",
    ".*\\\\DiscoveryCore\\\\Sets-KnowDiabetes.json"
  };

  private static final String INFERRED_SUFFIX = "-inferred.json";

  private static void generateInferred(TTImportConfig config) throws IOException, OWLOntologyCreationException {

    for (String coreFile : coreEntities) {
      if (!coreFile.contains(INFERRED_SUFFIX)) {
        TTManager manager = new TTManager();
        Path path = ImportUtils.findFileForId(config.getFolder(), coreFile);
        TTDocument document = manager.loadDocument(path.toFile());
        if (".*\\\\DiscoveryCore\\\\CoreOntology.json".equals(coreFile)) {
          codeToString(document);
        }
        LOG.info("Generating inferred document from {}", coreFile);
        Reasoner reasoner = new Reasoner();
        TTDocument inferred = reasoner.generateInferred(document);
        inferred = reasoner.inheritShapeProperties(inferred);
        manager = new TTManager();
        manager.setDocument(inferred);
        String inferredFile = path.toString().substring(0, path.toString().indexOf(".json")) + INFERRED_SUFFIX;
        manager.saveDocument(new File(inferredFile));

      }

    }
  }

  public static void codeToString(TTDocument document) {
    document.getEntities().forEach(e -> {
      if (e.getCode() != null) {
        String code = e.getCode().toString();
        e.setCode(code);
      }
    });
  }

  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, coreEntities,lookups);
  }

  /**
   * Imports the core ontology document
   *
   * @param config import config
   * @return TTImport object builder pattern
   * @throws Exception invalid document
   */
  @Override
  public void importData(TTImportConfig config) throws ImportException {
    try {
      LOG.info("Generating inferred ontologies...");
      generateInferred(config);
      LOG.info("Importing Core entities");
      for (String coreFile : coreEntities) {
        if (!coreFile.contains(INFERRED_SUFFIX))
          coreFile = coreFile.substring(0, coreFile.indexOf(".json")) + INFERRED_SUFFIX;
        try (TTManager manager = new TTManager()) {
          Path path = ImportUtils.findFileForId(config.getFolder(), coreFile);
          manager.loadDocument(path.toFile());
          TTDocument document = manager.getDocument();
          if (coreFile.endsWith("CoreOntology-inferred.json")) {
            generateDefaultCohorts(manager);
          }
          LOG.info("Filing {} from {}", document.getGraph().getIri(), coreFile);
          try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            try {
              filer.fileDocument(document);
            } catch (TTFilerException | QueryException e) {
              throw new IOException(e.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
    try (CoreEthnicityImport ethnicityImport = new CoreEthnicityImport()) {
      ethnicityImport.importData(config);
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }

  }

  private void generateDefaultCohorts(TTManager manager) throws JsonProcessingException {
      List<String> shapeFolders = new ArrayList<>();
      List<TTEntity> cohortEntities= new ArrayList<>();
      for (TTEntity entity:manager.getDocument().getEntities()) {
        if (entity.get(iri(IM.IS_CONTAINED_IN))!=null) {
          if (entity.get(iri(IM.IS_CONTAINED_IN)).getElements().stream().filter(f->f.asIriRef().equals(iri(IM.NAMESPACE+"HealthRecords"))).findAny().isPresent()){
            String cohortFolderIri = IM.NAMESPACE + "Q_" + entity.getIri().substring(entity.getIri().lastIndexOf("#") + 1);
            TTEntity cohortFolder = new TTEntity()
              .addType(iri(IM.FOLDER))
              .setIri(cohortFolderIri)
              .setName(entity.getName())
              .set(iri(IM.CONTENT_TYPE),iri(IM.COHORT_QUERY))
              .set(iri(SHACL.ORDER), TTLiteral.literal(entity.get(iri(SHACL.ORDER)).asLiteral().intValue()+1));
            cohortFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE+"Q_DefaultCohorts"));
            cohortEntities.add(cohortFolder);
            shapeFolders.add(entity.getIri());
          }
        }
      }
    for (TTEntity entity:manager.getDocument().getEntities()) {
        if (entity.isType(iri(SHACL.NODESHAPE))) {
          if (entity.get(IM.IS_CONTAINED_IN)!=null) {
            for (TTValue folder : entity.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
              if (shapeFolders.contains(folder.asIriRef().getIri())) {
                String shapeFolderIri = folder.asIriRef().getIri();
                String cohortFolderIri = IM.NAMESPACE + "Q_" + shapeFolderIri.substring(shapeFolderIri.lastIndexOf("#") + 1);
                int order = entity.get(iri(SHACL.ORDER))!=null ?entity.get(iri(SHACL.ORDER)).asLiteral().intValue() : 1000;
                TTEntity cohort = new TTEntity()
                  .setIri(IM.NAMESPACE + "Q_" + entity.getIri().substring(entity.getIri().lastIndexOf("#") + 1))
                  .addType(iri(IM.COHORT_QUERY))
                  .setName(entity.getName()+"s")
                  .setDescription("Cohort Query for entities of "+entity.getName()+"s")
                  .set(iri(SHACL.ORDER), TTLiteral.literal(order+1));
                cohort.addObject(iri(IM.IS_CONTAINED_IN), iri(cohortFolderIri));
                cohort.set(iri(IM.DEFINITION), TTLiteral.literal(new Query()
                  .setIri(cohort.getIri())
                  .setName(cohort.getName())
                  .setTypeOf(IM.NAMESPACE + cohort.getIri().substring(cohort.getIri().lastIndexOf("#") + 1).split("Q_")[1])));
                cohortEntities.add(cohort);
              }
            }
          }
        };
      }
    manager.getDocument().getEntities().addAll(cohortEntities);
  }



  @Override
  public void close() throws Exception {

  }
}
