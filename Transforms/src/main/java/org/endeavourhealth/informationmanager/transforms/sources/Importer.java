package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportByType;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manager Class which imports specialised data from a legacy classification or the core ontology using specialised importers
 */
public class Importer implements TTImportByType {
  private static final Logger LOG = LoggerFactory.getLogger(Importer.class);

  /**
   * Creates a type specific importer and imports and files rthe data
   *
   * @param importType The graph IRI for the particular source data type
   * @param config     Import configuration
   * @return TTImport object for reuse
   * @throws Exception if one of the sources is invalid
   */
  @Override
  public TTImportByType importByType(TTIriRef importType, TTImportConfig config) throws Exception {
    return importByType(importType.getIri(), config);
  }

  @Override
  public TTImportByType importByType(String importType, TTImportConfig config) throws Exception {
    LOG.info("Importing {}", importType);
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(config.getFolder());
      importer.importData(config);
      return this;
    }
  }


  @Override
  public TTImportByType validateByType(TTIriRef importType, String inFolder) throws Exception {
    return validateByType(importType.getIri(), inFolder);
  }

  @Override
  public TTImportByType validateByType(String importType, String inFolder) throws Exception {
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(inFolder);
      return this;
    }
  }


  private TTImport getImporter(String importType) throws ImportException {
    return switch (importType) {
      case IM.NAMESPACE + "SingleFileImporter" -> new SingleFileImporter();
      case GRAPH.QUERY -> new CoreQueryImporter();
      case GRAPH.BNF -> new BNFImporter();
      case GRAPH.DISCOVERY -> new CoreImporter();
      case GRAPH.BARTS_CERNER -> new BartsCernerImport();
      case SNOMED.NAMESPACE -> new SnomedImporter();
      case GRAPH.EMIS -> new EMISImport();
      case GRAPH.TPP -> new TPPImporter();
      case GRAPH.OPCS4 -> new OPCS4Importer();
      case GRAPH.ICD10 -> new ICD10Importer();
      case GRAPH.ENCOUNTERS -> new EncountersImporter();
      case GRAPH.VISION -> new VisionImport();
      case GRAPH.PRSB -> new PRSBImport();
      case GRAPH.KINGS_APEX -> new ApexKingsImport();
      case GRAPH.KINGS_WINPATH -> new WinPathKingsImport();
      case GRAPH.ODS -> new OdsImporter();
      case GRAPH.IM1 -> new IM1MapImport();
      case GRAPH.CEG -> new CEGImporter();
      case GRAPH.SMARTLIFE -> new SmartLifeImporter();
      case GRAPH.QOF -> new QOFQueryImport();
      case GRAPH.NHS_TFC -> new NHSTfcImport();
      case GRAPH.DELTAS -> new DeltaImporter();
      case QR.NAMESPACE -> new QImporter();
      case GRAPH.CPRD_MED -> new CPRDImport();
      case FHIR.GRAPH_FHIR -> new FHIRImporter();
      default -> throw new ImportException("Unrecognised import type [" + importType + "]");
    };
  }


}
