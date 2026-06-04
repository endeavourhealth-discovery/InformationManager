package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.interfacemanager.model.*;
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
  public TTImportByType importByType(IMPORTTYPE importType, TTImportConfig config) throws Exception {
    LOG.info("Importing {}", importType);
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(config.getFolder());
      importer.importData(config);
      return this;
    }
  }

  @Override
  public TTImportByType validateByType(IMPORTTYPE importType, String inFolder) throws Exception {
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(inFolder);
      return this;
    }
  }


  private TTImport getImporter(IMPORTTYPE importType) throws ImportException {
    return switch (importType) {
      case IMPORTTYPE.SINGLE_FILE -> new SingleFileImporter();
      case IMPORTTYPE.QUERY -> new CoreQueryImporter();
      case IMPORTTYPE.BNF -> new BNFImporter();
      case IMPORTTYPE.CORE -> new CoreImporter();
      case IMPORTTYPE.BARTS_CERNER -> new BartsCernerImport();
      case IMPORTTYPE.SNOMED -> new SnomedImporter();
      case IMPORTTYPE.EMIS -> new EMISImport();
      case IMPORTTYPE.TPP -> new TPPImporter();
      case IMPORTTYPE.OPCS4 -> new OPCS4Importer();
      case IMPORTTYPE.ICD10 -> new ICD10Importer();
      case IMPORTTYPE.ENCOUNTERS -> new EncountersImporter();
      case IMPORTTYPE.VISION -> new VisionImport();
      case IMPORTTYPE.PRSB -> new PRSBImport();
      case IMPORTTYPE.KINGS_APEX -> new ApexKingsImport();
      case IMPORTTYPE.KINGS_WINPATH -> new WinPathKingsImport();
      case IMPORTTYPE.ODS -> new OdsImporter();
      case IMPORTTYPE.IM1 -> new IM1MapImport();
      case IMPORTTYPE.CEG -> new CEGImporter();
      case IMPORTTYPE.SMARTLIFE -> new SmartLifeImporter();
      case IMPORTTYPE.QOF -> new QOFQueryImport();
      case IMPORTTYPE.NHS_TFC -> new NHSTfcImport();
      case IMPORTTYPE.DELTAS -> new DeltaImporter();
      case IMPORTTYPE.QR -> new QImporter();
      case IMPORTTYPE.CPRD_MED -> new CPRDImport();
      case IMPORTTYPE.FHIR -> new FHIRImporter();
      case IMPORTTYPE.SMARTLIFEINDICATOR -> new SmartLifeIndicatorImporter();
      default -> throw new ImportException("Unrecognised import type [" + importType + "]");
    };
  }


}
