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
  public TTImportByType importByType(ImportType importType, TTImportConfig config) throws Exception {
    LOG.info("Importing {}", importType);
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(config.getFolder());
      importer.importData(config);
      return this;
    }
  }

  @Override
  public TTImportByType validateByType(ImportType importType, String inFolder) throws Exception {
    try (TTImport importer = getImporter(importType)) {
      importer.validateFiles(inFolder);
      return this;
    }
  }


  private TTImport getImporter(ImportType importType) throws ImportException {
    return switch (importType) {
      case ImportType.SINGLE_FILE -> new SingleFileImporter();
      case ImportType.QUERY -> new CoreQueryImporter();
      case ImportType.BNF -> new BNFImporter();
      case ImportType.CORE -> new CoreImporter();
      case ImportType.BARTS_CERNER -> new BartsCernerImport();
      case ImportType.SNOMED -> new SnomedImporter();
      case ImportType.EMIS -> new EMISImport();
      case ImportType.TPP -> new TPPImporter();
      case ImportType.OPCS4 -> new OPCS4Importer();
      case ImportType.ICD10 -> new ICD10Importer();
      case ImportType.ENCOUNTERS -> new EncountersImporter();
      case ImportType.VISION -> new VisionImport();
      case ImportType.PRSB -> new PRSBImport();
      case ImportType.KINGS_APEX -> new ApexKingsImport();
      case ImportType.KINGS_WINPATH -> new WinPathKingsImport();
      case ImportType.ODS -> new OdsImporter();
      case ImportType.IM1 -> new IM1MapImport();
      case ImportType.CEG -> new CEGImporter();
      case ImportType.SMARTLIFE -> new SmartLifeImporter();
      case ImportType.QOF -> new QOFQueryImport();
      case ImportType.NHS_TFC -> new NHSTfcImport();
      case ImportType.DELTAS -> new DeltaImporter();
      case ImportType.QR -> new QImporter();
      case ImportType.CPRD_MED -> new CPRDImport();
      case ImportType.FHIR -> new FHIRImporter();
      default -> throw new ImportException("Unrecognised import type [" + importType + "]");
    };
  }


}
