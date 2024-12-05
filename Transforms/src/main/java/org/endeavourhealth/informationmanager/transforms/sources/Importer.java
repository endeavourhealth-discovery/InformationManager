package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.imapi.vocabulary.*;
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
    if (importType.equals(IM.NAMESPACE + "SingleFileImporter"))
      return new SingleFileImporter();
    else if (GRAPH.QUERY.equals(importType))
      return new CoreQueryImporter();
    else if (GRAPH.BNF.equals(importType))
      return new BNFImporter();
    else if (GRAPH.DISCOVERY.equals(importType))
      return new CoreImporter();
    else if (GRAPH.BARTS_CERNER.equals(importType))
      return new BartsCernerImport();
    else if (SNOMED.NAMESPACE.equals(importType))
      return new SnomedImporter();
    else if (GRAPH.EMIS.equals(importType))
      return new EMISImport();
    else if (GRAPH.TPP.equals(importType))
      return new TPPImporter();
    else if (GRAPH.OPCS4.equals(importType))
      return new OPCS4Importer();
    else if (GRAPH.ICD10.equals(importType))
      return new ICD10Importer();
    else if (GRAPH.ENCOUNTERS.equals(importType))
      return new EncountersImporter();
    else if (GRAPH.VISION.equals(importType))
      return new VisionImport();
    else if (GRAPH.PRSB.equals(importType))
      return new PRSBImport();
    else if (GRAPH.KINGS_APEX.equals(importType))
      return new ApexKingsImport();
    else if (GRAPH.QOF.equals(importType))
      return new QOFImport();
    else if (GRAPH.KINGS_WINPATH.equals(importType))
      return new WinPathKingsImport();
    else if (GRAPH.ODS.equals(importType))
      return new OdsImporter();
    else if (GRAPH.IM1.equals(importType))
      return new IM1MapImport();
    else if (GRAPH.CEG.equals(importType))
      return new CEGImporter();
    else if (GRAPH.SMARTLIFE.equals(importType))
      return new SmartLifeImporter();
    else if (GRAPH.NHS_TFC.equals(importType))
      return new NHSTfcImport();
    else if (GRAPH.DELTAS.equals(importType))
      return new DeltaImporter();
    else if (QR.NAMESPACE.equals(importType))
      return new QImporter();
    else if (GRAPH.CPRD_MED.equals(importType))
      return new CPRDImport();
    else if (FHIR.GRAPH_FHIR.equals(importType))
      return new FHIRImporter();
    else
      throw new ImportException("Unrecognised import type [" + importType + "]");
  }


}
