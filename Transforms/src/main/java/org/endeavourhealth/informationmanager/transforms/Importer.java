package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTImportByType;
import org.endeavourhealth.informationmanager.TTImportConfig;

/**
 * Manager Class which imports specialised data from a legacy classification or the core ontology using specialised importers
 */
public class Importer implements TTImportByType {


   /**
    * Creates a type specific importer and imports and files rthe data
    * @param importType The graph IRI for the particular source data type
    * @param config Import configuration
    * @return TTImport object for reuse
    * @throws Exception if one of the sources is invalid
    */
   @Override
   public TTImportByType importByType(TTIriRef importType, TTImportConfig config) throws Exception {
      System.out.println("Importing "+ importType.getIri());
      try (TTImport importer= getImporter(importType)){
         importer.validateFiles(config.folder);
         importer.validateLookUps(ImportUtils.getConnection());
         importer.importData(config);
      }
      return this;
   }

   @Override
   public TTImportByType validateByType(TTIriRef importType, String inFolder) throws Exception {
      try (TTImport importer= getImporter(importType)){
         importer.validateFiles(inFolder);
         importer.validateLookUps(ImportUtils.getConnection());
      }
      return this;
   }




   private TTImport getImporter(TTIriRef importType) throws Exception {
      if (IM.GRAPH_DISCOVERY.equals(importType))
         return new CoreImporter();
      else  if (IM.GRAPH_BARTS_CERNER.equals(importType))
         return new BartsCernerImport();
      else if (IM.GRAPH_SNOMED.equals(importType))
         return new SnomedImporter();
      else if (IM.GRAPH_EMIS.equals(importType))
         return new EMISImport();
      else if (IM.GRAPH_TPP.equals(importType))
         return new TPPImporter();
      else if (IM.GRAPH_OPCS4.equals(importType))
         return new OPCS4Importer();
      else if (IM.GRAPH_ICD10.equals(importType))
             return new ICD10Importer();
      else if (IM.MAP_DISCOVERY.equals(importType))
         return new DiscoveryMapsImporter();
      else if (IM.GRAPH_VISION.equals(importType))
         return new VisionImport();
      else if (IM.GRAPH_REPORTS.equals(importType))
          return new DiscoveryReportsImporter();
      else if (IM.GRAPH_PRSB.equals(importType))
          return new PRSBImport();
      else if (IM.GRAPH_KINGS_APEX.equals(importType))
         return new ApexKingsImport();
      else if (IM.GRAPH_KINGS_WINPATH.equals(importType))
         return new WinPathKingsImport();
      else if (IM.GRAPH_CEG16.equals(importType))
         return new EthnicityCEGImporter();
      else if (IM.GRAPH_IM1.equals(importType))
         return new IM1MapImport();
      else
         throw new Exception("Unrecognised import type");
   }


}
