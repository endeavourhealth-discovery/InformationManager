package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
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
        LOG.info("Importing {}", importType.getIri());
        try (TTImport importer = getImporter(importType)) {
            importer.validateFiles(config.getFolder());
            importer.importData(config);
            return this;
        }
    }

    @Override
    public TTImportByType validateByType(TTIriRef importType, String inFolder) throws Exception {
        try (TTImport importer = getImporter(importType)) {
            importer.validateFiles(inFolder);
            return this;
        }
    }


    private TTImport getImporter(TTIriRef importType) throws Exception {
        if (TTIriRef.iri(IM.NAMESPACE + "SingleFileImporter").equals(importType))
            return new SingleFileImporter();
        if (IM.GRAPH_QUERY.equals(importType))
            return new CoreQueryImporter();
        else if (IM.GRAPH_DISCOVERY.equals(importType))
            return new CoreImporter();
        else if (IM.GRAPH_BARTS_CERNER.equals(importType))
            return new BartsCernerImport();
        else if (SNOMED.GRAPH_SNOMED.equals(importType))
            return new SnomedImporter();
        else if (IM.GRAPH_EMIS.equals(importType))
            return new EMISImport();
        else if (IM.GRAPH_TPP.equals(importType))
            return new TPPImporter();
        else if (IM.GRAPH_OPCS4.equals(importType))
            return new OPCS4Importer();
        else if (IM.GRAPH_ICD10.equals(importType))
            return new ICD10Importer();
        else if (IM.GRAPH_ENCOUNTERS.equals(importType))
            return new EncountersImporter();
        else if (IM.GRAPH_VISION.equals(importType))
            return new VisionImport();
        else if (IM.GRAPH_PRSB.equals(importType))
            return new PRSBImport();
        else if (IM.GRAPH_KINGS_APEX.equals(importType))
            return new ApexKingsImport();
        else if (IM.GRAPH_KINGS_WINPATH.equals(importType))
            return new WinPathKingsImport();
        else if (IM.GRAPH_ODS.equals(importType))
            return new OdsImporter();
        else if (IM.GRAPH_IM1.equals(importType))
            return new IM1MapImport();
        else if (IM.GRAPH_CEG_QUERY.equals(importType))
            return new CEGImporter();
        else if (IM.GRAPH_NHS_TFC.equals(importType))
            return new NHSTfcImport();
        else if (IM.GRAPH_DELTAS.equals(importType))
            return new DeltaImporter();
        else if (TTIriRef.iri(QR.NAMESPACE).equals(importType))
            return new QImporter();
        else if (IM.GRAPH_CPRD_MED.equals(importType))
            return new CPRDImport();
        else
            throw new Exception("Unrecognised import type [" + importType.getIri() + "]");
    }


}
