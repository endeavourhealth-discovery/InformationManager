package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportByType;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.Vocabulary;
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
    public TTImportByType importByType(Vocabulary importType, TTImportConfig config) throws Exception {
        LOG.info("Importing {}", importType.getIri());
        try (TTImport importer = getImporter(importType.asTTIriRef())) {
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
    @Override
    public TTImportByType validateByType(Vocabulary importType, String inFolder) throws Exception {
        try (TTImport importer = getImporter(importType.asTTIriRef())) {
            importer.validateFiles(inFolder);
            return this;
        }
    }


    private TTImport getImporter(TTIriRef importType) throws Exception {
        if (TTIriRef.iri(IM.NAMESPACE.iri + "SingleFileImporter").equals(importType))
            return new SingleFileImporter();
        if (IM.GRAPH_QUERY.iri.equals(importType.getIri()))
            return new CoreQueryImporter();
        else if (IM.GRAPH_DISCOVERY.iri.equals(importType.getIri()))
            return new CoreImporter();
        else if (IM.GRAPH_BARTS_CERNER.iri.equals(importType.getIri()))
            return new BartsCernerImport();
        else if (SNOMED.GRAPH_SNOMED.iri.equals(importType.getIri()))
            return new SnomedImporter();
        else if (IM.GRAPH_EMIS.iri.equals(importType.getIri()))
            return new EMISImport();
        else if (IM.GRAPH_TPP.iri.equals(importType.getIri()))
            return new TPPImporter();
        else if (IM.GRAPH_OPCS4.iri.equals(importType.getIri()))
            return new OPCS4Importer();
        else if (IM.GRAPH_ICD10.iri.equals(importType.getIri()))
            return new ICD10Importer();
        else if (IM.GRAPH_ENCOUNTERS.iri.equals(importType.getIri()))
            return new EncountersImporter();
        else if (IM.GRAPH_VISION.iri.equals(importType.getIri()))
            return new VisionImport();
        else if (IM.GRAPH_PRSB.iri.equals(importType.getIri()))
            return new PRSBImport();
        else if (IM.GRAPH_KINGS_APEX.iri.equals(importType.getIri()))
            return new ApexKingsImport();
        else if (IM.GRAPH_KINGS_WINPATH.iri.equals(importType.getIri()))
            return new WinPathKingsImport();
        else if (IM.GRAPH_ODS.iri.equals(importType.getIri()))
            return new OdsImporter();
        else if (IM.GRAPH_IM1.iri.equals(importType.getIri()))
            return new IM1MapImport();
        else if (IM.GRAPH_CEG_QUERY.iri.equals(importType.getIri()))
            return new CEGImporter();
        else if (IM.GRAPH_NHS_TFC.iri.equals(importType.getIri()))
            return new NHSTfcImport();
        else if (IM.GRAPH_DELTAS.iri.equals(importType.getIri()))
            return new DeltaImporter();
        else if (QR.NAMESPACE.iri.equals(importType.getIri()))
            return new QImporter();
        else if (IM.GRAPH_CPRD_MED.iri.equals(importType.getIri()))
            return new CPRDImport();
        else
            throw new Exception("Unrecognised import type [" + importType.getIri() + "]");
    }


}
