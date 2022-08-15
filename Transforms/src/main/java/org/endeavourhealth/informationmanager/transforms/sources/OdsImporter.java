package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;
import static org.endeavourhealth.imapi.model.tripletree.TTLiteral.literal;

public class OdsImporter implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(OdsImporter.class);
    private static final String[] organisationFiles = {
        ".*\\\\TRUD\\\\ODS\\\\.*\\\\Organisation_Details.csv"
    };

    private List<String> fieldIndex;
    private String[] fieldData;

    public OdsImporter validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder, organisationFiles);
        return this;
    }


    /**
     * Imports the core ontology document
     *
     * @param config import config
     * @return TTImport object builder pattern
     * @throws Exception invalid document
     */
    @Override
    public TTImport importData(TTImportConfig config) throws Exception {
        LOG.info("Importing Organisation data");


        boolean graphCreated = false;
        for (String orgFile : organisationFiles) {
            TTManager manager = new TTManager();
            TTDocument doc = manager.createDocument();
            doc.setCrud(IM.UPDATE_ALL);
            if (!graphCreated) {
                doc.addEntity(manager.createGraph(IM.GRAPH_ODS.getIri(), "ODS Organisational code scheme and graph", "Official ODS code scheme and graph"));
                graphCreated = true;
            }

            Path file = ImportUtils.findFileForId(config.getFolder(), orgFile);

            LOG.info("Processing organisations in {}", file.getFileName());
            int i = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
                readLine(reader);
                processHeaders();

                while (readLine(reader)) {
                    processLine(doc);
                    i++;
                    if (i % 25000 == 0)
                        LOG.info("Processed {} lines", i);

                }
            }

            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(doc);
            }
        }
        return this;
    }

    private boolean readLine(BufferedReader reader) throws IOException {
        String line = reader.readLine();

        if (line == null || line.isEmpty())
            return false;

        line = line.substring(1, line.length() - 1);
        this.fieldData = line.split("\",\"");

        return true;
    }

    private void processHeaders() {
        this.fieldIndex = Arrays.asList(this.fieldData);
    }

    private void processLine(TTDocument doc) {
        String odsCode = fieldByName("OrganisationId");
        String orgIri = IM.ORGANISATION_NAMESPACE + odsCode;
        String addIri = IM.LOCATION_NAMESPACE + odsCode;

        TTEntity org = new TTEntity(orgIri);
        org.setName(fieldByName("Name"));
        org.set(IM.CODE, literal(odsCode));
        org.set(IM.ADDRESS, iri(addIri));
        doc.addEntity(org);

        TTEntity add = new TTEntity(addIri);
        add.addType(IM.ADDRESS_CLASS);
        add.set(IM.ADDRESS_LINE_1, literal(fieldByName("AddrLn1")));
        add.set(IM.ADDRESS_LINE_2, literal(fieldByName("AddrLn2")));
        add.set(IM.ADDRESS_LINE_3, literal(fieldByName("AddrLn3")));
        add.set(IM.LOCALITY, literal(fieldByName("Town")));
        add.set(IM.REGION, literal(fieldByName("County")));
        add.set(IM.POST_CODE, literal(fieldByName("PostCode")));
        add.set(IM.COUNTRY, literal(fieldByName("Country")));
        String uprn = fieldByName("UPRN");
        if (uprn != null && !uprn.isEmpty())
            add.set(IM.UPRN, literal(fieldByName("UPRN")));

        doc.addEntity(add);
    }

    private String fieldByName(String name) {
        int i = this.fieldIndex.indexOf(name);

        if (i >= this.fieldData.length)
            return null;

        return this.fieldData[i];
    }

}
