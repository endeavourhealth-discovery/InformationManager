package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.iml.*;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.TTToClassObject;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;


import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.List;

public class CoreQueryImporter implements TTImport {

    @Override
    public void importData(TTImportConfig config) throws Exception {
        TTManager manager = new TTManager();
        TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
        output(document);

        addCurrentReg(document, config.getFolder());
        output(document);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
    }

    private void addCurrentReg(TTDocument document, String outFolder) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        TTEntity qry = new TTEntity().addType(IM.QUERY);
        qry
            .setIri(IM.NAMESPACE + "Q_RegisteredGMS")
            .setName("Patients registered for GMS services on the reference date")
            .setDescription("For any registration period,a registration start date before the reference date and no end date," +
                "or an end date after the reference date.");
        Query prof = new Query();
        prof.setIri(qry.getIri());
        prof.setName(qry.getName());
        prof.setDescription(qry.getDescription());
        prof.from(f -> f
                .setIri(IM.NAMESPACE + "Patient").setName("Patient").setIsType(true))
            .setWhere(new Where()
                .setPathTo(IM.NAMESPACE + "gpRegistration")
                .and(pv -> pv
                    .setProperty(IM.NAMESPACE + "patientType")
                    .setIs(new TTAlias().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
                .and(pv -> pv
                    .setProperty(IM.NAMESPACE + "startDate")
                    .setValue(new Value()
                        .setComparison("<=")
                        .relativeTo(c -> c
                            .setVariable("$ReferenceDate"))))
                .or(pv -> pv
                    .notExist(not -> not
                        .setProperty(IM.NAMESPACE + "endDate")))
                .or(pv -> pv
                    .setProperty(IM.NAMESPACE + "endDate")
                    .setValue(new Value()
                        .setComparison(">")
                        .relativeTo(c -> c
                            .setVariable("$referenceDate")))
                ));

        qry.set(IM.DEFINITION, TTLiteral.literal(prof));
        qry.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);
        document.setContext(TTUtil.getDefaultContext());
        setProvenance(qry, document);
        outputQuery(prof);
    }

    private void setProvenance(TTEntity rdf, TTDocument document) {
        ProvActivity activity = new ProvActivity()
            .setIri(IM.NAMESPACE + "Q_RegisteredGMS")
            .setActivityType(IM.PROV_CREATION)
            .setEffectiveDate(LocalDateTime.now().toString())
            .setTargetEntity(TTIriRef.iri(rdf.getIri()));
        document.addEntity(activity);
    }


    @Override
    public void validateFiles(String inFolder) throws TTFilerException {
    }


    private void output(TTDocument document) throws IOException {
        if (ImportApp.testDirectory != null) {
            String directory = ImportApp.testDirectory.replace("%", " ");
            try (FileWriter writer = new FileWriter(directory + "\\Core-qry-LD.json")) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
                String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
                writer.write(doc);
            }
        }

    }


    private void outputQuery(Query qry) throws IOException {
        if (ImportApp.testDirectory != null) {
            String directory = ImportApp.testDirectory.replace("%", " ");
            try (FileWriter writer = new FileWriter(directory + "\\Core-qry.json")) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
                String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
                writer.write(doc);
            }
        }

    }

    @Override
    public void close() throws Exception {

    }
}
