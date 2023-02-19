package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.From;
import org.endeavourhealth.imapi.model.imq.Operator;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;
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
        allowableSubTypes(document);
        addTemplates(document);
        output(document);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
    }

    private void addTemplates(TTDocument document) {
       document
         .addEntity(addTemplate(IM.NAMESPACE+"QT_RegisteredGMS",IM.NAMESPACE+"Q_RegisteredGMS","from",1000));
    }

    private TTEntity addTemplate(String iri,String value, String path, int weighting) {
        TTEntity template= new TTEntity()
          .setIri(iri)
          .set(IM.WEIGHTING,TTLiteral.literal(weighting))
          .set(IM.PATH_TO,TTLiteral.literal(path));
        return template;
    }


    private static void allowableSubTypes(TTDocument document) throws IOException {
        TTEntity allowables= new TTEntity()
              .setIri(IM.NAMESPACE+"AllowableChildTypes")
              .addType(IM.QUERY)
              .setName("For a parent type, allowable child entity types and their predicates connecting to their parent");
            allowables.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE+"IMEditorQueries"));
            document.addEntity(allowables);

        Query query= new Query();
        query.setName("Allowable child types for editor");
        query
          .from(f->f
            .where(w1->w1.setIri(IM.IS_CONTAINED_IN.getIri())
              .addIn(IM.NAMESPACE+"EntityTypes")))
          .select(s->s
            .setIri(RDFS.LABEL.getIri()))
          .select(s->s
            .setIri(SHACL.PROPERTY.getIri())
            .where(w1->w1
              .setBool(Bool.and)
              .where(a2->a2
                .setIri(SHACL.NODE.getIri())
                .addIn(new From().setVariable("this")))
              .where(a2->a2
                .setIri(SHACL.PATH.getIri())
                .setIn(List.of(From.iri(IM.IS_CONTAINED_IN.getIri())
                  ,From.iri(RDFS.SUBCLASSOF.getIri()),From.iri(IM.IS_SUBSET_OF.getIri())))))
            .select(s1->s1
              .setIri(SHACL.PATH.getIri())));
        allowables.set(IM.DEFINITION, TTLiteral.literal(query));

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
        qry.set(IM.WEIGHTING,TTLiteral.literal(10000));
        prof.from(f -> f
            .setType(IM.NAMESPACE+"Patient")
            .setName("Patient")
          .where(p->p
            .setId("gpRegistration")
            .where(p1->p1
                .setId("patientType")
                    .addIn(new From().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .where(pv->pv
              .setId("effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo("$referenceDate"))
            .where(pv -> pv
              .setBool(Bool.or)
              .where(pv1->pv1
                .setNotExist(true)
                .setId("endDate"))
              .where(pv1->pv1
                    .setId("endDate")
                      .setOperator(Operator.gt)
                      .setRelativeTo("$referenceDate")))));

        qry.set(IM.DEFINITION, TTLiteral.literal(prof));
        qry.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);
        document.setContext(TTUtil.getDefaultContext());
        setProvenance(qry, document);
        outputQuery(prof);

    }

    private void setProvenance(TTEntity rdf, TTDocument document) {
        ProvActivity activity = new ProvActivity()
            .setIri(IM.NAMESPACE + "PROV_Q-RegisteredGMS")
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
            try (FileWriter writer = new FileWriter(directory + "\\CoreQueries.json")) {
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
