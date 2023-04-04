package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDateTime;
import java.util.List;

public class CoreQueryImporter implements TTImport {
    public static String ex="http://example.org/qry#";

    @Override
    public void importData(TTImportConfig config) throws Exception {
        TTManager manager = new TTManager();
        TTDocument document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
        output(document,config.getFolder());
        addCurrentReg(document, config.getFolder());
        addTemplates(document);
        testQuery(document,config.getFolder());
        output(document,config.getFolder());
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
    }

    private void testQuery(TTDocument document, String outFolder) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

        TTEntity qry = new TTEntity().addType(IM.QUERY);
        qry
          .setIri(IM.NAMESPACE + "Q_TestQuery")
          .setName("Test for patients either aged between 18 and 65 or with diabetes with the most recent systolic in the last 6 months >150"+
            "not followed by a screening invite, excluding hypertensives");
        Query prof = new Query()
          .setIri(IM.NAMESPACE + "Q_TestQuery")
          .setName("Test for patients either aged between 18 and 65 or with diabetes with the most recent systolic in the last 6 months >150"+
            "not followed by a screening invite, excluding hypertensives")
          .match(f->f
            .setType("Patient"))
          .match(w->w
            .setDescription("Registered for gms")
            .setSet(IM.NAMESPACE+"Q_RegisteredGMS")
            .setName("Registered for GMS services on reference date"))
          .match(m->m
            .setBoolMatch(Bool.or)
            .match(or->or
              .setDescription("aged between 65 and 70")
              .where(w->w
                .setId("age")
                .range(r->r
                  .from(from->from
                    .setOperator(Operator.gte)
                    .setValue("65"))
                  .to(to->to
                    .setOperator(Operator.lt)
                    .setValue("70")))))
            .match(or->or
              .setDescription("Diabetic")
              .setSet(ex+"Q_Diabetics"))
            .match(or->or
              .path(p->p.setId("observation"))
              .path(p->p.setType("Observation"))
              .where(ob->ob
                .setId("concept")
                .addIn(new Element().setId(SNOMED.NAMESPACE+"714628002").setDescendantsOf(true)))))
          .match(w->w
            .path(p->p.setId("observation"))
            .path(p->p.setType("Observation")
              .setVariable("latestBP"))
            .setBool(Bool.and)
            .where(ww->ww
              .setDescription("Home or office based Systolic")
              .setId("concept")
              .setName("concept")
              .addIn(new Element()
                .setId(SNOMED.NAMESPACE+"271649006")
                .setName("Systolic blood pressure"))
              .addIn(new Element()
                .setId(IM.CODE_SCHEME_EMIS.getIri()+"1994021000006104")
                .setName("Home systolic blood pressure"))
              .setValueLabel("Office or home systolic blood pressure"))
            .where(ww->ww
              .setDescription("Last 6 months")
              .setId("effectiveDate")
              .setOperator(Operator.gte)
              .setValue("-6")
              .setUnit("MONTHS")
              .relativeTo(r->r.setId("$referenceDate"))
              .setValueLabel("last 6 months"))
            .addOrderBy(new OrderLimit()
              .setId("effectiveDate")
              .setNode("latestBP")
              .setLimit(1)
              .setDirection(Order.descending)))
          .match(m->m
            .where(w->w
              .setNode("latestBP")
              .setId(IM.NAMESPACE+"numericValue")
              .setDescription(">150")
              .setOperator(Operator.gt)
              .setValue("150")))
          .match(w->w
            .setExclude(true)
            .setDescription("High BP not followed by screening invite")
            .path(p->p.setId(IM.NAMESPACE+"observation"))
            .path(p->p.setType("Observation"))
            .setBool(Bool.and)
            .where(inv->inv
              .setDescription("Invited for Screening after BP")
              .setId(IM.NAMESPACE+"concept")
              .addIn(new Element().setSet(IM.NAMESPACE+"InvitedForScreening")))
            .where(after->after
              .setDescription("after high BP")
              .setId(IM.NAMESPACE+"effectiveDate")
              .setOperator(Operator.gte)
              .relativeTo(r->r.setNode("latestBP").setId("effectiveDate"))))
          .match(w->w
            .setExclude(true)
            .setDescription("not hypertensive")
            .setSet(IM.NAMESPACE+"Q_Hypertensives")
            .setName("Hypertensives"));
        qry.set(IM.DEFINITION, TTLiteral.literal(prof));
        qry.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);
        document.setContext(TTUtil.getDefaultContext());
        outputQuery(prof);
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
        prof.match(f -> f
             .setType(IM.NAMESPACE+"Patient")
             .setName("Patient"))
           .match(m->m
             .path(p->p
               .setId("gpRegistration"))
            .setBool(Bool.and)
            .where(p1->p1
                .setIri("patientType")
                    .addIn(new Element().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .where(pv->pv
              .setIri("effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo(new Property().setParameter("$referenceDate")))
            .where(pv -> pv
              .setBool(Bool.or)
              .where(pv1->pv1
                .setIri("endDate")
                .setNull(true))
              .where(pv1->pv1
                    .setIri("endDate")
                      .setOperator(Operator.gt)
                      .setRelativeTo(new Property().setParameter("$referenceDate")))));

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


    private void output(TTDocument document,String folder) throws IOException {
        if (ImportApp.testDirectory != null) {
            try (FileWriter writer = new FileWriter(folder + "\\DiscoveryCore\\CoreQueries.json")) {
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
            try (FileWriter writer = new FileWriter(directory + "\\"+ qry.getName().substring(0,20)+"+.json")) {
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
