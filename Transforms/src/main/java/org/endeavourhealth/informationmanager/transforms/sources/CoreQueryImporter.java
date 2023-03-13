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
        allowableSubTypes(document);
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
        Query prof = new Query();
        prof.setIri(qry.getIri());
        prof.setName(qry.getName());
        prof.from(f->f
            .setIri(IM.NAMESPACE+"Patient")
          .where(w->w
            .setDescription("Registered for gms")
            .setIri(IM.IS_SUBSET_OF.getIri())
            .in(t->t.setSet(ex+"Q_RegisteredGMS")
              .setName("Registered for GMS services on reference date"))
            .setValueLabel("Registered for GMS on reference date"))
          .where(w->w
              .setDescription("aged 65 to 70 or diabetic")
                .setBool(Bool.or)
                .where (or->or
                    .setDescription("aged between 65 and 70")
                   .setIri(IM.NAMESPACE+"age")
                    .range(r->r
                      .from(from->from
                        .setOperator(Operator.gte)
                        .setValue("65"))
                    .to(to->to
                        .setOperator(Operator.lt)
                        .setValue("70"))))
                .where(or->or
                  .setDescription("Diabetic")
                  .setIri(IM.NAMESPACE+"observation")
                  .where(ob->ob
                    .setIri(IM.NAMESPACE+"concept")
                    .addIn(new TTAlias().setSet(ex+"Q_Diabetics"))
                    .addIn(new TTAlias().setIri(SNOMED.NAMESPACE+"714628002").setDescendantsOf(true)))))
          .where(w->w
                  .setDescription("latest BP in last 6 months is >150")
                  .setIri(IM.NAMESPACE+"observation")
                  .with(ob->ob
                    .setDescription("Home or office based systolic in the last 6 months is >150")
                    .setBool(Bool.and)
                      .where(ww->ww
                        .setDescription("Home or office based Systolic")
                        .setIri(IM.NAMESPACE+"concept")
                        .setName("concept")
                        .addIn(new TTAlias()
                          .setIri(SNOMED.NAMESPACE+"271649006")
                          .setName("Systolic blood pressure"))
                        .addIn(new TTAlias()
                          .setIri(IM.CODE_SCHEME_EMIS.getIri()+"1994021000006104")
                          .setName("Home systolic blood pressure"))
                        .setValueLabel("Office or home systolic blood pressure"))
                      .where(ww->ww
                        .setDescription("Last 6 months")
                        .setIri(IM.NAMESPACE+"effectiveDate")
                        .setAlias("LastBP")
                        .setOperator(Operator.gte)
                        .setValue("-6")
                        .setUnit("MONTHS")
                        .setRelativeTo("$referenceDate")
                        .setValueLabel("last 6 months"))
                      .setOrderBy(new TTAlias().setIri(IM.NAMESPACE+"effectiveDate"))
                      .setCount(1)
                    .then(ww->ww
                      .setIri(IM.NAMESPACE+"numericValue")
                      .setDescription(">150")
                      .setOperator(Operator.gt)
                      .setValue("150"))))
            .where(w->w
                  .setBool(Bool.not)
                  .setDescription("not followed by screening invite or is hypertensive")
                  .where(not->not
                    .setDescription("Invited for screening after high BP")
                    .setIri(IM.NAMESPACE+"observation")
                    .where(ob->ob
                      .setDescription("Invited for Screening after high BP")
                      .setBool(Bool.and)
                      .where(inv->inv
                        .setIri(IM.NAMESPACE+"concept")
                        .addIn(new TTAlias().setSet(IM.NAMESPACE+"InvitedForScreening")))
                      .where(after->after
                        .setIri(IM.NAMESPACE+"effectiveDate")
                        .setOperator(Operator.gte)
                        .setRelativeTo("LastBP")))))
          .where(w->w
            .setBool(Bool.not)
                  .where(not->not
                    .setDescription("Hypertensive")
                    .setIri(IM.NAMESPACE+"observation")
                    .where(ob1->ob1
                      .setIri(IM.NAMESPACE+"concept")
                    .addIn(new TTAlias().setSet(ex+"Hypertensives")
                      .setName("Hypertensives"))))));
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
            .setIri(IM.NAMESPACE+"gpRegistration")
            .where(p1->p1
                .setIri(IM.NAMESPACE+"patientType")
                    .addIn(new From().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .where(pv->pv
              .setIri(IM.NAMESPACE+"effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo("$referenceDate"))
            .where(pv -> pv
              .setBool(Bool.or)
              .where(pv1->pv1
                .setBool(Bool.not)
                .where(pv2->pv2
                .setIri(IM.NAMESPACE+"endDate")))
              .where(pv1->pv1
                    .setIri(IM.NAMESPACE+"endDate")
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


    private void output(TTDocument document,String folder) throws IOException {
        if (ImportApp.testDirectory != null) {
            try (FileWriter writer = new FileWriter(folder + "\\CoreQueries.json")) {
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
