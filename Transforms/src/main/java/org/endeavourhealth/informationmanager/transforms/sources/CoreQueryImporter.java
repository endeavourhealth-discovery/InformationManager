package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public class CoreQueryImporter implements TTImport {
    public static String ex="http://example.org/qry#";
    public TTDocument document;

    @Override
    public void importData(TTImportConfig config) throws Exception {
        TTManager manager = new TTManager();
        document = manager.createDocument(IM.GRAPH_DISCOVERY.getIri());
        getIsas();
        getConcepts();
        getAllowableProperties();
        getAllowableRanges();
        getSearchAll();
        allowableSubTypes();
        currentGMS();
        gpGMSRegisteredPractice();
        deleteSets();
        testQuery();
        objectPropertyRangeSuggestions();
        dataPropertyRangeSuggestions();
        dataModelPropertyRange();
        output(document,config.getFolder());
        if (!TTFilerFactory.isBulk()) {
            TTTransactionFiler filer= new TTTransactionFiler(null);
            filer.fileTransaction(document);
        }
        else {
            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(document);
            }
        }
    }

    private void objectPropertyRangeSuggestions() throws JsonProcessingException {
        TTEntity query= getQuery("ObjectPropertyRangeSuggestions","Range suggestions for object property","takes account of the data model shape that the property is part of");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
                .setName("Suggested range for a property")
                .setDescription("get node, class or datatype values (ranges)  of property objects that have 4this as their path")
                .match(m->m
                  .setBoolMatch(Bool.or)
                  .match(m1->m1
                    .path(p->p
                      .setIri(SHACL.NODE.getIri())
                      .node(n->n
                        .setVariable("range"))))
                  .match(m1->m1
                    .path(p->p
                      .setIri(SHACL.CLASS.getIri())
                      .node(n->n
                        .setVariable("range"))))
                  .match(m1->m1
                    .path(p->p
                      .setIri(SHACL.DATATYPE.getIri())
                      .node(n->n
                        .setVariable("range"))))
                  .where(w->w
                    .setIri(SHACL.PATH.getIri())
                    .addIn(new Node().setParameter("this"))))
                .return_(r->r.setNodeRef("range").property(p->p.setIri(RDFS.LABEL.getIri())))));
        document.addEntity(query);
    }

    private void dataModelPropertyRange() throws JsonProcessingException {
        TTEntity query= getQuery("DataModelPropertyRange","Data model property range","takes account of the data model shape that the property is part of");
        query.set(IM.DEFINITION,TTLiteral.literal(
                new Query()
                        .setName("Data model property range")
                        .setDescription("get node, class or datatype value (range)  of property objects for specific data model and property")
                        .match(m->m
                                .setParameter("myDataModel")
                                .path(p->p.setIri(SHACL.PROPERTY.getIri()).node(n->n.setVariable("shaclProperty")))
                                .where(w->w.setIri(SHACL.PATH.getIri()).addIn(new Node().setParameter("myProperty"))))
                        .return_(r->r.
                                setNodeRef("shaclProperty")
                                .setProperty(List.of(
                                        new ReturnProperty()
                                        .setIri(SHACL.CLASS.getIri())
                                        .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                        .setIri(SHACL.NODE.getIri())
                                        .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                        .setIri(SHACL.DATATYPE.getIri())
                                        .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                .setIri(RDFS.LABEL.getIri()))))))
                        )));
        document.addEntity(query);
    }
    private void dataPropertyRangeSuggestions() throws JsonProcessingException {
        TTEntity query= getQuery("dataPropertyRangeSuggestions","Range suggestions for object property","takes account of the data model shape that the property is part of");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setName("Suggested range for a data property")
            .setDescription("get datatype values (ranges)  of property objects that have 4this as their path")
            .match(m->m
                .path(p->p
                  .setIri(SHACL.DATATYPE.getIri())
                  .node(n->n
                    .setVariable("range")))
            )
            .return_(s->s.setNodeRef("range").property(p->p.setIri(RDFS.LABEL.getIri())))));
        document.addEntity(query);
    }

    private void testQuery() throws IOException{

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
                .setIri("age")
                .range(r->r
                  .from(from->from
                    .setOperator(Operator.gte)
                    .setValue("65"))
                  .to(to->to
                    .setOperator(Operator.lt)
                    .setValue("70")))))
            .match(or->or
              .setDescription("Diabetic")
              .setSet("http://example/queries#Q_Diabetics"))
            .match(or->or
              .path(p->p.setIri("observation")
                .node(n->n.setType("Observation")))
              .where(ob->ob
                .setIri("concept")
                .addIn(new Node().setIri(SNOMED.NAMESPACE+"714628002").setDescendantsOf(true)))))
          .match(w->w
            .path(p->p.setIri("observation")
              .node(n->n.setType("Observation")
                .setVariable("latestBP")))
            .setBool(Bool.and)
            .where(ww->ww
              .setDescription("Home or office based Systolic")
              .setIri("concept")
              .setName("concept")
              .addIn(new Node()
                .setIri(SNOMED.NAMESPACE+"271649006")
                .setName("Systolic blood pressure"))
              .addIn(new Node()
                .setIri(IM.CODE_SCHEME_EMIS.getIri()+"1994021000006104")
                .setName("Home systolic blood pressure"))
              .setValueLabel("Office or home systolic blood pressure"))
            .where(ww->ww
              .setDescription("Last 6 months")
              .setIri("effectiveDate")
              .setOperator(Operator.gte)
              .setValue("-6")
              .setUnit("MONTHS")
              .relativeTo(r->r.setIri("$referenceDate"))
              .setValueLabel("last 6 months"))
            .addOrderBy(new OrderLimit()
              .setIri("effectiveDate")
              .setVariable("latestBP")
              .setLimit(1)
              .setDirection(Order.descending)))
          .match(m->m
            .where(w->w
              .setVariable("latestBP")
              .setIri(IM.NAMESPACE+"numericValue")
              .setDescription(">150")
              .setOperator(Operator.gt)
              .setValue("150")))
          .match(w->w
            .setExclude(true)
            .setDescription("High BP not followed by screening invite")
            .path(p->p.setIri(IM.NAMESPACE+"observation")
              .node(n->n.setType("Observation")))
            .setBool(Bool.and)
            .where(inv->inv
              .setDescription("Invited for Screening after BP")
              .setIri(IM.NAMESPACE+"concept")
              .addIn(new Node().setSet(IM.NAMESPACE+"InvitedForScreening")))
            .where(after->after
              .setDescription("after high BP")
              .setIri(IM.NAMESPACE+"effectiveDate")
              .setOperator(Operator.gte)
              .relativeTo(r->r.setVariable("latestBP").setIri("effectiveDate"))))
          .match(w->w
            .setExclude(true)
            .setDescription("not hypertensive")
            .setSet(IM.NAMESPACE+"Q_Hypertensives")
            .setName("Hypertensives"));
        qry.set(IM.DEFINITION, TTLiteral.literal(prof));
        qry.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);

    }


    private void deleteSets() throws JsonProcessingException {
        TTEntity entity= new TTEntity()
          .setIri(IM.NAMESPACE+"DeleteSets")
          .setName("Delete all concept sets in a graph")
          .setDescription("Pass in the graph name as a 'this' argument and it deletes all sets");
        entity.set(IM.UPDATE_PROCEDURE,TTLiteral.literal(new Update()
          .match(m->m
            .setGraph(new Node().setParameter("this"))
            .setType(IM.CONCEPT_SET.getIri()))
          .addDelete(new Delete())));
        document.addEntity(entity);


    }


    private void currentGMS() throws JsonProcessingException {

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
              .setIri("gpRegistration"))
            .setBool(Bool.and)
            .where(p1->p1
              .setIri("patientType")
              .addIn(new Node().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
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

    }



    private void getSearchAll() throws JsonProcessingException {
        TTEntity query= getQuery("SearchmainTypes","Search for entities of the main types","used to filter free text searches excluding queries and concept sets");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setActiveOnly(true)
            .setName("Search for all main types")
            .match(f->f
              .setVariable("type")
              .setBoolMatch(Bool.or)
              .match(w->w
                .setType(IM.CONCEPT.getIri()))
              .match(w->w
                .setType(IM.CONCEPT_SET.getIri()))
              .match(w->w
                .setType(IM.FOLDER.getIri()))
              .match(w->w
                .setType(IM.VALUESET.getIri()))
              .match(w->w
                .setType(IM.NAMESPACE+"dataModelProperty")))
            .return_(s->s.setNodeRef("type")
              .property(p->p.setIri(RDFS.LABEL.getIri()))
            .property(p->p.setIri(RDFS.COMMENT.getIri()))
            .property(p->p.setIri(IM.HAS_STATUS.getIri()))
            .property(p->p.setIri(IM.WEIGHTING.getIri()))
            .property(p->p.setIri(IM.HAS_SCHEME.getIri()))
        )));
    }


    private void gpGMSRegisteredPractice() throws IOException {
        TTEntity entity= new TTEntity()
          .setIri(IM.NAMESPACE+"gpGMSRegisteredPractice")
          .setName("Current GMS registered practice")
          .setScheme(IM.CODE_SCHEME_DISCOVERY)
          .addType(IM.FUNCTION)
          .addType(RDF.PROPERTY);
        entity.addObject(RDFS.SUBCLASSOF,IM.FUNCTION_PROPERTY);

        Query query= new Query();
        query
          .setName("GMS registered practice on reference date")
          .return_(r->r
            .setNodeRef("practice")
            .property(s -> s
            .setIri(IM.NAMESPACE+"recordOwner")))
          .match(f->f
            .setParameter("this")
            .path(p->p
              .setIri("gpRegistration")
            .node(n->n
              .setIri("GPRegistration"))
              .setVariable("practice"))
            .setBool(Bool.and)
            .where(pv->pv
              .setIri("patientType")
              .addIn(new Node().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .where(pv->pv
              .setIri("effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo(new Property().setParameter("$referenceDate")))
            .where(pv->pv
              .setBool(Bool.or)
              .where(pv1->pv1
                .setNull(true)
                .setIri("endDate"))
              .where(pv1->pv1
                .setIri("endDate")
                .setOperator(Operator.gt)
                .setRelativeTo(new Property().setParameter("$referenceDate")))));
        entity
          .set(IM.DEFINITION,TTLiteral.literal(query));
        document.addEntity(entity);
    }




    private void allowableSubTypes() throws IOException {
        TTEntity entity= getQuery("AllowableChildTypes","for a parent entity, the types that can be child types","used in the editor to select the type of entity being created as a subtype");
        Query query= new Query();
        query.setName("Allowable child types for editor");
        query
          .match(f->f
            .setVariable("concept")
            .where(w1->w1.setIri(IM.IS_CONTAINED_IN.getIri())
              .addIn(IM.NAMESPACE+"EntityTypes")))
          .match(w1->w1
            .path(p->p
              .setIri(SHACL.PROPERTY.getIri())
            .node(n->n.setVariable("predicate")))
            .setBool(Bool.and)
            .where(a2->a2
              .setIri(SHACL.NODE.getIri())
              .addIn(new Match().setParameter("$this")))
            .where(a2->a2
              .setIri(SHACL.PATH.getIri())
              .setIn(List.of(Node.iri(IM.IS_CONTAINED_IN.getIri())
                , Match.iri(RDFS.SUBCLASSOF.getIri()), Match.iri(IM.IS_SUBSET_OF.getIri())))))
          .return_(s->s
            .setNodeRef("concept")
            .property(p->p
               .setIri(RDFS.LABEL.getIri()))
            .property(p->p
            .setIri(SHACL.PROPERTY.getIri())
            .node(s1->s1
              .setNodeRef("predicate")
              .property(p1->p1
                  .setIri(SHACL.PATH.getIri())))));
        entity.set(IM.DEFINITION, TTLiteral.literal(query));

    }
    private void getAllowableRanges() throws JsonProcessingException {
        TTEntity query= getQuery("AllowableRanges","Allowable ranges for a particular property or its ancestors","uses inverse range property to return the ranges of the property as authored. Should be used with another ");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setName("Allowable Ranges for a property and super properties")
            .setActiveOnly(true)
            .return_(r->r
            .property(s->s.setIri(IM.CODE.getIri()))
            .property(s->s.setIri(RDFS.LABEL.getIri())))
            .match(f ->f
              .where(w->w
                .setInverse(true)
                .setAncestorsOf(true)
                .setIri(RDFS.RANGE.getIri())
                .addIn(new Node().setParameter("this"))))));
        document.addEntity(query);
    }

    private void getAllowableProperties() throws JsonProcessingException {
        TTEntity query= getQuery("AllowableProperties","Allowable properties for a terminology concept","Returns a list of properties for a particular term concept, used in value set definitions with RCL");

        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setName("Allowable Properties for a terminology concept")
            .setActiveOnly(true)
            .return_(r->r
              .setNodeRef("concept")
              .property(p->p.setIri(IM.CODE.getIri()))
              .property(p->p.setIri(RDFS.LABEL.getIri())))
            .match(f ->f
              .setVariable("concept")
              .setType(IM.CONCEPT.getIri())
              .where(w->w
                .setDescription("property that has this concept or supertype as a domain")
                .setIri(RDFS.DOMAIN.getIri())
                .addIn(new Node().setParameter("this").setAncestorsOf(true))
              ))));
    }

    private void getConcepts() throws JsonProcessingException {
        TTEntity query= getQuery("SearchEntities","Search for entities of a certain type","parameter 'this' set to the list of type iris, Normally used with a text search entry to filter the list");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setActiveOnly(true)
            .setName("Search for concepts")
            .match(w->w
              .setParameter("this"))
            .return_(r->r
            .property(p->p.setIri(RDFS.LABEL.getIri()))
            .property(p->p.setIri(RDF.TYPE.getIri())))));
    }

    private void getIsas() throws JsonProcessingException {
        TTEntity query = getQuery("GetIsas","Get active subtypes of concept","returns transitive closure of an entity and its subtypes, usually used with a text search filter to narrow results");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE+"query"));
        query.set(IM.DEFINITION,
          TTLiteral.literal(new Query()
            .setName("All subtypes of an entity, active only")
            .setActiveOnly(true)
            .match(w->w
              .setVariable("isa")
              .setParameter("this")
              .setDescendantsOrSelfOf(true))
            .return_(s->s.setNodeRef("isa")
              .property(p->p.setIri(RDFS.LABEL.getIri()))
            .property(p->p.setIri(IM.CODE.getIri())))));
    }

    private TTEntity getQuery(String iri, String name, String comment) {
        TTEntity entity= new TTEntity()
          .setIri(IM.NAMESPACE+"Query_"+iri)
          .setName(name)
          .setDescription(comment)
          .addType(IM.QUERY);
        entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"IMFormValidationQueries"));
        document.addEntity(entity);
        return entity;
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


    private void output(TTDocument document,String directory) throws IOException {

            try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\CoreQueriesAll.json")) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
                String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
                writer.write(doc);
            }
            for (TTEntity entity:document.getEntities()){
                if (entity.get(IM.DEFINITION)!=null) {
                    Query query = entity.get(IM.DEFINITION).asLiteral().objectValue(Query.class);
                    outputQuery(query, directory);
                }
            }

    }


    private void outputQuery(Query qry,String directory) throws IOException {
        String name= qry.getName();
        if (name.length()>20)
            name= name.substring(0,20);
            try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\"+ name +"+.json")) {
                ObjectMapper objectMapper = new ObjectMapper();
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
                objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
                String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
                writer.write(doc);
            }


    }

    @Override
    public void close() throws Exception {

    }
}
