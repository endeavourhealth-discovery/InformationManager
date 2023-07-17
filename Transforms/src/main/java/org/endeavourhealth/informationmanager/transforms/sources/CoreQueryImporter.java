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
        searchProperties();
        dataModelPropertyRange();
        dataModelPropertyByShape();
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
                  .setBool(Bool.or)
                  .match(m1->m1
                    .property(p->p
                      .setIri(SHACL.NODE.getIri())
                      .match(n->n
                        .setVariable("range"))))
                  .match(m1->m1
                    .property(p->p
                      .setIri(SHACL.CLASS.getIri())
                      .match(n->n
                        .setVariable("range"))))
                  .match(m1->m1
                    .property(p->p
                      .setIri(SHACL.DATATYPE.getIri())
                      .match(n->n
                        .setVariable("range")))))
            .match(m->m
                  .property(w->w
                    .setIri(SHACL.PATH.getIri())
                    .addIn(new Match().setParameter("this"))))
                .return_(r->r.setNodeRef("range").property(p->p.setIri(RDFS.LABEL.getIri())))));
        document.addEntity(query);
    }

    private void dataModelPropertyByShape() throws JsonProcessingException {
        TTEntity query= getQuery("DataModelPropertyByShape","Data model property","takes account of the data model shape that the property is part of");
        query.set(IM.DEFINITION,TTLiteral.literal(
                new Query()
                        .setName("Data model property")
                        .setDescription("get properties of property objects for specific data model and property")
                        .match(m->m
                                .setParameter("myDataModel")
                                .property(p->p
                                  .setIri(SHACL.PROPERTY.getIri())
                                  .match(n->n.setVariable("shaclProperty")
                                    .property(w->w
                                      .setIri(SHACL.PATH.getIri())
                                      .addIn(new Node().setParameter("myProperty"))))))
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
                                                .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                                .setIri(SHACL.GROUP.getIri())
                                                .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                        .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                                .setIri(SHACL.FUNCTION.getIri())
                                                .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                        .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                                .setIri(SHACL.INVERSEPATH.getIri())
                                                .setNode(new Return().setProperty(List.of(new ReturnProperty()
                                                        .setIri(RDFS.LABEL.getIri())))),
                                        new ReturnProperty()
                                                .setIri(SHACL.ORDER.getIri()),
                                        new ReturnProperty()
                                                .setIri(SHACL.MAXCOUNT.getIri()),
                                        new ReturnProperty()
                                                .setIri(SHACL.MINCOUNT.getIri())
                                ))
                        )));
        document.addEntity(query);
    }

    private void dataModelPropertyRange() throws JsonProcessingException {
        TTEntity query= getQuery("DataModelPropertyRange","Data model property range","returns a flat list of data model property ranges based on input data model and property");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setName("Data model property range")
            .setDescription("get node, class or datatype value (range)  of property objects for specific data model and property")
            .match(m->m
              .setParameter("myDataModel")
              .property(p->p
                .setIri("http://www.w3.org/ns/shacl#property")
                .match(m1->m1
                  .setVariable("shaclProperty")
                  .setBool(Bool.and)
                  .property(p2->p2
                    .setIri(SHACL.PATH.getIri())
                    .in(in->in
                      .setParameter("myProperty")))
                  .property(p2->p2
                    .setBool(Bool.or)
                    .property(p3->p3
                      .setIri(SHACL.CLASS.getIri())
                      .match(m3->m3
                        .setVariable("propType")))
                    .property(p3->p3
                      .setIri(SHACL.NODE.getIri())
                      .match(m3->m3
                        .setVariable("propType")))
                    .property(p3->p3
                      .setIri(SHACL.DATATYPE.getIri())
                      .match(m3->m3
                        .setVariable("propType")))))))
            .return_(r->r
              .setNodeRef("propType")
              .property(p->p
                .setIri(RDFS.LABEL.getIri())))));
        document.addEntity(query);
    }

    private void dataPropertyRangeSuggestions() throws JsonProcessingException {
        TTEntity query= getQuery("dataPropertyRangeSuggestions","Range suggestions for object property","takes account of the data model shape that the property is part of");
        query.set(IM.DEFINITION,TTLiteral.literal(
          new Query()
            .setName("Suggested range for a data property")
            .setDescription("get datatype values (ranges)  of property objects that have 4this as their path")
            .match(m->m
                .property(p->p
                  .setIri(SHACL.DATATYPE.getIri())
                  .match(n->n
                    .setVariable("range")))
            )
            .return_(s->s.setNodeRef("range").property(p->p.setIri(RDFS.LABEL.getIri())))));
        document.addEntity(query);
    }

    private void testQuery() throws IOException{

        TTEntity qry = new TTEntity().addType(IM.COHORT_QUERY)
          .set(IM.RETURN_TYPE,TTIriRef.iri(IM.NAMESPACE+"Patient"));
        qry
          .setIri(IM.NAMESPACE + "Q_TestQuery")
          .setName("Test for patients either aged between 18 and 65 or with diabetes with the most recent systolic in the last 6 months >150"+
            "not followed by a screening invite, excluding hypertensives");
        Query prof = new Query()
          .setIri(IM.NAMESPACE + "Q_TestQuery")
          .setName("Test for patients either aged between 18 and 65 or with diabetes with the most recent systolic in the last 6 months >150"+
            "not followed by a screening invite, excluding hypertensives")
          .setType(IM.NAMESPACE + "Patient")
          .match(m->m
            .setSet(IM.NAMESPACE+"Q_RegisteredGMS")
            .setName("Registered for GMS services on reference date"))
          .match(m->m
            .setBool(Bool.or)
            .match(or->or
              .property(w->w
                .setIri("age")
                .range(r->r
                  .from(from->from
                    .setOperator(Operator.gte)
                    .setValue("65")
                    .setUnit("YEARS"))
                  .to(to->to
                    .setOperator(Operator.lt)
                    .setValue("70")
                    .setUnit("YEARS")))))
            .match(or->or
              .setSet("http://example/queries#Q_Diabetics"))
            .match(or->or
              .property(p->p.setIri("observation")
                .match(n->n.setType("Observation")
                  .property(ob->ob
                    .setIri("concept")
                    .addIn(new Node().setIri(SNOMED.NAMESPACE+"714628002").setDescendantsOf(true))
                    .setValueLabel("Prediabetes"))))))
          .match(m->m
            .setVariable("latestBP")
            .property(p->p.setIri("observation")
              .match(n->n.setType("Observation")
            .setBool(Bool.and)
            .property(ww->ww
              .setIri("concept")
              .setName("concept")
              .addIn(new Node()
                .setIri(SNOMED.NAMESPACE+"271649006")
                .setDescendantsOrSelfOf(true)
                .setName("Systolic blood pressure"))
              .addIn(new Match()
                .setIri(IM.CODE_SCHEME_EMIS.getIri()+"1994021000006104")
                .setDescendantsOrSelfOf(true)
                .setName("Home systolic blood pressure"))
              .setValueLabel("Office or home systolic blood pressure"))
            .property(ww->ww
              .setIri("effectiveDate")
              .setOperator(Operator.gte)
              .setValue("-6")
              .setUnit("MONTHS")
              .relativeTo(r->r.setParameter("$referenceDate"))
              .setValueLabel("last 6 months"))))
            .addOrderBy(new OrderLimit()
              .setIri("effectiveDate")
              .setLimit(1)
              .setDirection(Order.descending)))
          .match(m->m
            .setVariable("highBPReading")
            .setBool(Bool.or)
            .match(m1->m1
              .setNodeRef("latestBP")
              .setBool(Bool.and)
              .property(w->w
                .setIri(IM.NAMESPACE+"concept")
                .addIn(new Node()
                  .setIri(SNOMED.NAMESPACE+"271649006")
                  .setDescendantsOrSelfOf(true)
                  .setName("Systolic blood pressure"))
                .setValueLabel("Office blood pressure"))
              .property(w->w
                .setIri(IM.NAMESPACE+"numericValue")
                .setOperator(Operator.gt)
                .setValue("140")))
            .match(m1->m1
              .setNodeRef("latestBP")
              .setBool(Bool.and)
              .property(w->w
                .setIri(IM.NAMESPACE+"concept")
                .addIn(new Node()
                  .setIri(IM.CODE_SCHEME_EMIS.getIri()+"1994021000006104")
                  .setDescendantsOrSelfOf(true)
                  .setName("Home systolic blood pressure"))
                .setValueLabel("Home blood pressure"))
              .property(w->w
                .setIri(IM.NAMESPACE+"numericValue")
                .setOperator(Operator.gt)
                .setValue("130"))))
          .match(m->m
            .setExclude(true)
            .property(p->p.setIri(IM.NAMESPACE+"observation")
              .match(n->n.setType("Observation")
            .setBool(Bool.and)
            .property(inv->inv
              .setIri(IM.NAMESPACE+"concept")
              .addIn(new Node().setSet(IM.NAMESPACE+"InvitedForScreening")))
            .property(after->after
              .setIri(IM.NAMESPACE+"effectiveDate")
              .setOperator(Operator.gte)
              .relativeTo(r->r.setNodeRef("highBPReading").setIri("effectiveDate"))))))
          .match(m->m
            .setExclude(true)
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
        prof
          .setType(IM.NAMESPACE+"Patient")
          .setName("Patient")
          .match(m->m
            .property(p->p
              .setIri("gpRegistration")
              .match(m1->m1
            .setBool(Bool.and)
            .property(p1->p1
              .setIri("patientType")
              .addIn(new Node().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .property(pv->pv
              .setIri("effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo(new Property().setParameter("$referenceDate")))
            .property(pv -> pv
              .setBool(Bool.or)
              .property(pv1->pv1
                .setIri("endDate")
                .setNull(true))
              .property(pv1->pv1
                .setIri("endDate")
                .setOperator(Operator.gt)
                .setRelativeTo(new Property().setParameter("$referenceDate")))))));

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
              .setBool(Bool.or)
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
            .property(p->p
              .setIri("gpRegistration")
            .match(n->n
              .setType("GPRegistration")
            .setBool(Bool.and)
            .property(pv->pv
              .setIri("patientType")
              .addIn(new Node().setIri(IM.GMS_PATIENT.getIri()).setName("Regular GMS patient")))
            .property(pv->pv
              .setIri("effectiveDate")
              .setOperator(Operator.lte)
              .setRelativeTo(new Property().setParameter("$referenceDate")))
            .property(pv->pv
              .setBool(Bool.or)
              .property(pv1->pv1
                .setNull(true)
                .setIri("endDate"))
              .property(pv1->pv1
                .setIri("endDate")
                .setOperator(Operator.gt)
                .setRelativeTo(new Property().setParameter("$referenceDate")))))));
        entity
          .set(IM.DEFINITION,TTLiteral.literal(query));
        document.addEntity(entity);
    }




    private void allowableSubTypes() throws IOException {
        TTEntity entity= getQuery("AllowableChildTypes","for a parent entity, the types that can be child types","used in the editor to select the type of entity being created as a subtype");
        Query query= new Query();
        query.setName("Allowable child types for editor");
        query
          .match(m->m
            .setParameter("$this")
            .property(p->p
              .setIri(RDF.TYPE.getIri())
              .setValueVariable("thisType")))
          .match(f->f
            .setVariable("concept")
            .property(w1->w1.setIri(IM.IS_CONTAINED_IN.getIri())
              .addIn(IM.NAMESPACE+"EntityTypes")))
          .match(f->f
            .setNodeRef("concept")
            .property(p->p
              .setIri(SHACL.PROPERTY.getIri())
              .match(n->n
                .setVariable("predicate")
                .setBool(Bool.and)
                .property(a2->a2
                  .setIri(SHACL.NODE.getIri())
                  .addIn(new Node().setRef("thisType")))
                .property(a2->a2
                  .setIri(SHACL.PATH.getIri())
                  .setIn(List.of(Match.iri(IM.IS_CONTAINED_IN.getIri())
                    , Match.iri(RDFS.SUBCLASSOF.getIri()), Match.iri(IM.IS_SUBSET_OF.getIri())))))))
          .match(f->f
            .setBool(Bool.or)
            .match(m1->m1
              .setNodeRef("concept")
                .in(in->in.setRef("thisType")))
            .match(m1->m1
              .setParameter("$this")
              .property(p->p
                .setIri(IM.CONTENT_TYPE.getIri())
                .in(in->in.setRef("concept"))
                .in(in->in.setIri(IM.FOLDER.getIri()))))
            .match(m1->m1
              .setBool(Bool.and)
              .match(m2->m2
                .setParameter("$this")
                .property(p->p
                  .setIri(RDF.TYPE.getIri())
                  .in(in->in.setIri(IM.FOLDER.getIri()))))
              .match(m2->m2
                .setParameter("$this")
                .setExclude(true)
                .property(p->p
                .setIri(IM.CONTENT_TYPE.getIri())))))
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
              .property(w->w
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
              .property(w->w
                .setDescription("property that has this concept or supertype as a domain")
                .setIri(RDFS.DOMAIN.getIri())
                .addIn(new Node().setParameter("this").setAncestorsOf(true))
              ))));
    }

    private void searchProperties() throws JsonProcessingException {
        TTEntity query = getQuery("SearchProperties", "Search for properties by name","Returns a list of properties using a text search to filter the list.");

        query.set(IM.DEFINITION, TTLiteral.literal(
            new Query()
                .setName("Search for properties by name")
                .setActiveOnly(true)
                .match(f->f
                    .setVariable("concept")
                    .setType(RDF.PROPERTY.getIri()))
                .return_(r->r
                    .setNodeRef("concept")
                    .property(p->p.setIri(IM.CODE.getIri()))
                    .property(p->p.setIri(RDFS.LABEL.getIri())))
        ));
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
