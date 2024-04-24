package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.imapi.vocabulary.GRAPH;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CoreQueryImporter implements TTImport {
    public TTDocument document;

    @Override
    public void importData(TTImportConfig config) throws Exception {
        try (TTManager manager = new TTManager()) {
            document = manager.createDocument(GRAPH.DISCOVERY);
            getIsas();
            getDescendants();
            getConcepts();
            getAllowableProperties();
            getAllowableRanges();
            getSearchAll();
            allowableSubTypes();
            currentGMS();
            currentGMSAsMatch();
            agedOver18AsMatch();
            deleteSets();
            getAncestors();
            getSubsets();
            patientsWithActiveCondition(IM.NAMESPACE + "Q_Diabetics", "Patients with active diabetes",
                    SNOMED.NAMESPACE + "73211009", "Diabetes mellitus",
                    SNOMED.NAMESPACE + "315051004", "Diabetes resolved");
            patientsWithActiveCondition(IM.NAMESPACE + "Q_Hypertensives", "Patients with active hypertension",
                    SNOMED.NAMESPACE + "70995007", "Hypertension",
                    SNOMED.NAMESPACE + "162659009", "Hypertension resolved");
            testQuery();
            getActiveDiabetes();
            latestBPMatch();
            latestHighBP();
            objectPropertyRangeSuggestions();
            dataPropertyRangeSuggestions();
            searchProperties();
            dataModelPropertyRange();
            dataModelPropertyByShape();
            searchFolders();
            searchContainedIn();
            searchAllowableSubclass();
            searchAllowableContainedIn();
            output(document, config.getFolder());
            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(document);
            }
        }
    }

    private void agedOver18AsMatch() throws JsonProcessingException {
        TTEntity qry = new TTEntity().addType(iri(IM.MATCH_CLAUSE));
        qry.set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        qry.set(iri(IM.WEIGHTING), TTLiteral.literal(10000));
        qry.set(iri(SHACL.ORDER), 3);
        qry.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));
        qry.setIri(IM.NAMESPACE + "M_AgedOverEighteen")
                .setName("Aged over 18 (feature)")
                .setDescription("Tests wether a person is over 18 years of age.");
        Match over18 = getOver18();
        qry.set(iri(IM.DEFINITION), TTLiteral.literal(over18));
        document.addEntity(qry);
    }

    private Match getOver18() {
        return new Match()
                .setName("Aged over 18 years")
          .addWhere(new Where()
                        .setIri(IM.NAMESPACE + "age")
                        .setUnit("YEAR")
                        .setOperator(Operator.gte)
                        .setValue("18"));
    }

    private void objectPropertyRangeSuggestions() throws JsonProcessingException {
        TTEntity query = getQuery("ObjectPropertyRangeSuggestions", "Range suggestions for object property", "takes account of the data model shape that the property is part of");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Suggested range for a property")
                        .setDescription("get node, class or datatype values (ranges)  of property objects that have 4this as their path")
                        .match(m -> m
                          .addWhere(new Where()
                                .setBoolWhere(Bool.or)
          .where(p -> p
                  .setIri(SHACL.NODE)
                  .match(n -> n
                          .setVariable("range")))
          .where(p -> p
                  .setIri(SHACL.CLASS)
                  .match(n -> n
                          .setVariable("range")))
          .where(p -> p
                  .setIri(SHACL.DATATYPE)
                  .match(n -> n
                          .setVariable("range")))))
                        .match(m -> m
                                .addWhere(new Where()
          .setIri(SHACL.PATH)
          .addIs(new Node().setParameter("this"))))
                        .return_(r -> r.setNodeRef("range").property(p -> p.setIri(RDFS.LABEL)))));
        document.addEntity(query);
    }

    private void dataModelPropertyByShape() throws JsonProcessingException {
        TTEntity query = getQuery("DataModelPropertyByShape", "Data model property", "takes account of the data model shape that the property is part of");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Data model property")
                        .setDescription("get properties of property objects for specific data model and property")
                        .match(m -> m
                                .setInstanceOf(new Node()
          .setParameter("myDataModel"))
                                .addWhere( new Where()
          .setIri(SHACL.PROPERTY)
          .match(n -> n.setVariable("shaclProperty")
                  .addWhere(new Where()
                          .setIri(SHACL.PATH)
                          .addIs(new Node().setParameter("myProperty"))))))
                        .return_(r -> r.
                                setNodeRef("shaclProperty")
                                .setProperty(List.of(
          new ReturnProperty()
                  .setIri(SHACL.CLASS)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.NODE)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.DATATYPE)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.GROUP)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.FUNCTION)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.INVERSEPATH)
                  .setReturn(new Return().setProperty(List.of(new ReturnProperty()
                          .setIri(RDFS.LABEL)))),
          new ReturnProperty()
                  .setIri(SHACL.ORDER),
          new ReturnProperty()
                  .setIri(SHACL.MAXCOUNT),
          new ReturnProperty()
                  .setIri(SHACL.MINCOUNT)
                                ))
                        )));
        document.addEntity(query);
    }

    private void dataModelPropertyRange() throws JsonProcessingException {
        TTEntity query = getQuery("DataModelPropertyRange", "Data model property range", "returns a flat list of data model property ranges based on input data model and property");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Data model property range")
                        .setDescription("get node, class or datatype value (range)  of property objects for specific data model and property")
                        .match(m -> m
                                .setInstanceOf(new Node()
          .setParameter("myDataModel"))
                                .addWhere(new Where()
          .setIri("http://www.w3.org/ns/shacl#property")
          .match(m1 -> m1
                  .setVariable("shaclProperty")
                  .where(p2 -> p2
                          .setIri(SHACL.PATH)
                          .is(in -> in
                                  .setParameter("myProperty")))
                  .where(p2 -> p2
                          .setBoolWhere(Bool.or)
                          .where(p3 -> p3
                                  .setIri(SHACL.CLASS)
                                  .match(m3 -> m3
               .setVariable("propType")))
                          .where(p3 -> p3
                                  .setIri(SHACL.NODE)
                                  .match(m3 -> m3
               .setVariable("propType")))
                          .where(p3 -> p3
                                  .setIri(SHACL.DATATYPE)
                                  .match(m3 -> m3
               .setVariable("propType")))))))
                        .return_(r -> r
                                .setNodeRef("propType")
                                .property(p -> p
          .setIri(RDFS.LABEL)))));
        document.addEntity(query);
    }

    private void dataPropertyRangeSuggestions() throws JsonProcessingException {
        TTEntity query = getQuery("dataPropertyRangeSuggestions", "Range suggestions for object property", "takes account of the data model shape that the property is part of");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Suggested range for a data property")
                        .setDescription("get datatype values (ranges)  of property objects that have 4this as their path")
                        .match(m -> m
                                .addWhere(new Where()
          .setIri(SHACL.DATATYPE)
          .match(n -> n
                  .setVariable("range"))))
                        .return_(s -> s.setNodeRef("range").property(p -> p.setIri(RDFS.LABEL)))));
        document.addEntity(query);
    }

    private void getActiveDiabetes() throws JsonProcessingException {
        TTEntity entity = new TTEntity().addType(iri(IM.MATCH_CLAUSE))
                .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        entity.setIri(IM.NAMESPACE + "M_ActiveDiabetes");
        entity.setName("Active Diabetes (Latest entry for diabetes not followed by a resolution)");
        entity.setDescription("Entry for diabetes not followed by a diabetes resolved entry");
        entity.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));
        entity.set(iri(SHACL.ORDER), 2);
        entity.set(iri(IM.DEFINITION), TTLiteral.literal(getActiveDiabetesMatch()));
        document.addEntity(entity);
    }

    private void latestHighBP() throws JsonProcessingException {
        TTEntity entity = new TTEntity().addType(iri(IM.MATCH_CLAUSE))
                .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        entity.setIri(IM.NAMESPACE + "M_LatestRecentHighSystolic");
        entity.setName("Latest systolic blood pressure in the last 12 months is high");
        entity.setDescription("Latest home or office BP within the last 12 months is either >140 if in the office of >130 if done at home or self reported");
        entity.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));
        entity.set(iri(SHACL.ORDER), 4);
        Match match = new Match()
          .path(p -> p
            .setIri(IM.NAMESPACE + "patient"))
          .setTypeOf(IM.NAMESPACE + "Observation")
            .where(ww -> ww
              .setIri(IM.NAMESPACE + "concept")
              .setName("concept")
              .addIs(new Node()
                  .setIri(SNOMED.NAMESPACE + "999035921000230109")
                  .setDescendantsOrSelfOf(true)
                  .setName("Systolic blood pressure recording"))
              .addIs(new Node()
                  .setIri(GRAPH.EMIS + "1994021000006104")
                  .setDescendantsOrSelfOf(true)
                  .setName("Home systolic blood pressure"))
          .setValueLabel("Office home or self recorded systolic blood pressure"))
            .where(ww -> ww
          .setIri(IM.NAMESPACE + "value")
          .setIsNotNull(true))
            .where(ww -> ww
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setOperator(Operator.gte)
          .setValue("-12")
          .setUnit("MONTHS")
          .relativeTo(r -> r.setParameter("$referenceDate"))
          .setValueLabel("last 12 months"))
          .setOrderBy(new OrderLimit()
           .setProperty(new OrderDirection()
                  .setIri(IM.NAMESPACE + "effectiveDate")
                  .setDirection(Order.descending))
          .setLimit(1))
          .then(t -> t.setVariable("highBPReading")
            .setBoolMatch(Bool.or)
            .match(m4 -> m4
                .where(w -> w
                  .setIri(IM.NAMESPACE + "concept")
                  .addIs(new Node()
                                  .setIri(SNOMED.NAMESPACE + "271649006")
                                  .setDescendantsOrSelfOf(true)
                                  .setName("Systolic blood pressure"))
                          .setValueLabel("Office blood pressure"))
                .where(w -> w
                  .setIri(IM.NAMESPACE + "numericValue")
                          .setOperator(Operator.gt)
                          .setValue("140"))))
            .match(m4 -> m4
              .addWhere (new Where()
                .setBoolWhere(Bool.and)
                .where(w -> w
                          .setIri(IM.NAMESPACE + "concept")
                          .addIs(new Node()
                                  .setIri(GRAPH.EMIS + "1994021000006104")
                                  .setDescendantsOrSelfOf(true)
                                  .setName("Home systolic blood pressure"))
                          .setValueLabel("Home blood pressure"))
                .where(w -> w
                          .setIri(IM.NAMESPACE + "numericValue")
                          .setOperator(Operator.gt)
                          .setValue("130"))));
        entity.set(iri(IM.DEFINITION), TTLiteral.literal(match));
        document.addEntity(entity);
        entity.set(iri(IM.DEFINITION), TTLiteral.literal(match));
        document.addEntity(entity);
    }



    private void getAncestors() throws JsonProcessingException {
        TTEntity query = getQuery("GetAncestors", "Get active supertypes of concept", "returns transitive closure of an entity and its supertypes, usually used with a text search filter to narrow results");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
        query.set(iri(IM.DEFINITION),
                TTLiteral.literal(new Query()
                        .setName("All subtypes of an entity, active only")
                        .setActiveOnly(true)
                        .match(w -> w
                                .setVariable("isa")
                                .setInstanceOf(new Node()
          .setParameter("this")
          .setAncestorsOf(true)))
                        .return_(s -> s.setNodeRef("isa")
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(IM.CODE)))));
    }

    private void latestBPMatch() throws JsonProcessingException {
        TTEntity entity = new TTEntity().addType(iri(IM.MATCH_CLAUSE))
                .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        entity.setIri(IM.NAMESPACE + "M_LatestSystolicBP12M");
        entity.setName("Latest systolic blood pressure in the last 12 months");
        entity.setDescription("The latest systolic blood pressure that has a value");

        entity.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));
        entity.set(iri(SHACL.ORDER), 3);

        Match match = new Match()
                .path(p -> p
                        .setIri(IM.NAMESPACE + "patient"))
              .where(ww -> ww
          .setIri(IM.NAMESPACE + "concept")
          .setName("concept")
          .addIs(new Node()
                  .setIri(SNOMED.NAMESPACE + "999035921000230109")
                  .setName("Systolic blood pressure recording")))
              .where(ww -> ww
          .setIri(IM.NAMESPACE + "value")
          .setIsNotNull(true))
              .where(ww -> ww
          .setIri(IM.NAMESPACE + "effectiveDate")
          .setOperator(Operator.gte)
          .setValue("-12")
          .setUnit("MONTHS")
          .relativeTo(r -> r.setParameter("$referenceDate"))
          .setValueLabel("last 12 months"))
          .setOrderBy(new OrderLimit()
          .setProperty(new OrderDirection()
                  .setIri(IM.NAMESPACE + "effectiveDate")
                  .setDirection(Order.descending))
          .setLimit(1));
        entity.set(iri(IM.DEFINITION), TTLiteral.literal(match));
        document.addEntity(entity);
    }

    private Match getActiveDiabetesMatch() {
        return new Match()
                .setName("Active diabetics")
          .path(p->p.setIri(IM.NAMESPACE+"patient"))
          .setTypeOf(IM.NAMESPACE + "Observation")
          .setVariable("latestDiabetes")
            .where(ww -> ww
                .setIri(IM.NAMESPACE + "concept")
                .setName("concept")
                .addIs(new Node()
                  .setIri("http://snomed.info/sct#999004691000230108")
                  .setName("Diabetes Mellitus")))
          .setOrderBy(new OrderLimit()
            .setProperty(new OrderDirection()
                  .setIri(IM.NAMESPACE + "effectiveDate")
                  .setDirection(Order.descending))
                    .setLimit(1))
          .then(m1 -> m1
            .setExclude(true)
                .setVariable("ResolvedDiabetes")
                .where(ww -> ww
                  .setIri(IM.NAMESPACE + "concept")
                  .setName("concept")
                  .addIs(new Node()
                          .setIri("http://snomed.info/sct#999003371000230102")
                          .setName("Diabetes Resolved")))
                .where(ww -> ww
                  .setIri(IM.NAMESPACE + "effectiveDate")
                  .setOperator(Operator.gte)
                  .relativeTo(r -> r.setNodeRef("latestDiabetes").setIri(IM.NAMESPACE + "effectiveDate"))));
    }

    private void patientsWithActiveCondition(String iri, String name, String activeIri, String activeName, String inactiveIri, String inactiveName) throws JsonProcessingException {
        TTEntity qry = new TTEntity(iri)
                .addType(iri(IM.COHORT_QUERY))
                .setName(name)
                .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));

        Query definition = new Query()
                .setIri(iri)
                .setName(name)
                .setTypeOf(IM.NAMESPACE + "Patient")
                .match(m -> m
                        .path(p -> p
                                .setIri(IM.NAMESPACE + "patient"))
                    .setTypeOf(IM.NAMESPACE + "Observation")
                    .where(ww -> ww
                        .setIri(IM.NAMESPACE + "concept")
                        .setName("concept")
                        .addIs(new Node()
                          .setIri(activeIri)
                          .setName(activeName)
                          .setDescendantsOrSelfOf(true))
                        .addIs(new Node()
                          .setIri(inactiveIri)
                          .setName(inactiveName)
                          .setDescendantsOrSelfOf(true))
                  .setValueLabel(activeName + " or " + inactiveName))
          .setOrderBy(new OrderLimit()
                  .setProperty(new OrderDirection()
                          .setIri(IM.NAMESPACE + "effectiveDate")
                          .setDirection(Order.descending))
                  .setLimit(1))
          .then(t -> t
            .addWhere(new Where()
                          .setIri(IM.NAMESPACE + "concept")
                          .addIs(new Node()
                                  .setIri(activeIri)
                                  .setName(activeName)
                                  .setDescendantsOrSelfOf(true)))));

        qry.set(iri(IM.DEFINITION), TTLiteral.literal(definition));
        qry.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);
    }

    private void testQuery() throws IOException {

        TTEntity qry = new TTEntity().addType(iri(IM.COHORT_QUERY))
                .set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        qry
                .setIri(IM.NAMESPACE + "Q_TestQuery")
                .setName("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140," +
                        "not followed by a screening invite, excluding hypertensives");
        Query prof = new Query()
                .setIri(IM.NAMESPACE + "Q_TestQuery")
                .setName("Test for patients either aged between 65 and 70 or with diabetes with the most recent systolic in the last 12 months either home >130 or office >140," +
                        "not followed by a screening invite, excluding hypertensives")
                .setTypeOf(IM.NAMESPACE + "Patient")
                .match(m -> m
                        .addIs(new Node().setIri(IM.NAMESPACE + "Q_RegisteredGMS")
                                .setName("Registered for GMS services on reference date")))
                .match(m -> m
                        .setBoolMatch(Bool.or)
                        .match(or -> or
                          .addWhere(new Where()
                            .setIri(IM.NAMESPACE + "age")
                            .range(r -> r
                            .from(from -> from
                                .setOperator(Operator.gte)
                                .setValue("65")
                                .setUnit("YEARS"))
                            .to(to -> to
                                .setOperator(Operator.lt)
                                .setValue("70")
                                .setUnit("YEARS")))))
                        .match(or -> or
                                .addIs(new Node().setIri(IM.NAMESPACE + "Q_Diabetics")))
                        .match(or -> or
                            .path(p -> p.setIri(IM.NAMESPACE + "patient"))
                            .setTypeOf(IM.NAMESPACE + "Observation")
                            .addWhere(new Where()
                                .setIri(IM.NAMESPACE + "concept")
                                .addIs(new Node().setIri(SNOMED.NAMESPACE + "714628002").setDescendantsOf(true))
                                .setValueLabel("Prediabetes"))))
                .match(m -> m
                    .path(p -> p
                    .setIri(IM.NAMESPACE + "patient"))
                    .setTypeOf(IM.NAMESPACE + "Observation")
                    .where(ww -> ww
                        .setIri(IM.NAMESPACE + "concept")
                        .setName("concept")
                        .addIs(new Node()
                          .setIri(SNOMED.NAMESPACE + "271649006")
                          .setDescendantsOrSelfOf(true)
                          .setName("Systolic blood pressure"))
                        .addIs(new Node()
                          .setIri(GRAPH.EMIS + "1994021000006104")
                          .setDescendantsOrSelfOf(true)
                          .setName("Home systolic blood pressure"))
                  .setValueLabel("Office or home systolic blood pressure"))
                .where(ww -> ww
                  .setIri(IM.NAMESPACE + "effectiveDate")
                  .setOperator(Operator.gte)
                  .setValue("-12")
                  .setUnit("MONTHS")
                  .relativeTo(r -> r.setParameter("$referenceDate"))
                  .setValueLabel("last 12 months"))
          .setOrderBy(new OrderLimit()
                  .setProperty(new OrderDirection()
                          .setIri(IM.NAMESPACE + "effectiveDate")
                          .setDirection(Order.descending))
                  .setLimit(1))
          .then(t -> t.setVariable("highBPReading")
                  .setBoolMatch(Bool.or)
                  .match(m4 -> m4
                          .where(w -> w
                            .setIri(IM.NAMESPACE + "concept")
                            .addIs(new Node()
                            .setIri(SNOMED.NAMESPACE + "271649006")
                            .setDescendantsOrSelfOf(true)
                            .setName("Systolic blood pressure"))
                            .setValueLabel("Office blood pressure"))
                          .where(w -> w
                                  .setIri(IM.NAMESPACE + "numericValue")
                                  .setOperator(Operator.gt)
                                  .setValue("140")))
                  .match(m4 -> m4
                    .setBoolWhere(Bool.and)
                          .where(w -> w
                            .setIri(IM.NAMESPACE + "concept")
                            .addIs(new Node()
                                .setIri(GRAPH.EMIS + "1994021000006104")
                                .setDescendantsOrSelfOf(true)
                                .setName("Home systolic blood pressure"))
                            .setValueLabel("Home blood pressure"))
                          .where(w -> w
                                  .setIri(IM.NAMESPACE + "numericValue")
                                  .setOperator(Operator.gt)
                                  .setValue("130")))))
                .match(m -> m
                  .setBoolWhere(Bool.and)
                  .setExclude(true)
                  .path(w -> w.setIri(IM.NAMESPACE + "patient"))
                  .setTypeOf(IM.NAMESPACE + "Observation")
                    .where(inv -> inv
                        .setIri(IM.NAMESPACE + "concept")
                        .addIs(new Node().setIri(IM.NAMESPACE + "InvitedForScreening")))
                    .where(after -> after
                        .setIri(IM.NAMESPACE + "effectiveDate")
                        .setOperator(Operator.gte)
                        .relativeTo(r -> r.setNodeRef("highBPReading").setIri(IM.NAMESPACE + "effectiveDate"))))
                .match(m -> m
                        .setExclude(true)
                        .addIs(new Node().setIri(IM.NAMESPACE + "Q_Hypertensives")
                                .setName("Hypertensives")));
        qry.set(iri(IM.DEFINITION), TTLiteral.literal(prof));
        qry.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        document.addEntity(qry);
    }

    private void deleteSets() throws JsonProcessingException {
        TTEntity entity = new TTEntity()
                .setIri(IM.NAMESPACE + "DeleteSets")
                .setName("Delete all concept sets in a graph")
                .setDescription("Pass in the graph name as a 'this' argument and it deletes all sets");
        entity.set(iri(IM.UPDATE_PROCEDURE), TTLiteral.literal(new Update()
                .match(m -> m
                        .setGraph(new Node().setParameter("this"))
                        .setTypeOf(IM.CONCEPT_SET))
                .addDelete(new Delete())));
        document.addEntity(entity);


    }

    private void currentGMS() throws JsonProcessingException {

        TTEntity qry = new TTEntity().addType(iri(IM.COHORT_QUERY));
        qry.set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        qry.set(iri(IM.WEIGHTING), TTLiteral.literal(10000));
        qry.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "Q_StandardCohorts"));
        qry
                .setIri(IM.NAMESPACE + "Q_RegisteredGMS")
                .setName("Patients registered for GMS services on the reference date")
                .setDescription("For any registration period,a registration start date before the reference date and no end date," +
                        "or an end date after the reference date.");
        qry.set(iri(IM.DEFINITION), TTLiteral.literal(getGmsPatient(qry.getIri(), qry.getName(), qry.getDescription())));
        document.addEntity(qry);
    }

    private void currentGMSAsMatch() throws JsonProcessingException {

        TTEntity qry = new TTEntity().addType(iri(IM.MATCH_CLAUSE));
        qry.set(iri(IM.RETURN_TYPE), TTIriRef.iri(IM.NAMESPACE + "Patient"));
        qry.set(iri(IM.WEIGHTING), TTLiteral.literal(10000));
        qry.set(iri(SHACL.ORDER), 1);
        qry.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "M_CommonClauses"));
        qry
                .setIri(IM.NAMESPACE + "M_RegisteredGMS")
                .setName("Registered for GMS services on the reference date")
                .setDescription("For any registration period,a registration start date before the reference date and no end date," +
                        "or an end date after the reference date.");
        Match gmsMatch = getGMSMatch();
        qry.set(iri(IM.DEFINITION), TTLiteral.literal(gmsMatch));
        document.addEntity(qry);
    }

    private Query getGmsPatient(String iri, String name, String description) {
        Query prof = new Query();
        prof.setIri(iri);
        prof.setName(name);
        prof.setDescription(description);
        prof
                .setTypeOf(IM.NAMESPACE + "Patient")
                .setName("Patient");
        prof.addMatch(getGMSMatch());
        return prof;
    }

    private Match getGMSMatch() {
        return new Match()
          .setBoolWhere(Bool.and)
          .setName("Registered GMS services on the reference date")
          .path(p -> p
            .setIri(IM.NAMESPACE + "patient"))
          .setTypeOf(IM.NAMESPACE + "GPRegistrationEpisode")
            .where(p1 -> p1
                .setIri(IM.NAMESPACE + "gpPatientType")
                .addIs(new Node().setIri(IM.GMS_PATIENT).setName("Regular GMS patient")))
            .where(pv -> pv
                .setIri(IM.NAMESPACE + "effectiveDate")
                .setOperator(Operator.lte)
                .setRelativeTo(new PropertyRef().setParameter("$referenceDate")))
            .where(pv -> pv
                .setBoolWhere(Bool.or)
                .where(pv1 -> pv1
                  .setIri(IM.NAMESPACE + "endDate")
                  .setIsNull(true))
                .where(pv1 -> pv1
                  .setIri(IM.NAMESPACE + "endDate")
                  .setOperator(Operator.gt)
                  .setRelativeTo(new PropertyRef().setParameter("$referenceDate"))));
    }

    private void getSearchAll() throws JsonProcessingException {
        TTEntity query = getQuery("SearchmainTypes", "Search for entities of the main types", "used to filter free text searches excluding queries and concept sets");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setActiveOnly(true)
                        .setName("Search for all main types")
                        .match(f -> f
                                .setBoolMatch(Bool.or)
                                .match(w -> w
          .setTypeOf(IM.CONCEPT))
                                .match(w -> w
          .setTypeOf(IM.CONCEPT_SET))
                                .match(w -> w
          .setTypeOf(IM.FOLDER))
                                .match(w -> w
          .setTypeOf(IM.VALUESET))
                                .match(w -> w
          .setTypeOf(IM.NAMESPACE + "dataModelProperty")))
                        .return_(s -> s
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(RDFS.COMMENT))
                                .property(p -> p.setIri(IM.CODE))
                                .property(p -> p.setIri(IM.HAS_STATUS)
          .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
                                .property(p -> p.setIri(IM.HAS_SCHEME)
          .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
                                .property(p -> p.setIri(RDF.TYPE)
          .return_(r -> r.property(rp -> rp.setIri(RDFS.LABEL))))
                                .property(p -> p.setIri(IM.WEIGHTING))
                        )));
    }


    private void allowableSubTypes() throws IOException {
        TTEntity entity = getQuery("AllowableChildTypes", "Allowable child types for editor", "used in the editor to select the type of entity being created as a subtype");
        Query query = new Query();
        query.setName("Allowable child types for editor");
        query
                .match(m -> m
                  .setInstanceOf(new Node()
                    .setParameter("$this"))
                  .addWhere(new Where()
                                .setIri(RDF.TYPE)
                                .setValueVariable("thisType")))
                .match(m -> m
                  .setVariable("concept")
                  .addWhere(new Where()
                    .setIri(IM.IS_CONTAINED_IN)
                      .addIs(IM.NAMESPACE + "EntityTypes")))
                .match(m ->m
                  .setNodeRef("concept")
                  .addWhere(new Where()
                    .setIri(SHACL.PROPERTY)
                    .match(n -> n
                    .setVariable("predicate")
                        .where(a2 -> a2
                            .setIri(SHACL.NODE)
                            .addIs(new Node().setNodeRef("thisType")))
                        .where(a2 -> a2
                        .setIri(SHACL.PATH)
                        .setIs(List.of(Node.iri(IM.IS_CONTAINED_IN)
                          , Node.iri(RDFS.SUBCLASS_OF), Node.iri(IM.IS_SUBSET_OF)))))))
                .match(m -> m
                        .setBoolMatch(Bool.or)
                        .match(m1 -> m1
                                .setNodeRef("concept")
                                .setInstanceOf(new Node().setNodeRef("thisType")))
                        .match(m1 -> m1
                                .setInstanceOf(new Node()
                        .setParameter("$this"))
                          .addWhere(new Where()
                            .setIri(IM.CONTENT_TYPE)
                            .is(in -> in.setNodeRef("concept"))
                            .is(in -> in.setIri(IM.FOLDER))))
                        .match(m1 -> m1
                          .setBoolMatch(Bool.and)
                          .match(m2 -> m2
                            .setInstanceOf(new Node()
                            .setParameter("$this"))
                            .addWhere(new Where()
                            .setIri(RDF.TYPE)
                            .is(in -> in.setIri(IM.FOLDER))))
                          .match(m2 -> m2
                            .setInstanceOf(new Node()
                            .setParameter("$this"))
                            .setExclude(true)
                            .addWhere(new Where()
                                .setIri(IM.CONTENT_TYPE)))))
                .return_(s -> s
                        .setNodeRef("concept")
                        .property(p -> p
                                .setIri(RDFS.LABEL))
                        .property(p -> p
                                .setIri(SHACL.PROPERTY)
                                .return_(s1 -> s1
          .setNodeRef("predicate")
          .property(p1 -> p1
                  .setIri(SHACL.PATH)))));
        entity.set(iri(IM.DEFINITION), TTLiteral.literal(query));

    }

    private void getAllowableRanges() throws JsonProcessingException {
        TTEntity query = getQuery("AllowableRanges", "Allowable ranges for a particular property or its ancestors", "uses inverse range property to return the ranges of the property as authored. Should be used with another ");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Allowable Ranges for a property and super properties")
                        .setActiveOnly(true)
                        .return_(r -> r
                                .property(s -> s.setIri(IM.CODE))
                                .property(s -> s.setIri(RDFS.LABEL)))
                        .match(f -> f
                          .addWhere(new Where()
                            .setInverse(true)
                            .setIri(RDFS.RANGE)
                            .addIs(new Node().setParameter("this")
                              .setAncestorsOf(true))))));
        document.addEntity(query);
    }

    private void getAllowableProperties() throws JsonProcessingException {
        TTEntity query = getQuery("AllowableProperties", "Allowable properties for a terminology concept", "Returns a list of properties for a particular term concept, used in value set definitions with RCL");

        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Allowable Properties for a terminology concept")
                        .setActiveOnly(true)
                        .return_(r -> r
                                .setNodeRef("concept")
                                .property(p -> p.setIri(IM.CODE))
                                .property(p -> p.setIri(RDFS.LABEL)))
                        .match(f -> f
                                .setVariable("concept")
                                .setTypeOf(IM.CONCEPT)
                          .addWhere(new Where()
                            .setDescription("property that has this concept or supertype as a domain")
                            .setIri(RDFS.DOMAIN)
                            .addIs(new Node().setParameter("this").setAncestorsOf(true))
                                ))));
    }

    private void searchProperties() throws JsonProcessingException {
        TTEntity query = getQuery("SearchProperties", "Search for properties by name", "Returns a list of properties using a text search to filter the list.");

        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Search for properties by name")
                        .setActiveOnly(true)
                        .match(f -> f
                                .setVariable("concept")
                                .setTypeOf(RDF.PROPERTY))
                        .return_(r -> r
                                .setNodeRef("concept")
                                .property(p -> p.setIri(IM.CODE))
                                .property(p -> p.setIri(RDFS.LABEL)))
        ));
    }

    private void getConcepts() throws JsonProcessingException {
        TTEntity query = getQuery("SearchEntities", "Search for entities of a certain type", "parameter 'this' set to the list of type iris, Normally used with a text search entry to filter the list");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setActiveOnly(true)
                        .setName("Search for concepts")
                        .match(w -> w
                                .setTypeOf(new Node()
          .setParameter("this")))
                        .return_(r -> r
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(RDF.TYPE)))));
    }

    private void searchFolders() throws JsonProcessingException {
        TTEntity query = getQuery("SearchFolders", "Search for folder by name", "Returns a list of folder using a text search");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Search for folders by name")
                        .setActiveOnly(true)
                        .match(f -> f
                                .setVariable("folder")
                                .setTypeOf(IM.FOLDER))
                        .return_(r -> r
                                .setNodeRef("folder")
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(RDF.TYPE)))
        ));
    }

    private void searchContainedIn() throws JsonProcessingException {
        TTEntity query = getQuery("SearchContainedIn", "Search for entities contained in parent folder", "parameter 'value' needs to be set to the parent folder");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Search for entities contained in parent folder")
                        .setActiveOnly(true)
                        .match(f -> f
                            .addWhere(new Where()
                            .setIri(IM.IS_CONTAINED_IN)
                                .is(i -> i
                             .setParameter("value")
          )
                                )
                        )
        ));
    }

    private void searchAllowableSubclass() throws JsonProcessingException {
        TTEntity query = getQuery("SearchAllowableSubclass", "Search for allowable subclasses", "parameter 'value' needs to be set to current entity type");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Search for allowable subclasses")
                        .setActiveOnly(true)
                        .match(f -> f
                                .addWhere(new Where()
          .setIri(RDF.TYPE)
          .is(i -> i
                  .setParameter("value")
          )
                                )
                        )
        ));
    }

    private void searchAllowableContainedIn() throws JsonProcessingException {
        TTEntity query = getQuery("SearchAllowableContainedIn", "Search for allowable parent folder", "parameter 'value' needs to be set to the current entity type");
        query.set(iri(IM.DEFINITION), TTLiteral.literal(
                new Query()
                        .setName("Search for allowable contained in")
                        .setActiveOnly(true)
                        .match(m -> m
                                .setVariable("folder")
                                .setTypeOf(IM.FOLDER)
                            .addWhere(new Where()
                                .setBoolWhere(Bool.or)
                                .where(p -> p
                                .setIri(IM.CONTENT_TYPE)
                                .setIsNull(true))
                                .where(p -> p
                                    .setIri(IM.CONTENT_TYPE)
                                    .is(i -> i.setParameter("value")))

                        )
                        )
                        .return_(r -> r
                                .setNodeRef("folder")
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(RDF.TYPE)))
        ));
    }

    private void getIsas() throws JsonProcessingException {
        TTEntity query = getQuery("GetIsas", "Get active subtypes of concept", "returns transitive closure of an entity and its subtypes, usually used with a text search filter to narrow results");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
        query.set(iri(IM.DEFINITION),
                TTLiteral.literal(new Query()
                        .setName("All subtypes of an entity, active only")
                        .setActiveOnly(true)
                        .match(w -> w
                                .setVariable("isa")
                                .setInstanceOf(new Node()
          .setParameter("this")
          .setDescendantsOrSelfOf(true)))
                        .return_(s -> s.setNodeRef("isa")
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(IM.CODE)))));
    }

    private void getDescendants() throws JsonProcessingException {
        TTEntity query = getQuery("GetDescendants", "Get active subtypes of concept", "returns transitive closure of an entity and its subtypes, usually used with a text search filter to narrow results");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
        query.set(iri(IM.DEFINITION),
                TTLiteral.literal(new Query()
                        .setName("All subtypes of an entity, active only")
                        .setActiveOnly(true)
                        .match(w -> w
                                .setVariable("isa")
                                .setInstanceOf(new Node()
          .setParameter("this")
          .setDescendantsOf(true)))
                        .return_(s -> s.setNodeRef("isa")
                                .property(p -> p.setIri(RDFS.LABEL))
                                .property(p -> p.setIri(IM.CODE)))));
    }

    private void getSubsets() throws JsonProcessingException {
        TTEntity query = getQuery("GetSubsets", "Get subsets using superset iri", "return items which have a isSubsetOf predicate linked to the iri provided");
        query.getPredicateMap().remove(TTIriRef.iri(IM.NAMESPACE + "query"));
        query.set(iri(IM.DEFINITION),
            TTLiteral.literal(new Query()
                .setName("All subsets of an entity, active only")
                .setActiveOnly(true)
                .return_(r -> r
                    .property(s -> s.setIri(IM.CODE))
                    .property(s -> s.setIri(RDFS.LABEL)))
                .match(f -> f
                    .addWhere(new Where()
                        .setIri(IM.IS_SUBSET_OF)
                        .addIs(new Node().setParameter("this")
                        )
                    )
                )
            )
        );
    }

    private TTEntity getQuery(String iri, String name, String comment) {
        TTEntity entity = new TTEntity()
                .setIri(IM.NAMESPACE + "Query_" + iri)
                .setName(name)
                .setDescription(comment)
                .addType(iri(IM.QUERY));
        entity.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "IMFormValidationQueries"));
        document.addEntity(entity);
        return entity;
    }

    @Override
    public void validateFiles(String inFolder) throws TTFilerException {
        // No files to validate
    }

    private void output(TTDocument document, String directory) throws IOException {

        try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\CoreQueriesAll.json")) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
            String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(document);
            writer.write(doc);
        }
        for (TTEntity entity : document.getEntities()) {
            if (entity.isType(iri(IM.MATCH_CLAUSE))) {
                Match match = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Match.class);
                outputMatch(entity.getName(), match, directory);
            } else {
                if (entity.get(iri(IM.DEFINITION)) != null) {
                    Query query = entity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
                    outputQuery(query, directory);
                }
            }
        }

    }

    private void outputQuery(Query qry, String directory) throws IOException {
        String name = qry.getName();
        if (name.length() > 20)
            name = name.substring(0, 20);
        try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\" + name + "+.json")) {
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
            String doc = objectMapper.writerWithDefaultPrettyPrinter()
                    .withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qry);
            writer.write(doc);
        }
    }

    private void outputMatch(String name, Match qry, String directory) throws IOException {
        if (name.length() > 20)
            name = name.substring(0, 20);
        try (FileWriter writer = new FileWriter(directory + "\\DiscoveryCore\\CoreQueries\\" + name + "+.json")) {
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
