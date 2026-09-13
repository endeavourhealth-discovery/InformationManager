package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.logic.service.SetService;
import org.endeavourhealth.imapi.model.Pageable;
import org.endeavourhealth.imapi.queryengine.QueryDescriptor;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.requests.QueryRequest;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.NAMESPACE;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class IndicatorImporter {
  public ObjectMapper om= new ObjectMapper();
  private static final Logger LOG = LoggerFactory.getLogger(IndicatorImporter.class);
  private final EntityService entityService = new EntityService();
  private final QueryDescriptor descriptor = new QueryDescriptor();
  private NAMESPACE namespace;
  private final SearchService searchService = new SearchService();
  private final Set<String> unlabelledClauses= new HashSet<>();
  private TTDocument document;


  public void generate(String indicatorFile,String indicatorFolderIri,String indicatorFolderName,NAMESPACE namespace) throws Exception {
    this.namespace = namespace;

    try (TTManager manager = new TTManager()) {
      document = manager.createDocument();
      try {
        TTEntity folder = new TTEntity()
          .setIri(indicatorFolderIri)
          .setName(indicatorFolderName)
          .addType(iri(IM.FOLDER))
          .setScheme(iri(NAMESPACE.SMARTLIFE))
          .set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(NAMESPACE.IM + "Indicators"))
          .addObject(iri(IM.CONTENT_TYPE), iri(IM.INDICATOR));
        document.addEntity(folder);
        importIndicators(indicatorFile,folder.getIri());
        for (int i = 0; i < document.getEntities().size(); i++) {
          TTEntity indicator = document.getEntities().get(i);
          if (indicator.isType(iri(IM.INDICATOR))) {
            TTEntity indicatorQuery = entityService.getPartialEntities(Set.of(indicator.get(iri(IM.NUMERATOR)).asIriRef().getIri()), Set.of(IM.DEFINITION.toString())).getFirst();
            addColumnGroups(indicator, indicatorQuery.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class));
          }
        }
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
          filer.fileDocument(document);
        }
      } catch (Exception e) {
        LOG.error("Error importing indicators: Indicators not imported", e);
        ;
      }
    }
  }


  private void addColumnGroups(TTEntity indicator, Query indicatorQuery) throws Exception {
    Map<String,Set<String>> valueSets= new HashMap<>();
    if (indicator.get(iri(IM.DENOMINATOR))!=null) {
      String cohortIri = indicator.get(IM.DENOMINATOR).asIriRef().getIri();
      TTEntity dataSetEntity = new TTEntity().setName("Data set for " + indicator.getName());
      String dataSetIri= (namespace + "I_" + (indicator.getName().replaceAll("[^a-zA-Z0-9._~\\[\\],%-]", ""))).toLowerCase();
      dataSetEntity.setIri(dataSetIri)
        .addType(iri(IM.QUERY));
      dataSetEntity.setScheme(iri(namespace));
      document.addEntity(dataSetEntity);
      Query datasetQuery = new Query();
      datasetQuery.setTypeOf(NAMESPACE.IM + "Patient");
      datasetQuery.setIs(Node.iri(cohortIri));
      dataSetEntity.addObject(iri(IM.DEPENDENT_ON), iri(cohortIri));
      TTEntity patientDetails = entityService.getPartialEntity(NAMESPACE.IM+"ColumnGroup_patient_details",Set.of(IM.DEFINITION.toString()));
      Query patientColumnGroup = patientDetails.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
      datasetQuery.addColumnGroup(patientColumnGroup);
      addValueSets(indicatorQuery,valueSets);
      if (!valueSets.isEmpty()) {
        for (String setType : valueSets.keySet()) {
          Set<String> sets = valueSets.get(setType);
          String valueSet= createValueSet(indicator,setType,sets);
          addEventGroup(datasetQuery, setType,valueSet);
        }
      }
      dataSetEntity.set(iri(IM.DEFINITION), TTLiteral.literal(datasetQuery));
      indicator.set(iri(IM.HAS_DATASET),iri(dataSetIri));
    }
  }

  private String createValueSet(TTEntity indicator, String setType, Set<String> sets) {
    Set<String> members= new HashSet<>();
    SetService setService = new SetService();
    for (String set : sets) {
      Pageable<Node> nodeMembers= setService.getDirectOrEntailedMembersFromIri(set,false,1,10000);
      if (nodeMembers.getResult()!=null) {
        for (Node nodeMember : nodeMembers.getResult()) {
          members.add(nodeMember.getIri());
        }
      }
      else members.add(set);
    }
    TTEntity setEntity = new TTEntity().setName("Concept set ("+setType+") for " + indicator.getName());
    setEntity.setIri(namespace + "CSET_" +setType+"_" +(indicator.getName().replaceAll("[^a-zA-Z0-9._~\\[\\],%-]", "")).toLowerCase());
    setEntity.addType(iri(IM.CONCEPT_SET));
    setEntity.setScheme(iri(namespace));
    for (String member : members) {
      setEntity.addObject(iri(IM.HAS_MEMBER),iri(member));
    }
    document.addEntity(setEntity);
    return setEntity.getIri();
  }

  private void addValueSets(Query query,Map<String,Set<String>> valueSets) throws JsonProcessingException {
    for (List<Query> matches : Arrays.asList(query.getAnd(), query.getOr(), query.getRule())) {
      if (matches != null) {
        int clauseIndex = 0;
        for (Query match : matches) {
          clauseIndex++;
          if (clauseIndex < 2 &&query.getRule() != null) continue;
          if (match.getIs() != null) {
            String iri = match.getIs().getIri();
            TTEntity dependentEntity = entityService.getPartialEntities(Set.of(iri), Set.of(IM.DEFINITION.toString())).getFirst();
            Query dependentQuery = dependentEntity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
            if (dependentQuery != null) {
              addValueSets(dependentQuery, valueSets);
            }
          }
          if (match.getTypeOf() != null&&match.getWhere()!=null) {
            String typeOf = match.getTypeOf().getIri();
            if (Set.of(NAMESPACE.IM + "ClinicalEntry", NAMESPACE.IM + "Observation").contains(typeOf)) {
              addConceptSets(match.getWhere(), "clinical", valueSets);
            }
            if (typeOf.contains("Medication")) {
              addConceptSets(match.getWhere(), "medication", valueSets);

            }
          }
          else addValueSets(match, valueSets);
        }
      }
    }


  }


  private void addEventGroup(Query datasetQuery, String setType,String valueSet) throws JsonProcessingException {
    String columnGroupIri= setType.equals("clinical") ?NAMESPACE.IM+"ColumnGroup_clinical_details"
    :NAMESPACE.IM+"ColumnGroup_medication_details";
    TTEntity columnEntity= entityService.getPartialEntity(columnGroupIri,Set.of(IM.DEFINITION.toString()));
    Query columnReturns= columnEntity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
    Query columnGroup = new Query();
    columnGroup.setTypeOf(setType.equals("clinical")? NAMESPACE.IM+"ClinicalEntry" : NAMESPACE.IM+"MedicationRequest" );
    columnGroup.setReturn(columnReturns.getReturn());

    columnGroup.setName("Latest "+ setType+" details");
    Where where= new Where();
    where.setIri(NAMESPACE.IM+"concept");
    where.addIs(Node.iri(valueSet).setMemberOf(true));
    columnGroup.setWhere(where);
    setOptional(columnGroup);
    if (columnGroup.getOrderBy()==null) {
      columnGroup.orderBy(o -> o
        .addPartition(new IriLD().setIri(NAMESPACE.IM + "patient").setName("patient"))
        .addPartition(new IriLD().setIri(NAMESPACE.IM + "concept").setName("concept"))
        .addProperty(new OrderDirection()
          .setDirection(Order.descending)
          .setIri(NAMESPACE.IM + "effectiveDate"))
        .setLimit(1));
    }
    datasetQuery.addColumnGroup(columnGroup);
  }


  private void addConceptSets(Where where,String setType, Map<String,Set<String>> conceptSets) {
      if (where.getIri() != null) {
        if (where.getIri().contains("concept")) {
          if (where.getIs() != null) {
            conceptSets.computeIfAbsent(setType, k -> new HashSet<>());
            conceptSets.get(setType).addAll(where.getIs().stream().map(Node::getIri).collect(Collectors.toSet()));
          }
        }
      }
      for (List<Where> wheres : Arrays.asList(where.getAnd(), where.getOr())) {
        if (wheres!=null){
          for (Where subWhere:wheres){
            addConceptSets(subWhere,setType,conceptSets);
          }
        }
      }
    }



  public void importIndicators(String indicatorFile,String indicatorFolderIri) throws Exception {
    TTFilerFactory.setBulk(false);
    int order=0;
    String parentIndicator=null;
    try (BufferedReader reader = new BufferedReader(new FileReader(indicatorFile))) {
      reader.readLine();
      String line = reader.readLine();
      while (line != null && !line.isEmpty()) {
        line = line.replace("\"", "");
        String[] fields = line.split("\t");
        if (fields.length > 1) {
          String inputType = fields[0];
          if (inputType.equals("I") || inputType.equals("S") || inputType.equals("R")) {
            String indicatorIri= (namespace + "I_" + (fields[1].replaceAll("[^a-zA-Z0-9._~:/?#\\[\\]@!$&'()*+,;=%-]", ""))).toLowerCase();
            String indicatorName = fields[7];
            String numeratorName= fields[8];
            if (numeratorName.isEmpty()) {
              line= reader.readLine();continue;
            }
            String denominatorName= fields[10];
            List<TTBundle> numeratorEntity= entityService.getEntityFromTerm(numeratorName, Set.of(namespace.toString(), NAMESPACE.QOF.toString()));
            if (numeratorEntity.isEmpty()){
              System.out.println("Numerator entity not found for indicator: "+indicatorName + " with numerator name: "+numeratorName);
              line=reader.readLine();
              continue;
            }
            String numeratorIri = numeratorEntity.getFirst().getEntity().getIri();
            List<TTBundle> denominatorEntity= entityService.getEntityFromTerm(denominatorName, Set.of(namespace.toString(), NAMESPACE.QOF.toString()));
            if (denominatorEntity.isEmpty()){
              System.out.println("Denominator entity not found for indicator: "+indicatorName + " with denominator name: "+denominatorName);
              line=reader.readLine();
              continue;
            }
            String denominatorIri = denominatorEntity.getFirst().getEntity().getIri();
            if (inputType.equals("I")) {
              order=0;
              parentIndicator=indicatorIri;
            }
            else order++;
            TTEntity indicator = new TTEntity();
            indicator.setIri(indicatorIri);
            indicator.setScheme(iri(namespace.toString()));
            indicator.setName(indicatorName);
            indicator.addType(iri(IM.INDICATOR));
            indicator.addObject(iri(IM.DENOMINATOR), iri(denominatorIri));
            indicator.set(iri(IM.NUMERATOR), iri(numeratorIri));
            if (order>0)
              indicator.set(iri(SHACL.ORDER), TTLiteral.literal(order));
            document.addEntity(indicator);
            if (inputType.equals("I")) {
              indicator.addObject(iri(IM.IS_CONTAINED_IN), iri(indicatorFolderIri));
            }
            else indicator.addObject(iri(IM.IS_SUBINDICATOR_OF), iri(parentIndicator));
          }
        }
        line = reader.readLine();
      }
    }
  }

  private void createColumnGroupEntities(){

  }



  private void setOptional(Query match) {
    Set<String> nodeRefs = new HashSet<>();
    setNodeRefList(match.getWhere(), nodeRefs);
    setOptionalPaths(match.getPath(), nodeRefs);
  }

  private void setOptionalPaths(List<Path> paths, Set<String> nodeRefs) {
    if (paths==null) return;
    for (Path path:paths) {
      if (!nodeRefs.contains(path.getNode()))
        path.setOptional(true);
      if (path.getPath()!=null) setOptionalPaths(path.getPath(),nodeRefs);
    }
  }

  private void setNodeRefList(Where where,Set<String> nodeRefs) {
    if (where.getNodeRef()!=null) nodeRefs.add(where.getNodeRef());
    for (List<Where> wheres : Arrays.asList(where.getAnd(),where.getOr())) {
      if (wheres!=null){
        for (Where subWhere:wheres){
          setNodeRefList(subWhere,nodeRefs);
        }
      }
    }
  }








  private boolean actionWhere(Where where) throws QueryException {
    if (where.getIri()!=null &&where.getIs()!=null) {
      Node first= where.getIs().getFirst();
      if (searchService.askQueryIM(isChild(first.getIri(),Set.of("http://snomed.info/sct#363787002","http://snomed.info/sct#71388002")))){
        return true;
      }
    }
    return false;
  }

  private void flattenMatches(Query match,List<Where> wheres) {
    if (match.getWhere() != null) {
      if (match.getWhere().getIri()!=null)
        wheres.add(match.getWhere());
      else for (Where subWhere : match.getWhere().getAnd()) {
        if (subWhere.getIri()!=null)
          wheres.add(subWhere);
      }
    }
  }



  private Map<Integer,List<Where>> getWhereClauses(Query match) throws QueryException {
    List<Where> wheres = new ArrayList<>();
    flattenMatches(match, wheres);
    int clauseIndex=0;
    Map<Integer, List<Where>> actionClauses = new HashMap<>();
    for (int whereIndex = 0; whereIndex < wheres.size(); whereIndex++) {
      Where where = wheres.get(whereIndex);
      if (actionWhere(where)) {
        clauseIndex++;
        actionClauses.put(clauseIndex, new ArrayList<>());
        actionClauses.get(clauseIndex).add(where);
        whereIndex= addDateValueClause(wheres, whereIndex, actionClauses.get(clauseIndex));
      }
    }
    return actionClauses;
  }

  private Integer addDateValueClause(List<Where> wheres, int startIndex, List<Where> clauseWheres) throws QueryException {
    for (int whereIndex=startIndex+1; whereIndex<wheres.size(); whereIndex++) {
      Where subWhere = wheres.get(whereIndex);
      if (subWhere.getIri()!=null && subWhere.getIri().contains("concept")) return whereIndex-1;
      if (subWhere.getIri() != null && subWhere.getIri().contains("effectiveDate")) {
        if (subWhere.getRange() == null && subWhere.getValue() == null&&subWhere.getCompare()==null) continue;
        clauseWheres.add(subWhere);
      }
      if (subWhere.getIri() != null && subWhere.getIri().contains("value")) {
        String value = subWhere.getValue();
        Range range = subWhere.getRange();
        if (value != null && range == null && !value.equals("0")) {
          clauseWheres.add(subWhere);
          return whereIndex;
        }
        else if (range != null) {
          clauseWheres.add(subWhere);
          return whereIndex;
        }
      }
    }
    return wheres.size();
  }


  private TTNode createScheduleNode(Value from) {
    TTNode scheduleNode = new TTNode();
    TTIriRef units=null;
    if (from.getUnits()!=null) units=from.getUnits();
    String value = from.getValue();
    if (value.equals("15")){
      value="1";
      units=iri(NAMESPACE.IM+"Years");
    }
    else if (value.equals("27")){
      value="2";
      units=iri(NAMESPACE.IM+"Years");
    }
    scheduleNode.set(NAMESPACE.IM + "value", TTLiteral.literal(value));
    if (units!=null) {
      scheduleNode.set(NAMESPACE.IM + "unit", units);
    }
    return scheduleNode;

  }





  private String getLabel(String longLabel){
    if (longLabel.contains("Patients with"))
      return longLabel.split("Patients with")[1];
    if (longLabel.contains(" - ")) return longLabel.split(" - ")[0];
    String[] words = longLabel.split("\\s+");
    if (words.length>8) {
      return String.join(" ", Arrays.copyOfRange(words, 0, 8));
    }
    else return String.join(" ", Arrays.copyOfRange(words, 0, words.length));
  }

  private TTEntity getEntityFromIri(String iri) throws JsonProcessingException {
    return entityService.getBundleByPredicateExclusions(iri, null).getEntity();
  }


  private QueryRequest isChild(String iri, Set<String> parents){
    Query query= new Query()
      .setParameter("$concept")
      .or(m->m
        .where(w->w
          .setIri(IM.IS_A.toString())
          .is(is->is
            .setParameter("$parents"))))
      .or(m->m
        .path(p->p
          .setIri(IM.HAS_MEMBER.toString())
          .setNode("member"))
        .where(w->w
          .setIri(IM.IS_A.toString())
          .is(is->is
            .setParameter("$parents"))));
    return createRequest(iri,parents,query);

  }

  private QueryRequest createRequest(String conceptIri, Set<String> parentIris, Query query) {
    Set<TTIriRef> parents;
    if (parentIris!=null)
      parents=parentIris.stream().map(TTIriRef::iri).collect(Collectors.toSet());
    else {
      parents = null;
    }
    QueryRequest request= new QueryRequest()
      .setQuery(query)
      .argument(a->a
        .setParameter("concept")
        .setValueIri(iri(conceptIri)));
    if (parents!=null) {
      request.argument(a->a
        .setParameter("parents")
        .setValueIriList(parents));
    }
    return request;
  }




}
