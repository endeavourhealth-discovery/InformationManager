package org.endeavourhealth.informationmanager.utils.autogenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.map.HashedMap;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.reasoner.LogicOptimizer;
import org.endeavourhealth.imapi.logic.service.ConceptService;
import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.logic.service.QueryDescriptor;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.requests.QueryRequest;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.imapi.vocabulary.SHACL;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


public class IndicatorGenerator {
	public ObjectMapper om= new ObjectMapper();
	private final EntityService entityService = new EntityService();
	private final QueryDescriptor descriptor = new QueryDescriptor();
	private String namespace;
	private final SearchService searchService = new SearchService();
	private final Map<String,Boolean> indicatorMap = new HashedMap();
	private final Map<String,TTEntity> entities = new HashMap<>();
	private final Set<String> unlabelledClauses= new HashSet<>();
	private final Set<String> unlabelledIndicators= new HashSet<>();
	private final Map<String,String> queryIriToIndicator = new HashMap<>();
	private final Map<String,String> indicatorToQueryIri = new HashMap<>();
	private final List<String> indicators= new ArrayList<>();
	private final Map<String,Integer> indicatorOrder= new HashMap<>();
	private TTEntity pathway;
	private String indicatorFolder;
	private String pathwayFolder;
	private TTDocument document;
	private final Map<String,String> matchLabel= new HashMap<>();
	private String queryName;



	public void generate(String folder,String mainFolder,String pathwayFolder,String namespace) throws Exception {
		this.namespace=namespace;
		this.indicatorFolder =mainFolder;
		this.pathwayFolder=pathwayFolder;
		try (TTManager manager = new TTManager() ) {
			document = manager.createDocument();
			importIndicators(folder);
			for (String indicatorLabel: indicators) {
				String queryIri = indicatorToQueryIri.get(indicatorLabel);
				String indicatorIri = namespace + "Indicator-" + indicatorLabel.hashCode();
				TTEntity indicator = new TTEntity();
				indicator.setIri(indicatorIri);
				indicator.setName(indicatorLabel);
				indicator.addType(iri(IM.INDICATOR));
				indicator.set(iri(IM.HAS_QUERY), iri(queryIri));
				indicator.set(iri(SHACL.ORDER), TTLiteral.literal(indicatorOrder.get(indicatorLabel)));
				indicator.addObject(iri(IM.IS_CONTAINED_IN), iri(mainFolder));

				entities.put(indicatorIri, indicator);
				configureKPI(indicator, queryIri);
			}
			for (Map.Entry<String,TTEntity> entry : entities.entrySet()) {
				document.addEntity(entry.getValue());
			}
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
				filer.fileDocument(document);
				}
			}
		try (FileWriter writer = new FileWriter(folder + "UnlabelledClauses.txt")) {
			for (String indicatorLabel : unlabelledIndicators) {
				writer.write(indicatorLabel+"\n");
			}
			for (String clauseLabel : unlabelledClauses) {
				writer.write(clauseLabel+"\n");
			}
		}
	}



	private void configureKPI(TTEntity indicator,String queryIri) throws Exception {
		System.out.println(indicator.getName());
		TTEntity queryEntity = getEntityFromIri(queryIri);
		configureIndicator(indicator,queryEntity,Bool.and);

	}

	private void configureIndicator(TTEntity indicatorEntity, TTEntity queryEntity, Bool operator) throws Exception {
		if (indicatorMap.containsKey(queryEntity.getIri())) {
			indicatorMap.get(queryEntity.getIri());
			return;
		}
		queryName= queryEntity.getName();
		String cohortName=queryEntity.getName();
		System.out.println(cohortName);
		Query query= queryEntity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
		query= descriptor.describeQuery(query,DisplayMode.LOGICAL);
		LogicOptimizer.optimizeQuery(query);
		boolean or= false;
		boolean indicator=false;
		if (query.getAnd() != null) {
			int clauseIndex=0;
			for (Match subMatch : query.getAnd()) {
				configureMatch(indicatorEntity, subMatch, queryEntity, Bool.and);
			}
		}
		else if (query.getOr() != null) {
			for (Match subMatch : query.getOr()) {
				configureMatch(indicatorEntity,subMatch,queryEntity,Bool.or);
			}
		}

		indicatorMap.put(queryEntity.getIri(),indicator);
	}

	private void configureMatch(TTEntity indicatorEntity, Match match,TTEntity queryEntity,Bool operator) throws Exception {
		if (match.getIsCohort() != null) {
			TTIriRef cohortIri = match.getIsCohort();
			TTEntity cohortEntity = getEntityFromIri(cohortIri.getIri());
			TTEntity childEntity = createChildIndicator(cohortIri.getIri(),cohortEntity.getName(),indicatorEntity,operator);
			configureIndicator(childEntity,cohortEntity,operator==Bool.or ?Bool.or: Bool.and);
			return;
		}
		if (match.getAnd() != null) {

			for (Match subMatch : match.getAnd()) {
					configureMatch(indicatorEntity, subMatch, queryEntity, Bool.and);
			}
		}
		else if (match.getOr() != null) {
			for (Match subMatch : match.getOr()) {
				configureMatch(indicatorEntity,subMatch,queryEntity,Bool.or);
			}
		}
		else {
				configureActivity(match);
		}
	}



	private TTEntity createChildIndicator(String queryIri,String label,TTEntity parentIndicator,Bool operator) throws IOException, QueryException {
		TTEntity indicator = new TTEntity();
		String indicatorLabel=queryIriToIndicator.get(queryIri);
		if (indicatorLabel==null) {
			indicatorLabel=getLabel(label);
			unlabelledIndicators.add(label);
		}
		indicator.setIri(namespace+"Indicator"+indicatorLabel.hashCode());
		indicator.setName(getLabel(indicatorLabel));
		indicator.addType(iri(IM.INDICATOR));
		indicator.set(iri(IM.HAS_QUERY), iri(queryIri));
		if (operator==Bool.and) {
			parentIndicator.addObject(iri(Namespace.IM+"andIndicator"), iri(indicator.getIri()));
		}
		else parentIndicator.addObject(iri(Namespace.IM+"orIndicator"), iri(indicator.getIri()));
		entities.put(indicator.getIri(),indicator);
		return indicator;
	}




	private boolean actionNeeded(Match match) throws QueryException {
		if (match.getWhere()!=null){
			boolean actionNeeded= actionNeeded(match.getWhere());
			if (actionNeeded) return true;
		}
		if (match.getThen()!=null){
			return actionNeeded(match.getThen());
		}
		else return false;
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

	private boolean actionNeeded(Where where) throws QueryException {
		if (where.getIri()!=null &&where.getIs()!=null) {
			Node first= where.getIs().getFirst();
			if(searchService.askQueryIM(isChild(first.getIri(),Set.of("http://snomed.info/sct#363787002","http://snomed.info/sct#71388002")))){
				return true;
			}
		}
		for (List<Where> wheres : Arrays.asList(where.getAnd(),where.getOr())) {
			if (wheres != null) {
				for (Where subWhere : wheres) {
					boolean actionNeeded = actionNeeded(subWhere);
					if (actionNeeded) return true;
				}
			}
		}
		return false;
	}

	private void flattenMatches(Match match,List<Where> wheres) {
		if (match.getWhere() != null) {
			if (match.getWhere().getIri()!=null)
				wheres.add(match.getWhere());
			else for (Where subWhere : match.getWhere().getAnd()) {
				if (subWhere.getIri()!=null)
					wheres.add(subWhere);
			}
		}
		if (match.getThen() != null) {
			flattenMatches(match.getThen(),wheres);
		}
	}



	private Map<Integer,List<Where>> getWhereClauses(Match match) throws QueryException {
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
				if (subWhere.getRange() == null && subWhere.getValue() == null&&subWhere.getFunction()==null) continue;
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



	private void configureActivity(Match match) throws Exception {
		Map<Integer,List<Where>> actionClauses = getWhereClauses(match);
		if (actionClauses.isEmpty()) return;
		for (Integer clauseIndex : actionClauses.keySet()) {
		String targetName=null;
		String dateRangeLabel=null;
		String activityLabel=null;
		Where procedureWhere = null;
		Where dateWhere=null;
		List<Where> wheres = actionClauses.get(clauseIndex);
		for (Where where : wheres) {
					if (where.getIri() != null && where.getIri().equals(Namespace.IM + "concept") && where.getIs() != null) {
						activityLabel= new ConceptService().getShortestTerm(where.getIs().getFirst().getIri());
						if (activityLabel==null) activityLabel=where.getValueLabel();
						procedureWhere = where;
					}
					else if (where.getIri() != null && where.getIri().contains("effectiveDate")) {
						dateWhere= where;
						String valueLabel=where.getValueLabel();
						if (valueLabel==null){
						valueLabel= where.getRelativeTo().getQualifier();
						}
						dateRangeLabel= where.getQualifier()+" "+valueLabel;
					}
					else if (where.getIri() != null && where.getIri().contains("value")) {
						if (where.getRange()!=null ||(!where.getValue().equals("0"))) {
							targetName = where.getQualifier() + " " + where.getValueLabel();
						}
				}
			}
		if (activityLabel!=null) {
			String careActivityLabel = matchLabel.get(activityLabel);
			if (careActivityLabel == null) {
				unlabelledClauses.add(activityLabel);
				careActivityLabel = activityLabel;
			}
			String careActivityIri = namespace + "CareActivity" + (om.writeValueAsString(careActivityLabel).hashCode());
			TTEntity careActivityEntity = entities.get(careActivityIri);
			if (careActivityEntity == null) {
				careActivityEntity = new TTEntity();
				entities.put(careActivityIri, careActivityEntity);
				createSchedule(careActivityEntity, procedureWhere, dateWhere);
				careActivityEntity.setIri(careActivityIri);
				careActivityEntity.setName(careActivityLabel);
				careActivityEntity.addType(iri(IM.CARE_ACTIVITY));

				pathway.addObject(iri(Namespace.IM + "careActivity"), iri(careActivityEntity.getIri()));
			}
			if (targetName != null) {
				String targetLabel = matchLabel.get(targetName);
				if (targetLabel == null) {
					unlabelledClauses.add(targetName);
					targetLabel = targetName;
				}
				String targetIri = namespace + "CareTarget" + (targetLabel.hashCode());
				TTEntity targetEntity = entities.get(targetIri);
				if (targetEntity == null) {
					targetEntity = new TTEntity();
					entities.put(targetIri, targetEntity);
					targetEntity.setIri(targetIri);
					targetEntity.setName(targetLabel);
					targetEntity.addType(iri(IM.CARE_TARGET));
				}
				careActivityEntity.addObject(iri(Namespace.IM + "careTarget"), iri(targetEntity.getIri()));
			}
		}


		}

	}

	private void createSchedule(TTEntity careActivityEntity, Where procedureWhere,Where dateWhere) throws Exception {
		if (procedureWhere == null) return;
		String procedureIri = procedureWhere.getIs().getFirst().getIri();
		careActivityEntity.set(Namespace.IM + "procedure", iri(procedureIri));
		if (dateWhere!=null) {
			if (dateWhere.getRange() != null) {
				if (dateWhere.getRange() != null) {
					Value from = dateWhere.getRange().getFrom();
					TTNode scheduleNode = createScheduleNode(from);
					careActivityEntity.set(Namespace.IM + "schedule", scheduleNode);
				}
			}
			if (dateWhere.getValue() != null && dateWhere.getValue().startsWith("-")) {
				String value = dateWhere.getValue().substring(1);
				throw new Exception("Negative dates not supported");
			}
		}
	}

	private TTNode createScheduleNode(Value from) {
		TTNode scheduleNode = new TTNode();
		TTIriRef units=null;
		if (from.getUnits()!=null) units=from.getUnits();
		String value = from.getValue();
		if (value.equals("15")){
			value="1";
			units=iri(Namespace.IM+"Years");
		}
		else if (value.equals("27")){
			value="2";
			units=iri(Namespace.IM+"Years");
		}
		scheduleNode.set(Namespace.IM + "value", TTLiteral.literal(value));
		if (units!=null) {
			scheduleNode.set(Namespace.IM + "unit", units);
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

	private TTEntity getEntityFromIri(String iri) {
		return entityService.getBundleByPredicateExclusions(iri, null).getEntity();
	}


	private QueryRequest isChild(String iri,Set<String> parents){
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
				.setVariable("member"))
				.where(w->w
					.setIri(IM.IS_A.toString())
					.is(is->is
						.setParameter("$parents"))));
		return createRequest(iri,parents,query);

	}

	private QueryRequest createRequest(String conceptIri, Set<String> parentIris, Query query) {
		Set<TTIriRef> parents;
		if (parentIris!=null)
			parents=parentIris.stream().map(TTIriRef::iri).collect(java.util.stream.Collectors.toSet());
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

	public void importIndicators(String folder) throws Exception {
		TTFilerFactory.setBulk(false);
		try (BufferedReader reader = new BufferedReader(new FileReader(folder + "Indicator-query.txt"))) {
			reader.readLine();
			int order=0;
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				line= line.replace("\"","");
				String[] fields = line.split("\t");
				if (fields.length > 1) {
					String inputType = fields[0];
					if (inputType.equals("P")){
						createPathway(fields[2]);
					}
					if (inputType.equals("A")) {
						matchLabel.put(fields[4],fields[1]);
					}
					else if (inputType.equals("I")||inputType.equals("S")) {
							String indicatorLabel = fields[2];
							String queryLabel = fields[3].replace("\"","");
							List<TTBundle> entities= entityService.getEntityFromTerm(queryLabel,Set.of(namespace,Namespace.QOF.toString()));
							if (entities.isEmpty())
								throw new Exception("Indicator not found: " + queryLabel);
							else {
								String queryIri = entities.get(0).getEntity().getIri();
								if (inputType.equals("I")) {
									indicators.add(fields[2]);
									order++;
									indicatorOrder.put(fields[2],order);
								}
								indicatorToQueryIri.put(fields[2], queryIri);
								queryIriToIndicator.put(queryIri, indicatorLabel);
							}
					}

				}
				line = reader.readLine();
			}
		}
	}

	private void createPathway(String field) {
		pathway = new TTEntity();
		pathway.setIri(namespace+field.hashCode())
			.setName(field)
			.addType(iri(Namespace.IM+"CarePathway"))
			.setScheme(Namespace.SMARTLIFE.asIri())
			.addObject(iri(IM.IS_CONTAINED_IN),iri(pathwayFolder));
		document.addEntity(pathway);
	}

}



