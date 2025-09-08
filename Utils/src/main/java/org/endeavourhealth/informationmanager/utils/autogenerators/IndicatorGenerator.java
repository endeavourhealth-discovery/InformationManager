package org.endeavourhealth.informationmanager.utils.autogenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.map.HashedMap;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.logic.reasoner.LogicOptimizer;
import org.endeavourhealth.imapi.logic.service.ConceptService;
import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.logic.service.QueryDescriptor;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.requests.QueryRequest;
import org.endeavourhealth.imapi.model.tripletree.TTBundle;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.Namespace;

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
	private final Map<String,String> indicators = new HashMap<>();
	private final Map<String,String> matchToActivity= new HashMap<>();




	public void generate(String folder,String mainFolder,String namespace) throws Exception {
		this.namespace=namespace;
		importIndicators(folder);
		try (TTManager manager = new TTManager() ) {
			TTDocument document = manager.createDocument();
			for (Map.Entry<String,String> entry : indicators.entrySet()) {
				String indicatorLabel = entry.getKey();
				String queryIri = entry.getValue();
				String indicatorIri = namespace + "Indicator-" + indicatorLabel.hashCode();
				TTEntity indicator = new TTEntity();
				indicator.setIri(indicatorIri);
				indicator.setName(indicatorLabel);
				indicator.addType(iri(IM.INDICATOR));
				indicator.set(iri(IM.HAS_QUERY), iri(queryIri));
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
			writer.write("Indicator/Activity\t\term");
			for (String indicatorLabel : unlabelledIndicators) {
				writer.write("I\t"+indicatorLabel+"\n");
			}
			for (String clauseLabel : unlabelledClauses) {
				writer.write("A\t"+clauseLabel+"\n");
			}
		}
	}



	private void configureKPI(TTEntity indicator,String queryIri) throws IOException, QueryException {
		System.out.println(indicator.getName());
		TTEntity queryEntity = getEntityFromIri(queryIri);
		configureIndicator(indicator,queryEntity,Bool.and);

	}

	private void configureIndicator(TTEntity indicatorEntity, TTEntity queryEntity, Bool operator) throws IOException, QueryException {
		if (indicatorMap.containsKey(queryEntity.getIri())) {
			indicatorMap.get(queryEntity.getIri());
			return;
		}
		String cohortName=queryEntity.getName();
		System.out.println(cohortName);
		Query query= queryEntity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
		query= descriptor.describeQuery(query,DisplayMode.LOGICAL);
		LogicOptimizer.optimizeQuery(query);
		boolean or= false;
		boolean indicator=false;
		configureMatch(indicatorEntity,query,queryEntity,Bool.and);
		indicatorMap.put(queryEntity.getIri(),indicator);
	}

	private void configureMatch(TTEntity indicatorEntity, Match match,TTEntity queryEntity,Bool operator) throws IOException, QueryException {
		boolean indicator=false;
		if (match.getIsCohort() != null) {
			TTIriRef cohortIri = match.getIsCohort();
			TTEntity cohortEntity = getEntityFromIri(cohortIri.getIri());
			TTEntity childEntity = createChildIndicator(cohortIri.getIri(),cohortEntity.getName(),indicatorEntity,operator);
			configureIndicator(childEntity,cohortEntity,operator==Bool.or ?Bool.or: Bool.and);
			return;
		}
		if (match.getAnd() != null) {
			int clauseIndex=0;
			for (Match subMatch : match.getAnd()) {
				clauseIndex++;
				if (clauseIndex>1 ||subMatch.getIsCohort()!=null) {
					configureMatch(indicatorEntity, subMatch, queryEntity, Bool.and);
				}
			}
		}
		else if (match.getOr() != null) {
			for (Match subMatch : match.getOr()) {
				configureMatch(indicatorEntity,subMatch,queryEntity,Bool.or);
			}
		}
		else {
			boolean actionNeeded= actionNeeded(match);
			if (actionNeeded){
				configureAcivity(indicatorEntity,match,queryEntity.getIri());
			}
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
			parentIndicator.addObject(iri(Namespace.IM+"must"), iri(indicator.getIri()));
		}
		else parentIndicator.addObject(iri(Namespace.IM+"alternative"), iri(indicator.getIri()));
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


	private List<Where> getWheres(Match match) throws QueryException {
		List<Where> wheres= new ArrayList<>();
		if (match.getWhere()!=null) {
			if (match.getWhere().getIri() != null) {
				wheres.add(match.getWhere());
			}
			if (match.getWhere().getAnd() != null) {
				wheres.addAll(match.getWhere().getAnd());
			}
		}
		if (match.getThen()!=null){
			wheres.addAll(getWheres(match.getThen()));
		}
		return wheres;
	}



	private void configureAcivity(TTEntity indicatorEntity, Match match, String queryIri) throws QueryException, JsonProcessingException {

		List<Where> wheres = getWheres(match);
		if (wheres.isEmpty()) return;
		String procedureName=null;
		String targetName=null;
		String dateRangeLabel=null;
		StringBuilder indicatorLabel = new StringBuilder();
		StringBuilder procedureWheres= new StringBuilder();
		StringBuilder targetWheres=new StringBuilder();
		for (Where where : wheres) {
					if (where.getIri() != null && where.getIri().equals(Namespace.IM + "concept") && where.getIs() != null) {
						procedureName= new ConceptService().getShortestTerm(where.getIs().getFirst().getIri());
						if (procedureName==null) procedureName=where.getValueLabel();
						procedureName = getLabel(procedureName);
						indicatorLabel.append(procedureName);
						procedureWheres.append(om.writeValueAsString(where.getIs()));
						targetWheres.append(om.writeValueAsString(where.getIs()));
					}
					else if (where.getIri() != null && where.getIri().contains("effectiveDate")) {
						String valueLabel=where.getValueLabel();
						if (valueLabel==null){
						valueLabel= where.getRelativeTo().getQualifier();
						}
						dateRangeLabel= where.getQualifier()+" "+valueLabel;
						procedureWheres.append(dateRangeLabel);
						indicatorLabel.append(" ").append(dateRangeLabel);
					}
					else if (where.getIri() != null && where.getIri().contains("value")) {
						if (where.getRange()!=null ||(!where.getValue().equals("0"))) {
							targetName = where.getQualifier() + " " + where.getValueLabel();
							targetWheres.append(targetName);
						}
				}
			}
		if (procedureName!=null) {
			String subIndicatorLabel= matchToActivity.get(procedureWheres);
			if (subIndicatorLabel==null) {
				unlabelledClauses.add(procedureWheres.toString());
				subIndicatorLabel=procedureWheres.toString();
			}
			String subIndicatorIri = namespace + "Procedure" + (om.writeValueAsString(procedureWheres).hashCode());
			TTEntity subIndicatorEntity=entities.get(subIndicatorIri);
			if (subIndicatorEntity==null){
				subIndicatorEntity = new TTEntity();
				entities.put(subIndicatorIri, subIndicatorEntity);
				subIndicatorEntity.setIri(subIndicatorIri);
				subIndicatorEntity.setName(subIndicatorLabel);
				subIndicatorEntity.addType(iri(IM.INDICATOR));
				indicatorEntity.addObject(iri(Namespace.IM+"caresubIndicator"), iri(subIndicatorEntity.getIri()));
			}
			String activityLabel= matchToActivity.get(procedureWheres);
				if (activityLabel==null) {
					unlabelledClauses.add(procedureWheres.toString());
					activityLabel=procedureWheres.toString();
				}
				String activityIri = namespace + "Procedure" + (om.writeValueAsString(procedureWheres).hashCode());
				TTEntity activityEntity= entities.get(activityIri);
				if (activityEntity==null){
					activityEntity = new TTEntity();
					entities.put(activityIri, activityEntity);
					activityEntity.setIri(activityIri);
					activityEntity.setName(activityLabel);
					activityEntity.addType(iri(IM.INDICATOR));
				}
				subIndicatorEntity.addObject(iri(Namespace.IM+"careActivity"), iri(activityEntity.getIri()));
				if (targetName!=null) {
					String targetLabel= matchToActivity.get(targetWheres.toString());
					if (targetLabel==null) {
						unlabelledClauses.add(procedureWheres.toString());
						targetLabel="Review of treatment";
					}
					String targetIri = namespace + "TargetActivity" + (targetLabel.hashCode());
					TTEntity targetEntity = entities.get(targetIri);
					if (targetEntity == null) {
						targetEntity = new TTEntity();
						entities.put(targetIri, targetEntity);
						targetEntity.setIri(targetIri);
						targetEntity.setName(targetLabel);
						targetEntity.addType(iri(IM.INDICATOR));
					}
					subIndicatorEntity.addObject(iri(Namespace.IM+"targetActivity"), iri(subIndicatorEntity.getIri()));
			}
		}
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
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				if (fields.length > 1) {
					String iOrA = fields[0];
					if (iOrA.equals("I")) {
						indicators.put(fields[2],fields[3]);
					}
					if (iOrA.equals("I")||iOrA.equals("S")) {
							String indicatorLabel = fields[2];
							String queryLabel = fields[3];
							List<TTBundle> entities= entityService.getEntityFromTerm(queryLabel,Set.of(namespace));
							if (entities.isEmpty())
								throw new Exception("Indicator not found: " + queryLabel);
							else {
								String queryIri = entities.get(0).getEntity().getIri();
								queryIriToIndicator.put(queryIri, indicatorLabel);
							}
					}
					if (iOrA.equals("A")) {
						if (!fields[4].equals(""))
							matchToActivity.put(fields[4], fields[2]);
					}
				}
				line = reader.readLine();
			}
		}
	}

}



