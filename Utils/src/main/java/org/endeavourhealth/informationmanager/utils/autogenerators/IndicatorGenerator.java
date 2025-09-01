package org.endeavourhealth.informationmanager.utils.autogenerators;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.map.HashedMap;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.reasoner.LogicOptimizer;
import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.logic.service.QueryDescriptor;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.customexceptions.OpenSearchException;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.requests.QueryRequest;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.imapi.vocabulary.RDFS;

import java.io.IOException;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


public class IndicatorGenerator {
	public ObjectMapper om= new ObjectMapper();
	private final EntityService entityService = new EntityService();
	private final QueryDescriptor descriptor = new QueryDescriptor();
	private TTDocument document;
	private String namespace;
	private final SearchService searchService = new SearchService();
	private final Map<String,Set<String>> indicatorTree = new HashMap<>();
	private final Map<String,Set<Match>> activityTree = new HashMap<>();
	private final Map<String,Set<String>> indicatorActivityTree = new HashMap<>();
	private final Map<String,Boolean> indicatorMap = new HashedMap();
	private final Map<String,TTEntity> entities = new HashMap<>();



	public void generate() throws IOException, QueryException, TTFilerException,OpenSearchException {

		List<String> folders = entityService.getChildIris(Namespace.IM + "Indicators");
		try (TTManager manager = new TTManager() ) {
			document = manager.createDocument();
			scanFolders(folders);
			for (TTEntity indicator : entities.values()) {
				document.addEntity(indicator);
			}
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
				filer.fileDocument(document);
				}
			}
	}

	private void scanFolders(List<String> folders) throws IOException, QueryException, OpenSearchException {
		for (String folderIri : folders) {
			namespace= folderIri.substring(0,folderIri.lastIndexOf("#")+1);
			TTEntity folder = getEntityFromIri(folderIri);
			if (folder.isType(iri(IM.FOLDER))) {
				List<String> subFolders = entityService.getChildIris(folderIri);
				scanFolders(subFolders);
			}
			else if (folder.isType(iri(IM.INDICATOR))){
				configureKpi(folder);
				generateIndicators(folder.get(iri(IM.HAS_QUERY)).asIriRef().getIri(),folder.getIri());
			}
		}

	}

	private void configureKpi(TTEntity kpi) throws IOException, QueryException {
		System.out.println(kpi.getName());

		TTIriRef queryIri= kpi.get(iri(IM.HAS_QUERY)).asIriRef();
		TTEntity queryEntity = getEntityFromIri(queryIri.getIri());
		configureIndicator(queryEntity);

	}

	private boolean configureIndicator(TTEntity queryEntity) throws IOException, QueryException {
		if (indicatorMap.containsKey(queryEntity.getIri())) {
			return indicatorMap.get(queryEntity.getIri());
		}
		String cohortName=queryEntity.getName();
		System.out.println(cohortName);
		Query query= queryEntity.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
		query= descriptor.describeQuery(query,DisplayMode.LOGICAL);
		LogicOptimizer.optimizeQuery(query);
		boolean or= false;
		boolean indicator=false;
		indicator= configureMatch(query,queryEntity);
		indicatorMap.put(queryEntity.getIri(),indicator);
		return indicator;
	}

	private boolean configureMatch(Match match,TTEntity parentEntity) throws IOException, QueryException {
		boolean indicator=false;
		if (match.getIsCohort() != null) {
			TTIriRef cohortIri = match.getIsCohort();
			TTEntity cohortEntity = getEntityFromIri(cohortIri.getIri());
			if (configureIndicator(cohortEntity)){
				indicatorTree.computeIfAbsent(parentEntity.getIri(), k->new HashSet<>()).add(cohortEntity.getIri());
				return true;
			}
		}
		if (match.getAnd() != null) {
			for (Match subMatch : match.getAnd()) {
				if (configureMatch(subMatch,parentEntity)) {
					indicator=true;
				}
			}
		}
		else if (match.getOr() != null) {
			for (Match subMatch : match.getOr()) {
				if (configureMatch(subMatch,parentEntity)) {
					indicator=true;
				}
			}
		}
		else {
			boolean actionNeeded= actionNeeded(match);
			if (actionNeeded){
				configureRetainAs(match,parentEntity);
				indicator=true;
			}
		}
		return indicator;
	}


	private void configureRetainAs(Match match,TTEntity parentEntity) {
			activityTree.computeIfAbsent(parentEntity.getIri(), k->new HashSet<>()).add(match);
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


	private void generateIndicators(String parentIri,String parentIndicatorIri) throws QueryException, OpenSearchException, JsonProcessingException {
		for (String childIri : indicatorTree.get(parentIri)) {
			TTEntity queryEntity = getEntityFromIri(childIri);
			TTEntity indicator = new TTEntity();
			String lname = queryEntity.getIri().substring(queryEntity.getIri().lastIndexOf("#") + 1);
			indicator.setIri(namespace + "Indicator-" + lname);
			indicator.setName("Indicator - " + queryEntity.getName());
			indicator.addType(iri(IM.INDICATOR));
			indicator.set(iri(IM.HAS_QUERY), iri(childIri));
			indicator.addObject(iri(IM.IS_CHILD_OF), iri(parentIndicatorIri));
			document.addEntity(indicator);
			if (indicatorTree.containsKey(childIri)) {
				generateIndicators(childIri,indicator.getIri());
			}
			if (activityTree.containsKey(childIri)){
				for (Match match: activityTree.get(childIri)){
					setActivityIndicator(match,childIri);
				}
			}
		}
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



	private void setActivityIndicator(Match match, String queryIri) throws QueryException, JsonProcessingException {
		String indicatorIri=namespace+"Indicator-"+queryIri.substring(queryIri.lastIndexOf("#")+1);
		List<Where> wheres = getWheres(match);
		if (wheres.isEmpty()) return;
		String procedureName=null;
		String targetName=null;
		String dateRangeLabel=null;
		StringBuilder indicatorLabel = new StringBuilder();
		for (Where where : wheres) {
					if (where.getIri() != null && where.getIri().equals(Namespace.IM + "concept") && where.getIs() != null) {
						procedureName = getLabel(where.getValueLabel());
						indicatorLabel.append(procedureName);
					}
					else if (where.getIri() != null && where.getIri().contains("effectiveDate")) {
						String valueLabel=where.getValueLabel();
						if (valueLabel==null){
						valueLabel= where.getRelativeTo().getQualifier();
						}
						dateRangeLabel= where.getQualifier()+" "+valueLabel;
						indicatorLabel.append(" ").append(dateRangeLabel);
					}
					else if (where.getIri() != null && where.getIri().contains("value")) {
						if (where.getRange()!=null ||(!where.getValue().equals("0"))) {
							targetName = where.getQualifier() + " " + where.getValueLabel();

						}
				}
			}
			String subindicatorIri = namespace + ("Indicator"+(om.writeValueAsString(wheres).hashCode()));
			TTEntity subIndicatorEntity = entities.get(subindicatorIri);
				if (subIndicatorEntity == null) {
				subIndicatorEntity = new TTEntity();
				entities.put(subindicatorIri, subIndicatorEntity);
				subIndicatorEntity.setIri(subindicatorIri);
				subIndicatorEntity.setName(indicatorLabel.toString());
				subIndicatorEntity.addType(iri(IM.INDICATOR));
				subIndicatorEntity.addObject(iri(IM.IS_CHILD_OF), iri(indicatorIri));
			}
			if (procedureName!=null) {
				String activityIri = namespace + "Procedure" + (om.writeValueAsString(wheres).hashCode());
				if (!entities.containsKey(activityIri)) {
					TTEntity activityEntity = new TTEntity();
					entities.put(activityIri, activityEntity);
					activityEntity.setIri(activityIri);
					activityEntity.addType(iri(IM.INDICATOR));
					activityEntity.addObject(iri(IM.IS_CHILD_OF), iri(subIndicatorEntity.getIri()));
					activityEntity.setName("Procedure - " + procedureName);
				}
			}
			if (targetName!=null){
				String targetIri= namespace+"Target"+(om.writeValueAsString(wheres).hashCode());
				TTEntity targetEntity= entities.get(targetIri);
				if (targetEntity==null){
					targetEntity= new TTEntity();
					entities.put(targetIri,targetEntity);
					targetEntity.setIri(targetIri);
					targetEntity.addType(iri(IM.INDICATOR));
					targetEntity.addObject(iri(IM.IS_CHILD_OF), iri(subIndicatorEntity.getIri()));
					targetEntity.setName("Target - "+procedureName+" "+targetName);
				}
				String activityIri= namespace+"Review"+(om.writeValueAsString(wheres).hashCode());
				if (!entities.containsKey(activityIri)){
					TTEntity activityEntity= new TTEntity();
					entities.put(activityIri,activityEntity);
					activityEntity.setIri(activityIri);
					activityEntity.addType(iri(IM.INDICATOR));
					activityEntity.addObject(iri(IM.IS_CHILD_OF), iri(targetEntity.getIri()));
					activityEntity.setName("Medication/Condition review");
				}
		}
	}

	private String getLabel(String longLabel){
		String[] words = longLabel.split("\\s+");
		if (words.length>8) {
			return String.join(" ", Arrays.copyOfRange(words, 0, 8));
		}
		else return String.join(" ", Arrays.copyOfRange(words, 0, words.length));
	}

	private TTEntity getEntityFromIri(String iri) {
		return entityService.getBundleByPredicateExclusions(iri, null).getEntity();
	}

	private QueryRequest getActivityName(String iri){
		Query query= new Query()
			.or(m->m
				.instanceOf(ins->ins.setParameter("$concept")))
			.or(m ->m
				.where(w->w
				.setIri(IM.HAS_MEMBER.toString())
					.setInverse(true)
					.is(is->is
					.setParameter("$concept"))))
				.return_(r->r
					.property(p->p
						.setIri(RDFS.LABEL.toString())));
		return createRequest(iri,null,query);
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

}



