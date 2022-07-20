package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.sets.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;


import javax.swing.*;
import java.io.IOException;
import java.io.InvalidClassException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

public class EqdToTT {
	private static Map<String, String> reportNames;
	private static final Set<String> roles = new HashSet<>();
	private static final Set<TTIriRef> fieldGroups = new HashSet<>();
	private TTIriRef owner;
	private Properties dataMap;
	private Properties labels;
	private int varCounter;
	private String activeReport;
	private TTDocument document;
	private final String slash = "/";
	private TTIriRef fieldGroupFolder;
	private TTIriRef valueSetFolder;
	private Map<String, String> propertyVar;
	private final Map<String, Match> varMatch = new HashMap<>();
	private final Map<String, Set<TTIriRef>> valueMap = new HashMap<>();
	private final ImportMaps importMaps = new ImportMaps();


	private final Map<Object, Object> vocabMap = new HashMap<>();

	public void convertDoc(TTDocument document, TTIriRef mainFolder, TTIriRef fieldGroupFolder, TTIriRef valueSetFolder, EnquiryDocument eqd, TTIriRef owner, Properties dataMap,
												 Properties criteriaLabels) throws DataFormatException, IOException {
		this.owner = owner;
		this.dataMap = dataMap;
		this.document = document;
		this.labels = criteriaLabels;
		this.fieldGroupFolder = fieldGroupFolder;
		this.valueSetFolder = valueSetFolder;
		addReportNames(eqd);
		convertFolders(mainFolder, eqd);
		convertReports(eqd);
	}

	private void addReportNames(EnquiryDocument eqd) {
		if (reportNames == null)
			reportNames = new HashMap<>();
		for (EQDOCReport eqReport : Objects.requireNonNull(eqd.getReport())) {
			if (eqReport.getId() != null)
				reportNames.put(eqReport.getId(), eqReport.getName());
		}

	}

	private void convertReports(EnquiryDocument eqd) throws DataFormatException, IOException {
		for (EQDOCReport eqReport : Objects.requireNonNull(eqd.getReport())) {
			if (eqReport.getId() == null)
				throw new DataFormatException("No report id");
			if (eqReport.getName() == null)
				throw new DataFormatException("No report name");

			TTEntity qry = convertReport(eqReport);
			document.addEntity(qry);
			setProvenance(qry.getIri(), "CEG");
		}
	}

	private void convertFolders(TTIriRef mainFolder, EnquiryDocument eqd) throws DataFormatException {
		List<EQDOCFolder> eqFolders = eqd.getReportFolder();
		if (eqFolders != null) {
			for (EQDOCFolder eqFolder : eqFolders) {
				if (eqFolder.getId() == null)
					throw new DataFormatException("No folder id");
				if (eqFolder.getName() == null)
					throw new DataFormatException("No folder name");
				String iri = "urn:uuid:" + eqFolder.getId();
				TTEntity folder = new TTEntity()
					.setIri(iri)
					.addType(IM.FOLDER)
					.setName(eqFolder.getName())
					.set(IM.IS_CONTAINED_IN, mainFolder);
				document.addEntity(folder);
				if (eqFolder.getAuthor() != null && eqFolder.getAuthor().getAuthorName() != null)
					setProvenance(iri, eqFolder.getAuthor().getAuthorName());
			}
		}
	}

	private void setProvenance(String iri, String authorName) {
		ProvActivity activity = new ProvActivity()
			.setIri("urn:uuid:" + UUID.randomUUID())
			.setActivityType(IM.PROV_CREATION)
			.setEffectiveDate(LocalDateTime.now().toString());
		document.addEntity(activity);
		if (authorName != null) {
			String uir = getPerson(authorName);
			ProvAgent agent = new ProvAgent()
				.setPersonInRole(TTIriRef.iri(uir))
				.setParticipationType(IM.AUTHOR_ROLE);
			agent.setName(authorName);
			agent.setIri(uir.replace("uir.", "agent."));
			activity.addAgent(TTIriRef.iri(agent.getIri()))
				.setTargetEntity(TTIriRef.iri(iri));
			if (!roles.contains(agent.getIri())) {
				document.addEntity(agent);
				roles.add(agent.getIri());
			}

		}
	}

	private String getPerson(String name) {
		StringBuilder uri = new StringBuilder();
		name.chars().forEach(c -> {
			if (Character.isLetterOrDigit(c))
				uri.append(Character.toString(c));
		});
		String root = owner.getIri();
		root = root.substring(0, root.lastIndexOf("#"));
		return root.replace("org.", "uir.") + "/personrole#" +
			uri;
	}

	public TTEntity convertReport(EQDOCReport eqReport) throws DataFormatException, IOException {

		setVocabMaps();
		activeReport = eqReport.getId();
		TTEntity entity = new TTEntity();
		entity.setIri("urn:uuid:" + eqReport.getId());
		entity.setName(eqReport.getName());
		entity.setDescription(eqReport.getDescription().replace("\n", "<p>"));
		if (eqReport.getFolder() != null)
			entity.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri("urn:uuid:" + eqReport.getFolder()));
		if (eqReport.getCreationTime() != null)
			setProvenance(entity.getIri(), null);

		if (eqReport.getPopulation() != null) {
			entity.addType(IM.QUERY);
			Query profile = new Query();
			profile.setName(entity.getName());
			profile.setMainEntity(TTIriRef.iri(IM.NAMESPACE + "Person"));
			profile.setDescription(entity.getDescription());
			profile.setIri(entity.getIri());
			profile.setSelect(new Select());
			Select select = profile.getSelect();
			select.setEntityType(TTIriRef.iri(IM.NAMESPACE + "Person").setName("Person"));
			Match main = new Match();
			select.addMatch(main);

			if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
				setFrom(main, TTIriRef.iri(IM.NAMESPACE + "Q_RegisteredGMS").setName("Registered with GP for GMS services on the reference date"));
			}
			else {
				if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
					String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
					setFrom(main, TTIriRef.iri("urn:uuid:" + id).setName(reportNames.get(id)));
				}
			}

			convertPopulation(eqReport.getPopulation(), select);
			upgradeQuery(profile);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json = objectMapper.writeValueAsString(profile);
			entity.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));
		}
		if (eqReport.getListReport() != null) {
			entity.addType(IM.QUERY);
			Query set = new Query();
			set.setName(entity.getName());
			set.setDescription(entity.getDescription());
			set.setIri(entity.getIri());
			set.setMainEntity(TTIriRef.iri(IM.NAMESPACE + "Person"));
			Select select = new Select();
			set.setSelect(select);
			select.setEntityType(TTIriRef.iri(IM.NAMESPACE + "Person").setName("Person"));
			Match main = new Match();
			select.addMatch(main);
			String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
			setFrom(main, TTIriRef.iri("urn:uuid:" + id).setName(reportNames.get(id)));
			convertListReport(eqReport.getListReport(), set);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json = objectMapper.writeValueAsString(set);
			entity.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));
		}
		if (eqReport.getAuditReport() != null) {
			entity.addType(IM.QUERY);
			Query set = new Query();
			set.setName(entity.getName());
			set.setDescription(entity.getDescription());
			set.setIri(entity.getIri());
			set.setMainEntity(TTIriRef.iri(IM.NAMESPACE + "Person"));
			convertAuditReport(eqReport.getAuditReport(), set);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json = objectMapper.writeValueAsString(set);
			entity.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));

		}
		return entity;
	}

	private void convertAuditReport(EQDOCAuditReport auditReport, Query set) throws IOException {
		Select select = new Select();
		set.setSelect(select);
		Match mainFilter = new Match();
		select.setEntityType(TTIriRef.iri(IM.NAMESPACE + "Person").setName("Person"));
		select.addMatch(mainFilter);
		mainFilter.setEntityType(getIri(IM.NAMESPACE + "Person"));
		for (String popId : auditReport.getPopulation()) {
			mainFilter.addEntityInSet(TTIriRef.iri("urn:uuid:" + popId).setName(reportNames.get(popId)));
		}
		PropertySelect property = new PropertySelect();
		select.addProperty(property);
		property.
			setIri(IM.NAMESPACE + "id")
			.setAlias("id");

		EQDOCAggregateReport agg = auditReport.getCustomAggregate();
		String eqTable = agg.getLogicalTable();
		for (EQDOCAggregateGroup group : agg.getGroup()) {
			for (String eqColum : group.getGroupingColumn()) {
				String predicate = (String) dataMap.get(eqTable + slash + eqColum);
				String[] path = predicate.split("/");
				property = new PropertySelect();
				select.addProperty(property);
				for (int part = 0; part < path.length; part = part + 2) {
					property.setIri(getIri(IM.NAMESPACE + path[part]));
					if (part + 2 < path.length) {
						select = new Select();
						property.setSelect(select);
						property = new PropertySelect();
						select.addProperty(property);
					}
				}
			}
		}
	}

	private void convertListReport(EQDOCListReport eqListReport, Query shape) throws DataFormatException, IOException {
		for (EQDOCListReport.ColumnGroups eqColGroups : eqListReport.getColumnGroups()) {
			EQDOCListColumnGroup eqColGroup = eqColGroups.getColumnGroup();
			Select selectGroup = new Select();
			shape.addSubselect(selectGroup);
			convertListGroup(eqColGroup, selectGroup);
		}
	}


	private void convertListGroup(EQDOCListColumnGroup eqColGroup, Select group) throws DataFormatException, IOException {
		propertyVar = new HashMap<>();
		group.setName(eqColGroup.getDisplayName());
		String eqTable = eqColGroup.getLogicalTableName();

		if (eqColGroup.getCriteria() == null) {
			convertPatientColumns(eqColGroup, eqTable, group);
		} else {
			convertEventColumns(eqColGroup, eqTable, group);
		}
		storeGroup(eqColGroup, group);
	}

	private void convertPatientColumns(EQDOCListColumnGroup eqColGroup, String eqTable, Select mainSelect) throws IOException {
		EQDOCListColumns eqCols = eqColGroup.getColumnar();
		for (EQDOCListColumn eqCol : eqCols.getListColumn()) {
			String eqDisplay = eqCol.getDisplayName();
			String eqColumn= String.join("/",eqCol.getColumn());
				String predicatePath = dataMap.getProperty(eqTable + slash + eqColumn);
				String[] path = predicatePath.split("/");
						PropertySelect property = new PropertySelect();
						mainSelect.addProperty(property);
						property.setName(eqDisplay);
						property.setAlias(CaseUtils.toCamelCase(eqColumn, false).replaceAll("[^a-zA-Z0-9_]", ""));
						property.setIri(getIri(IM.NAMESPACE + path[path.length-1]));
		}

	}

	private void convertEventColumns(EQDOCListColumnGroup eqColGroup, String eqTable, Select mainSelect) throws DataFormatException, IOException {
		Match mainMatch = new Match();
		mainSelect.addMatch(mainMatch);
		mainSelect.addPathTo(new ConceptRef().setIri(getIri(IM.NAMESPACE+"isSubjectOf").getIri()));
		convertCriteria(eqColGroup.getCriteria(), mainMatch);
		EQDOCListColumns eqCols = eqColGroup.getColumnar();
		for (EQDOCListColumn eqCol : eqCols.getListColumn()) {
			PropertySelect property = new PropertySelect();
			mainSelect.addProperty(property);
			String eqDisplay = eqCol.getDisplayName();
			property.setName(eqDisplay);
			String eqColumn= String.join("/",eqCol.getColumn());
			String predicatePath = dataMap.getProperty(eqTable + slash + eqColumn);
			String[] path = predicatePath.split("/");
			for (int part = 0; part < path.length; part = part + 2) {
					if (part + 2 < path.length) {
						property.setIri(getIri(IM.NAMESPACE+path[part]).getIri());
						property.setSelect(new Select());
						Select select= new Select();
						property.setSelect(select);
						property= new PropertySelect();
						select.addProperty(property);
					}
					else {
						property.setIri(getIri(IM.NAMESPACE + path[part]));
						property.setAlias(CaseUtils.toCamelCase(eqColumn, false).replaceAll("[^a-zA-Z0-9_]", ""));
					}

				}
		}

	}


	private void storeGroup(EQDOCListColumnGroup eqColGroup, Select group) throws JsonProcessingException {
		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);

		TTIriRef iri = TTIriRef.iri("urn:uuid:" + eqColGroup.getId());
		if (!fieldGroups.contains(iri)) {
			TTEntity fieldGroup = new TTEntity()
				.addType(IM.FIELD_GROUP)
				.setIri(iri.getIri())
				.setName(eqColGroup.getDisplayName());
			document.addEntity(fieldGroup);
			fieldGroup.addObject(IM.IS_CONTAINED_IN, fieldGroupFolder);
			fieldGroup.addObject(IM.QUERY_DEFINITION, TTLiteral.literal(objectMapper.writeValueAsString(group)));
			fieldGroups.add(iri);
		}

	}


	private TTIriRef getIri(String token) throws IOException {
		if (token.equals("label"))
			return RDFS.LABEL.setName("label");
		else {
			if (!token.contains(":")) {
				TTIriRef iri = TTIriRef.iri(IM.NAMESPACE + token);
				iri.setName(importMaps.getCoreName(IM.NAMESPACE + token));
				return iri;
			} else {
				TTIriRef iri = TTIriRef.iri(token);
				iri.setName(importMaps.getCoreName(token));
				return iri;
			}
		}
	}


	private void convertPopulation(EQDOCPopulation population,Select mainSelect) throws DataFormatException, IOException {
		Match orMatch = null;
		propertyVar = new HashMap<>();
		for (EQDOCCriteriaGroup eqGroup : population.getCriteriaGroup()) {
			VocRuleAction ifTrue = eqGroup.getActionIfTrue();
			VocRuleAction ifFalse = eqGroup.getActionIfFalse();

			if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.NEXT) {
				if (orMatch==null) {
					orMatch = new Match();
					mainSelect.addMatch(orMatch);
				}
				addToOr(eqGroup, orMatch);

			} else if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.REJECT) {
				if (orMatch != null) {
					addToOr(eqGroup, orMatch);
					orMatch = null;
				} else {
					addAnd(eqGroup, mainSelect,null);
				}
			} else if (ifTrue == VocRuleAction.NEXT && ifFalse == VocRuleAction.REJECT) {
				if (orMatch != null) {
					addToOr(eqGroup, orMatch);
					orMatch = null;
				} else {
					addAnd(eqGroup, mainSelect, null);
				}
			} else if (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.NEXT) {
				if (orMatch!=null){
					addToOr(eqGroup,orMatch);
				}
				else {
					orMatch = new Match();
					mainSelect.addMatch(orMatch);
					addToOr(eqGroup,orMatch);
				}
			} else if (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.SELECT) {
				if (orMatch != null) {
					addToOr(eqGroup, orMatch);
					orMatch = null;
				} else {
					addAnd(eqGroup, mainSelect,null);
				}

			} else
				throw new DataFormatException("unrecognised action rule combination : " + activeReport);

		}
	}

	private void addAnd(EQDOCCriteriaGroup eqGroup, Select mainSelect, Match topMatch) throws DataFormatException, IOException {
		VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
		boolean negation = eqGroup.getActionIfTrue() == VocRuleAction.REJECT;
		if (memberOp == VocMemberOperator.AND) {
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match andMatch = new Match();
				if (topMatch == null)
					mainSelect.addMatch(andMatch);
				else
					topMatch.addAnd(andMatch);
				if (negation)
					andMatch.setNotExist(true);
				convertCriteria(eqCriteria, andMatch);
			}
		}
		else {
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match orFilter = new Match();
				if (topMatch==null){
					topMatch= new Match();
					mainSelect.addMatch(topMatch);
				}
				topMatch.addOr(orFilter);
				convertCriteria(eqCriteria, orFilter);
			}
		}

	}

	private void addToOr(EQDOCCriteriaGroup eqGroup, Match orFilter) throws DataFormatException, IOException {

		VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
		boolean negation = eqGroup.getActionIfTrue() == VocRuleAction.REJECT;
			if (memberOp == VocMemberOperator.OR){
			  for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
					Match subOrFilter= new Match();
					orFilter.addOr(subOrFilter);
				  if (negation)
				 		subOrFilter.setNotExist(true);
				convertCriteria(eqCriteria, subOrFilter);
				}
			}
			else if (eqGroup.getDefinition().getCriteria().size()==1){
				Match subOrFilter= new Match();
				orFilter.addOr(subOrFilter);
				if (negation)
					subOrFilter.setNotExist(true);
				convertCriteria(eqGroup.getDefinition().getCriteria().get(0), subOrFilter);
			}
			else {
				Match subOrFilter= new Match();
				orFilter.addOr(subOrFilter);
				if (negation)
					subOrFilter.setNotExist(true);
				addAnd(eqGroup,null,subOrFilter);
			}

	}


	private void convertCriteria(EQDOCCriteria eqCriteria,
															 Match match) throws DataFormatException, IOException {

		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			match.addEntityInSet(TTIriRef.iri("urn:uuid:" + srch.getReportGuid())
				.setName(reportNames.get(srch.getReportGuid())));
		} else {
			if (eqCriteria.getCriterion().getId() != null) {
				match.setIri("urn:uuid:" + eqCriteria.getCriterion().getId());
			}
			convertCriterion(eqCriteria.getCriterion(), match);
		}

	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Match match) throws DataFormatException, IOException {
		String eqTable = eqCriterion.getTable();
		String entityType = (String) dataMap.get(eqTable);

		if (!eqTable.equals("PATIENTS")) {
			match.addPathTo(new ConceptRef(getIri(IM.NAMESPACE + "isSubjectOf")));
			match.setEntityType(getIri(IM.NAMESPACE + entityType));
		}


		if (labels.get(eqCriterion.getId()) != null) {
			match.setName(labels.get(eqCriterion.getId()).toString());
		}
		if (eqCriterion.isNegation()) {
			match.setNotExist(true);
		}
		if (eqCriterion.getDescription() != null)
			match.setDescription(eqCriterion.getDescription());
		if (eqCriterion.getLinkedCriterion() != null) {
			convertLinkedCriterion(eqTable, eqCriterion,
				match);
		} else if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			convertColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), match);
			setRestriction(eqCriterion, match);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				for (EQDOCColumnValue cvs : eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue()) {
					PropertyValue pv = new PropertyValue();
					match.addTestProperty(pv);
					setMainCriterion(eqTable, cvs, match, pv);
				}
			}
		} else
			convertColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), match);

	}


	private void convertColumns(EQDOCFilterAttribute filterAttribute, String eqTable, Match match) throws DataFormatException, IOException {

			for (EQDOCColumnValue cv : filterAttribute.getColumnValue()) {
				PropertyValue pv= new PropertyValue();
				match.addProperty(pv);
				setMainCriterion(eqTable, cv, match,pv);
		}

	}

	private String addDateFilter(Match match) throws DataFormatException, IOException {
		varCounter++;
		match.property(pv-> pv
			.setIri(IM.NAMESPACE+"effectiveDate")
			.setName("date")
			.setAlias("effectiveDate"+varCounter));
		return "effectiveDate"+ varCounter;
	}

	private void setRestriction(EQDOCCriterion eqCriterion, Match match) throws IOException {
		String eqTable = eqCriterion.getTable();
		varCounter++;
		String linkColumn = eqCriterion.getFilterAttribute().getRestriction()
			.getColumnOrder().getColumns().get(0).getColumn().get(0);
		OrderLimit sort = new OrderLimit();
		match.setOrderLimit(sort);
		String predicatePath = (String) dataMap.get(eqTable + slash + linkColumn);
		String orderField = getIri(IM.NAMESPACE + predicatePath).getIri();
		sort.orderBy(ob -> ob
			.setIri(orderField)
			.setAlias(predicatePath+varCounter))
			.setCount(1);
		EQDOCFilterRestriction restrict = eqCriterion.getFilterAttribute().getRestriction();
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC)
			sort.setDirection(Order.ASCENDING);
		else
			sort.setDirection(Order.DESCENDING);
	}


	private void setMainCriterion(String eqTable, EQDOCColumnValue cv, Match match,PropertyValue pv) throws DataFormatException, IOException {
		String eqColumn= String.join("/",cv.getColumn());
		setPropertyValue(cv, eqTable, eqColumn, match,pv);
	}


	private void setPropertyValue(EQDOCColumnValue cv, String eqTable, String eqColumn,
																Match match,PropertyValue pv) throws DataFormatException, IOException {
		String predPath = getMap(eqTable + slash + eqColumn);
		if (predPath.split("/").length>1){
			String[] pathsTo= predPath.split("/");
			for (int i=0; i<pathsTo.length-1; i=i+2){
				pv.addPathTo(new ConceptRef(getIri(IM.NAMESPACE+ pathsTo[i])));
			}
		}
		String predicate = predPath.substring(predPath.lastIndexOf("/") + 1);
		TTIriRef propertyIri= getIri(IM.NAMESPACE + predicate);
		pv.setIri(propertyIri.getIri());
		pv.setName(propertyIri.getName());
		VocColumnValueInNotIn in = cv.getInNotIn();
		varCounter++;
		pv.setAlias(predicate + varCounter);
		propertyVar.put(predPath, predicate + varCounter);
		varMatch.put(predicate + varCounter, match);
		boolean notIn = (in == VocColumnValueInNotIn.NOTIN);
		if (!cv.getValueSet().isEmpty()) {
			for (EQDOCValueSet vs : cv.getValueSet()) {
				if (vs.getAllValues() != null) {
					pv.setNotInSet(getExceptionSet(vs.getAllValues()));
				} else {
					if (!notIn) {
						if (isValueSet(vs)) {
							pv.addInSet(getValueSet(vs));
						} else
							pv.setIsConcept(getInlineValues(vs));
					}
					else {
						if (isValueSet(vs)) {
							pv.addNotInSet(getValueSet(vs));
						}
						else
							 pv.setIsNotConcept(getInlineValues(vs));
					}
				}
			}
		} else if (!CollectionUtils.isEmpty(cv.getLibraryItem())) {
			for (String vset : cv.getLibraryItem()) {
				String vsetName = "Unknown code set";
				if (labels.get(vset) != null)
					vsetName = (String) labels.get(vset);
				TTIriRef iri = TTIriRef.iri("urn:uuid:" + vset).setName(vsetName);
				if (!notIn)
					pv.addInSet(iri);
				else
					pv.addNotInSet(iri);
				storeLibraryItem(iri);
			}
		} else if (cv.getRangeValue() != null) {
			setRangeValue(cv.getRangeValue(), pv);
		}

	}


	private void setRangeValue(EQDOCRangeValue rv, PropertyValue pv) throws DataFormatException {

		EQDOCRangeFrom rFrom = rv.getRangeFrom();
		EQDOCRangeTo rTo = rv.getRangeTo();
		if (rFrom != null) {
			if (rTo == null) {
				setCompareFrom(pv, rFrom);
			} else {
				setRangeCompare(pv, rFrom, rTo);
			}
		}
		if (rTo != null && rFrom == null) {
			setCompareTo(pv, rTo);
		}

	}

	private void setCompareFrom(PropertyValue pv, EQDOCRangeFrom rFrom) throws DataFormatException {
		Comparison comp;
		if (rFrom.getOperator() != null)
			comp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rFrom.getValue().getValue();
		String units = null;
		if (rFrom.getValue().getUnit() != null)
			units = rFrom.getValue().getUnit().value();
		VocRelation relation = VocRelation.ABSOLUTE;
		if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
			relation = VocRelation.RELATIVE;
		}
		setCompare(pv, comp, value, units, relation, true);
	}

	private void setCompare(PropertyValue pv, Comparison comp, String value, String units, VocRelation relation,
													boolean from) throws DataFormatException {
		if (relation == VocRelation.RELATIVE) {
			String first = from ? "$this" : "$referenceDate";
			String second = from ? "$referenceDate" : "$this";
			Function function = getTimeDiff(units, first, second);
			if (from) {
				comp = reverseComp(comp);
				value = String.valueOf(-Integer.parseInt(value));
			}
			pv.setValue(new Compare()
				.setComparison(comp)
				.setValueData(value));
			pv.setFunction(function);
		} else {
			pv.setValue(new Compare()
				.setComparison(comp)
				.setValueData(value));
			if (pv.getIri().equals(IM.NAMESPACE + "age")) {
				if (units == null)
					throw new DataFormatException("missing units from age");
				pv.addArgument(new Argument()
						.setParameter("units")
						.setValueData(units));
			}
		}
	}

	private Comparison reverseComp(Comparison comp) {
		if (comp == Comparison.GREATER_THAN_OR_EQUAL)
			return Comparison.LESS_THAN_OR_EQUAL;
		else if (comp == Comparison.GREATER_THAN)
			return Comparison.LESS_THAN;
		else if (comp == Comparison.LESS_THAN)
			return Comparison.GREATER_THAN;
		else if (comp == Comparison.LESS_THAN_OR_EQUAL)
			return Comparison.GREATER_THAN_OR_EQUAL;
		return comp;
	}


	private void setCompareTo(PropertyValue pv, EQDOCRangeTo rTo) throws DataFormatException {
		Comparison comp;
		if (rTo.getOperator() != null)
			comp = (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rTo.getValue().getValue();
		String units = null;
		if (rTo.getValue().getUnit() != null)
			units = rTo.getValue().getUnit().value();
		VocRelation relation = VocRelation.ABSOLUTE;
		if (rTo.getValue().getRelation() != null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
			relation = VocRelation.RELATIVE;
		}
		setCompare(pv, comp, value, units, relation, false);
	}


	private Function getTimeDiff(String units, String first, String second) {
		Function function = new Function().setIri(TTIriRef.iri(IM.NAMESPACE + "TimeDifference")
			.setName("Time Difference"));
		function.addArgument(new Argument().setParameter("units").setValueData(units));
		function.addArgument(new Argument().setParameter("firstDate").setValueVariable(first));
		function.addArgument(new Argument().setParameter("secondDate").setValueVariable(second));
		return function;
	}

	private void setRangeCompare(PropertyValue pv, EQDOCRangeFrom rFrom, EQDOCRangeTo rTo) throws DataFormatException {
		Range range = new Range();
		pv.setInRange(range);
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.EQUAL;
		String fromValue = rFrom.getValue().getValue();
		String units = null;
		if (rFrom.getValue().getUnit() != null)
			units = rFrom.getValue().getUnit().value();
		if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
			Function function = getTimeDiff(units, "$this", "$referenceDate");
			range.setFrom(new Compare()
				.setComparison(fromComp)
				.setValueData(fromValue));
			pv.setFunction(function);
		} else {
			range.setFrom(new Compare()
				.setComparison(fromComp)
				.setValueData(fromValue));
			if (pv.getIri().equals(IM.NAMESPACE + "age")) {
				if (units == null)
					throw new DataFormatException("missing units from age");
				pv.addArgument(new Argument()
						.setParameter("units")
						.setValueData(units));
			}
		}

		Comparison toComp;
		if (rTo.getOperator() != null)
			toComp = (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp = Comparison.EQUAL;
		String toValue = rTo.getValue().getValue();
		units = null;
		if (rTo.getValue().getUnit() != null)
			units = rTo.getValue().getUnit().value();
		if (rTo.getValue().getRelation() != null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
			Function function = getTimeDiff(units, "$referenceDate", "$this");
			range.setTo(new Compare()
				.setComparison(toComp)
				.setValueData(toValue));
			pv.setFunction(function);
		} else {
			range.setTo(new Compare()
				.setComparison(toComp)
				.setValueData(toValue));
		}

	}


	private String getMap(String from) throws DataFormatException {
		Object target = dataMap.get(from);
		if (target == null)
			throw new DataFormatException("unknown map : " + from);
		return (String) target;
	}

	private String getDateAlias(Match match,String iri){
		for (PropertyValue pv:match.getProperty()){
			if (pv.getIri().equals(iri))
				return pv.getAlias();
		}
		if (match.getTestProperty()!=null) {
			for (PropertyValue pv : match.getTestProperty()) {
				if (pv.getIri().equals(iri))
					return pv.getAlias();
			}
		}
		if (match.getOrderLimit()!=null){
			if (match.getOrderLimit().getOrderBy().getIri().equals(iri))
			 return match.getOrderLimit().getOrderBy().getAlias();
		}
		return null;
	}

	private void convertLinkedCriterion(String eqTable, EQDOCCriterion eqCriterion, Match match) throws DataFormatException, IOException {
		Match targetMatch = new Match();
		match.addAnd(targetMatch);
		convertColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), targetMatch);
		if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			setRestriction(eqCriterion, targetMatch);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				for (EQDOCColumnValue cvs : eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue()) {
					PropertyValue pv = new PropertyValue();
					targetMatch.addTestProperty(pv);
					setMainCriterion(eqTable, cvs, match, pv);
				}
			}
		}
		String dateAlias=null;
		if (eqCriterion.getLinkedCriterion().getRelationship().getParentColumn().contains("DATE")){
			dateAlias= getDateAlias(targetMatch,IM.NAMESPACE+"effectiveDate");
			if (dateAlias==null)
				dateAlias= addDateFilter(targetMatch);
		}
		else
			throw new DataFormatException("Only date link fields supported at the moment");

		Match linkMatch = new Match();
		match.addAnd(linkMatch);
		EQDOCCriterion eqTargetCriterion = eqCriterion.getLinkedCriterion().getCriterion();
		convertColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), linkMatch);
		if (eqTargetCriterion.getFilterAttribute().getRestriction() != null) {
			setRestriction(eqCriterion, linkMatch);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				for (EQDOCColumnValue cvs : eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue()) {
					PropertyValue pv = new PropertyValue();
					linkMatch.addTestProperty(pv);
					setMainCriterion(eqTable, cvs, linkMatch, pv);
				}
			}
		}

		PropertyValue testPv= new PropertyValue();
		linkMatch.addProperty(testPv);
		testPv.setIri(IM.NAMESPACE+"effectiveDate");
		testPv.setName("date");
		varCounter++;
		testPv.setAlias("effectiveDate"+varCounter);
		EQDOCRelationship eqRel = eqCriterion.getLinkedCriterion().getRelationship();
		String units = eqRel.getRangeValue().getRangeFrom().getValue().getUnit().value();
		VocRangeFromOperator eqOp = eqRel.getRangeValue().getRangeFrom().getOperator();
		String value = eqRel.getRangeValue().getRangeFrom().getValue().getValue();
		Function function = getTimeDiff(units,dateAlias, testPv.getAlias());
		testPv.within(w -> w
			.setFunction(function)
			.setCompare(new Compare()
				.setComparison((Comparison) vocabMap.get(eqOp))
				.setValueData(value)));

	}


	/*
	private TTIriRef getExceptionSet(EQDOCException set) throws DataFormatException, IOException {
		TTEntity valueSet = new TTEntity();
		String iri = "urn:uuid:" + UUID.randomUUID();
		valueSet.setIri(iri);
		valueSet.addType(IM.CONCEPT_SET);
		valueSet.addObject(IM.USED_IN,TTIriRef.iri(reportMap.get(activeReport).getIri()));
		document.addEntity(valueSet);

		VocCodeSystemEx scheme= set.getCodeSystem();
		for (EQDOCExceptionValue ev:set.getValues()){
			Set<TTIriRef> values= getValue(scheme,ev.getValue(),ev.getDisplayName(),ev.getLegacyValue());
			if (values!=null) {
				TTNode ors = new TTNode();
				valueSet.addObject(IM.DEFINITION, ors);
				values.forEach(v -> ors.addObject(SHACL.OR, v));
			}
		}
		return TTIriRef.iri(iri);
	}

	private TTEntity getDuplicateSet(TTEntity candidate){
		for (TTEntity test: valueSets){
			if (TTCompare.equals(candidate,test))
				return test;
		}
		return null;
	}

	 */

	private List<ConceptRef> getExceptionSet(EQDOCException set) throws DataFormatException, IOException {
		List<ConceptRef> valueSet = new ArrayList<>();
		VocCodeSystemEx scheme = set.getCodeSystem();
		for (EQDOCExceptionValue ev : set.getValues()) {
			Set<ConceptRef> values = getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
			if (values != null) {
				valueSet.addAll(new ArrayList<>(values));
			} else
				System.err.println("Missing exception sets\t" + ev.getValue() + "\t " + ev.getDisplayName());
		}

		return valueSet;
	}

	private boolean isValueSet(EQDOCValueSet vs) {
		VocCodeSystemEx scheme = vs.getCodeSystem();
		if (scheme == VocCodeSystemEx.EMISINTERNAL) {
			return false;
		}
		if (vs.getValues() != null)
			return vs.getValues().size() >2;
		return true;
	}

	private List<ConceptRef> getInlineValues(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<ConceptRef> setContent = new ArrayList<>();
		VocCodeSystemEx scheme = vs.getCodeSystem();
		for (EQDOCValueSetValue ev : vs.getValues()) {
			Set<ConceptRef> concepts = getValue(scheme, ev);
			if (concepts != null) {
				for (ConceptRef iri : concepts) {
					ConceptRef conRef = new ConceptRef(iri.getIri(), iri.getName());
					conRef.setIncludeSubtypes(true);
					setContent.add(conRef);
				}
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}
		return setContent;
	}


	private TTIriRef getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTIriRef> setContent = new ArrayList<>();
		StringBuilder vsetName = new StringBuilder();
		VocCodeSystemEx scheme = vs.getCodeSystem();
		if (labels.get(vs.getId())!=null){
			vsetName.append((String) labels.get(vs.getId()));
		}
		if (vs.getDescription() != null)
			vsetName = new StringBuilder(vs.getDescription());
		int i = 0;
		for (EQDOCValueSetValue ev : vs.getValues()) {
			i++;
			Set<ConceptRef> concepts = getValue(scheme, ev);
			if (concepts != null) {
				setContent.addAll(new ArrayList<>(concepts));
				if (vsetName.toString().isEmpty()){
					if (i==3)
						vsetName.append("...more");
					if (i==2)
						vsetName.append(", ");
					if (i<3){
						if (ev.getDisplayName() != null) {
							vsetName.append(ev.getDisplayName());
						}
						else
							vsetName.append(concepts.stream().findFirst().get().getName());
					}
				}
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}


		storeValueSet(vs, setContent, vsetName.toString());
		return TTIriRef.iri("urn:uuid:" + vs.getId()).setName(vsetName.toString());
	}

	private void storeLibraryItem(TTIriRef iri) {
		if (!CEGImporter.valueSets.containsKey(iri)) {
			TTEntity conceptSet = new TTEntity()
				.setIri(iri.getIri())
				.addType(IM.CONCEPT_SET)
				.setName(iri.getName());
			conceptSet.addObject(IM.IS_CONTAINED_IN, valueSetFolder);
			conceptSet.addObject(IM.USED_IN, TTIriRef.iri("urn:uuid:" + activeReport));
			document.addEntity(conceptSet);
			CEGImporter.valueSets.put(iri, conceptSet);
		}
	}

	private void storeValueSet(EQDOCValueSet vs, List<TTIriRef> valueSet, String vSetName) {
		if (vs.getId() != null) {
			TTIriRef iri = TTIriRef.iri("urn:uuid:" + vs.getId()).setName(vSetName);
			if (!CEGImporter.valueSets.containsKey(iri)) {
				TTEntity conceptSet = new TTEntity()
					.setIri(iri.getIri())
					.addType(IM.CONCEPT_SET)
					.setName(vSetName);
				conceptSet.addObject(IM.IS_CONTAINED_IN, valueSetFolder);
				TTNode ors = new TTNode();
				conceptSet.addObject(IM.DEFINITION, ors);
				for (TTIriRef member : valueSet)
					ors.addObject(SHACL.OR, member);
				document.addEntity(conceptSet);
				CEGImporter.valueSets.put(iri, conceptSet);
			}
			CEGImporter.valueSets.get(iri).addObject(IM.USED_IN, TTIriRef.iri("urn:uuid:" + activeReport));
		}
	}

	private Set<ConceptRef> getValue(VocCodeSystemEx scheme, EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
	}

	private Set<ConceptRef> getValue(VocCodeSystemEx scheme, String originalCode,
																	 String originalTerm, String legacyCode) throws DataFormatException, IOException {
		if (scheme == VocCodeSystemEx.EMISINTERNAL) {
			String key = "EMISINTERNAL/" + originalCode;
			Object mapValue = dataMap.get(key);
			if (mapValue != null) {
				TTIriRef iri = getIri(mapValue.toString());
				String name = importMaps.getCoreName(iri.getIri());
				if (name != null)
					iri.setName(name);
				Set<TTIriRef> result = new HashSet<>();
				result.add(iri);
				return result.stream().map(ConceptRef::new).collect(Collectors.toSet());
			} else
				throw new DataFormatException("unmapped emis internal code : " + key);
		} else if (scheme == VocCodeSystemEx.SNOMED_CONCEPT || scheme.value().contains("SCT")) {
			List<String> schemes = new ArrayList<>();
			schemes.add(SNOMED.NAMESPACE);
			schemes.add(IM.CODE_SCHEME_EMIS.getIri());
			Set<TTIriRef> snomed = valueMap.get(originalCode);
			if (snomed == null) {

				snomed = getCoreFromCode(originalCode, schemes);
				if (snomed == null)
					if (legacyCode != null)
						snomed = getCoreFromCode(legacyCode, schemes);
				if (snomed == null)
					if (originalTerm != null)
						snomed = getCoreFromLegacyTerm(originalTerm);
				if (snomed == null)
					snomed = getCoreFromCodeId(originalCode);
				if (snomed == null)
					snomed = getLegacyFromTermCode(originalCode);

				if (snomed != null)
					valueMap.put(originalCode, snomed);
			}
			if (snomed != null)
				return snomed.stream().map(ConceptRef::new).collect(Collectors.toSet());
			else
				return null;
		} else
			throw new DataFormatException("code scheme not recognised : " + scheme.value());

	}


	private Set<TTIriRef> getCoreFromCodeId(String originalCode) {

		try {
			return importMaps.getCoreFromCodeId(originalCode, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e) {
			System.err.println("unable to retrieve iri from term code " + e.getMessage());
			e.printStackTrace();
			return null;
		}
	}


	private Set<TTIriRef> getLegacyFromTermCode(String originalCode) {
		try {
			return importMaps.getLegacyFromTermCode(originalCode, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e) {
			System.err.println("unable to retrieve iri from term code " + e.getMessage());
			return null;
		}
	}

	private Set<TTIriRef> getCoreFromLegacyTerm(String originalTerm) {
		try {
			if (originalTerm.contains("s disease of lymph nodes of head, face AND/OR neck"))
				System.out.println("!!");

			return importMaps.getCoreFromLegacyTerm(originalTerm, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e) {
			System.err.println("unable to retrieve iri from term " + e.getMessage());
			return null;
		}
	}

	private Set<TTIriRef> getCoreFromCode(String originalCode, List<String> schemes) {
		return importMaps.getCoreFromCode(originalCode, schemes);
	}


	private void setFrom(Match match, TTIriRef parent) {
		match.addEntityInSet(parent);
	}


	private void setVocabMaps() {
		vocabMap.put(VocRangeFromOperator.GTEQ, Comparison.GREATER_THAN_OR_EQUAL);
		vocabMap.put(VocRangeFromOperator.GT, Comparison.GREATER_THAN);
		vocabMap.put(VocRangeToOperator.LT, Comparison.LESS_THAN);
		vocabMap.put(VocRangeToOperator.LTEQ, Comparison.LESS_THAN_OR_EQUAL);
		vocabMap.put(VocOrderDirection.DESC, SortOrder.DESCENDING);
		vocabMap.put(VocOrderDirection.ASC, SortOrder.ASCENDING);
	}

	public void upgradeQuery(Query query) {
		for (int i=0; i<query.getSelect().getMatch().size(); i++){
			upgradeMatch(query.getSelect().getMatch().get(i));
		}
	}

	private void upgradeMatch(Match match) {
		if (match.getTestProperty() != null) {
			if (match.getOrderLimit().getOrderBy().getIri().equals(IM.NAMESPACE + "effectiveDate")) {
				if (match.getTestProperty().get(0).getInSet() != null) {
					notFollowedBy(match);
				}
				else if (match.getTestProperty().get(0).getIsConcept() != null) {
					notFollowedBy(match);
				}
			}
		}
		else if (match.getOr()!=null){
			for (Match or:match.getOr()){
				upgradeMatch(or);
			}
		}
	}



	private void notFollowedBy(Match match) {
		List<ConceptRef> mixed;
		if (match.getOrderLimit() != null) {
			if (match.getOrderLimit().getDirection() == Order.DESCENDING) {
				if (match.getProperty().get(0).getInSet() != null) {
					if (match.getProperty().get(0).getInSet().size() > 1) {
						mixed = match.getProperty().get(0).getInSet();
						if (match.getTestProperty().get(0).getInSet()!=null){
						if (match.getTestProperty().get(0).getInSet().size() == 1) {
							List<ConceptRef> main = match.getTestProperty().get(0).getInSet();
							reformAsFollowed(match, main, mixed);
						}
						}
					}
				}
				else {
					if (match.getProperty().get(0).getIsConcept() != null) {
						if (match.getProperty().get(0).getIsConcept().size() > 1) {
							mixed = match.getProperty().get(0).getIsConcept();
							if (match.getTestProperty().get(0).getIsConcept()!=null){
								if (match.getTestProperty().get(0).getIsConcept().size() == 1) {
									List<ConceptRef> main = match.getTestProperty().get(0).getIsConcept();
									reformAsFollowed(match, main, mixed);
								}
							}
						}
					}
				}
			}
		}
	}


	private void reformAsFollowed(Match match, List<ConceptRef> main, List<ConceptRef> original) {
		Match mainMatch= new Match();
		match.addAnd(mainMatch);
		mainMatch.setProperty(match.getProperty());
		mainMatch.setOrderLimit(match.getOrderLimit());
			if (original.contains(main.get(0))){
				List<ConceptRef> notFollowed= original;
				notFollowed.remove(main.get(0));
				if (match.getProperty().get(0).getInSet()!=null)
					match.getProperty().get(0).setInSet(main);
				else
					match.getProperty().get(0).setIsConcept(main);
				Match nFM= new Match();
				match.addAnd(nFM);
				nFM.setNotExist(true);
				varCounter++;
				String date= "effectiveDate"+varCounter;
				PropertyValue cm= match.getTestProperty().get(0);
				nFM.addProperty(cm);
				if (cm.getInSet()!=null)
						cm.setInSet(notFollowed);
				else
					cm.setIsConcept(notFollowed);
				match.setTestProperty(null);
				match.setProperty(null);
				match.setOrderLimit(null);
				nFM.property(dm->dm
						.setIri(new ConceptRef(IM.NAMESPACE+"effectiveDate"))
						.setAlias(date)
						.setName("date")
						.value(v->v
							.setComparison(Comparison.GREATER_THAN)
							.setValueVariable(mainMatch.getOrderLimit().getOrderBy().getAlias())));
			}
	}


}
