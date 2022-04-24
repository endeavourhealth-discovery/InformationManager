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
import java.util.zip.DataFormatException;

public class EqdToTT {
	private static  Map<String,String> reportNames;
	private static final Map<TTIriRef,TTEntity> valueSets = new HashMap<>();
	private static final Set<String> roles= new HashSet<>();
	private static final Set<TTIriRef> fieldGroups = new HashSet<>();
	private TTIriRef owner;
	private Properties dataMap;
	private Properties labels;
	private int varCounter;
	private int tableNumber;
	private String activeReport;
	private TTDocument document;
	private final String slash = "/";
	private TTIriRef fieldGroupFolder;
	private TTIriRef valueSetFolder;
	private boolean needsDob;
	private String dobVar;
	private Map<String,String> propertyVar;
	private final Map<String,Match> varMatch = new HashMap<>();
	private final Map<String,Set<TTIriRef>> valueMap= new HashMap<>();
	private final ImportMaps importMaps = new ImportMaps();


	String dateMatch;
	private final Map<Object, Object> vocabMap = new HashMap<>();

	public void convertDoc(TTDocument document, TTIriRef mainFolder,TTIriRef fieldGroupFolder, TTIriRef valueSetFolder, EnquiryDocument eqd, TTIriRef owner, Properties dataMap,
						   Properties criteriaLabels) throws DataFormatException, IOException {
		this.owner = owner;
		this.dataMap = dataMap;
		this.document= document;
		this.labels= criteriaLabels;
		this.fieldGroupFolder= fieldGroupFolder;
		this.valueSetFolder= valueSetFolder;
		addReportNames(eqd);
		convertFolders(mainFolder,eqd);
		convertReports(eqd);
	}

	private void addReportNames(EnquiryDocument eqd) {
		if (reportNames==null)
			reportNames= new HashMap<>();
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

				TTEntity qry= convertReport(eqReport);
				document.addEntity(qry);
				setProvenance(qry.getIri(),"CEG");
		}
	}

	private void convertFolders(TTIriRef mainFolder,EnquiryDocument eqd) throws DataFormatException {
		List<EQDOCFolder> eqFolders= eqd.getReportFolder();
		if (eqFolders!=null){
			for (EQDOCFolder eqFolder:eqFolders) {
				if (eqFolder.getId()==null)
					throw new DataFormatException("No folder id");
				if (eqFolder.getName()==null)
					throw new DataFormatException("No folder name");
				String iri= "urn:uuid:"+ eqFolder.getId();
				TTEntity folder = new TTEntity()
					.setIri(iri)
						.addType(IM.FOLDER)
							.setName(eqFolder.getName())
					.set(IM.IS_CONTAINED_IN,mainFolder);
				document.addEntity(folder);
				if (eqFolder.getAuthor()!=null && eqFolder.getAuthor().getAuthorName()!=null)
					setProvenance(iri,eqFolder.getAuthor().getAuthorName());
			}
		}
	}

	private void setProvenance(String iri,String authorName) {
		ProvActivity activity= new ProvActivity()
			.setIri("urn:uuid:"+ UUID.randomUUID())
			.setActivityType(IM.PROV_CREATION)
			.setEffectiveDate(LocalDateTime.now().toString());
		document.addEntity(activity);
		if (authorName!=null) {
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
		StringBuilder uri= new StringBuilder();
		name.chars().forEach(c-> {
			if (Character.isLetterOrDigit(c))
				uri.append(Character.toString(c));
		});
		String root= owner.getIri();
		root= root.substring(0,root.lastIndexOf("#"));
		return root.replace("org.","uir.")+"/personrole#"+
			uri;
	}

	public TTEntity convertReport(EQDOCReport eqReport) throws DataFormatException, IOException {

		setVocabMaps();
		activeReport = eqReport.getId();
		TTEntity entity= new TTEntity();
		entity.setIri("urn:uuid:" + eqReport.getId());
		entity.setName(eqReport.getName());
		entity.setDescription(eqReport.getDescription());
		if (eqReport.getFolder()!=null)
			entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri("urn:uuid:"+ eqReport.getFolder()));
		if (eqReport.getCreationTime()!=null)
			setProvenance(entity.getIri(), null);

		if (eqReport.getPopulation() != null) {
			entity.addType(IM.PROFILE);
			DataSet profile= new DataSet();
			profile.setName(entity.getName());
			profile.setDescription(entity.getDescription());
			profile.setIri(entity.getIri());
			Match main= new Match();
			profile.setMatch(main);
			main.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient").setName("Patient"));
			if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
				setFrom(main,TTIriRef.iri(IM.NAMESPACE+"Q_RegisteredGMS"), "Registered with GP for GMS services on the reference date");
			}
			if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
				String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
				setFrom(main,TTIriRef.iri("urn:uuid:" + id), reportNames.get(id));
			}
			convertPopulation(eqReport.getPopulation(), main);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json= objectMapper.writeValueAsString(profile);
			entity.set(IM.DEFINITION,TTLiteral.literal(json));
		}
		if (eqReport.getListReport()!=null){
			entity.addType(IM.DATASET);
			DataSet set= new DataSet();
			set.setName(entity.getName());
			set.setDescription(entity.getDescription());
			set.setIri(entity.getIri());
			Match main= new Match();
			set.setMatch(main);
			String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
			setFrom(main,TTIriRef.iri("urn:uuid:" + id), reportNames.get(id));
			convertListReport(eqReport.getListReport(),set);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json= objectMapper.writeValueAsString(set);
			entity.set(IM.DEFINITION,TTLiteral.literal(json));
		}
		if (eqReport.getAuditReport()!=null){
			entity.addType(IM.DATASET);
			DataSet set= new DataSet();
			set.setName(entity.getName());
			set.setDescription(entity.getDescription());
			set.setIri(entity.getIri());
			convertAuditReport(eqReport.getAuditReport(),set);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json= objectMapper.writeValueAsString(set);
			entity.set(IM.DEFINITION,TTLiteral.literal(json));

		}
		return entity;
	}

	private void convertAuditReport(EQDOCAuditReport auditReport, DataSet set) throws IOException {
		Match mainMatch= new Match();
		set.setMatch(mainMatch);
		mainMatch.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient"));
		mainMatch.setProperty(getIri(IM.IN_RESULT_SET.getIri()));
		varCounter++;
		mainMatch.setValueVar("resultSet"+varCounter);
		Select select= new Select();
		set.addGroupBy(select);
		select.setName("population");
		select.setVar("resultSet"+varCounter);
		set.addSelect(select);
		for (String popId:auditReport.getPopulation()){
			mainMatch.addValueIn(TTIri.iri("urn:uuid:"+ popId).setName(reportNames.get(popId)));
		}
		Match patientEntity= new Match();
		mainMatch.setValueObject(patientEntity);
		patientEntity.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient").setName("Patient"));
		Match patientId= new Match();
		patientId.addAnd(patientId);
		patientId.setProperty(TTIriRef.iri(IM.NAMESPACE+"id").setName("id"));
		patientId.setValueVar("id");
		EQDOCAggregateReport agg= auditReport.getCustomAggregate();
		String eqTable= agg.getLogicalTable();

		for (EQDOCAggregateGroup group:agg.getGroup()){
			for (String eqColum: group.getGroupingColumn()){
				Match thisMatch= new Match();
				patientEntity.addAnd(thisMatch);
				String predicate= (String) dataMap.get(eqTable+slash+ eqColum);
				String[] path= predicate.split("/");
				thisMatch.setProperty(getIri(path[0]));
				String valueVar;
				if (path.length>1) {
					Match valueObject = new Match();
					thisMatch.setValueObject(valueObject);
					valueObject.setEntityType(getIri(path[1]));
					valueObject.setProperty(getIri(path[2]));
					varCounter++;
					valueVar= path[2]+varCounter;
					valueObject.setValueVar(path[2] + varCounter);
				}
				else {
					varCounter++;
					thisMatch.setValueVar(path[0]+varCounter);
					valueVar= path[0]+ varCounter;
				}
				Select column= new Select();
				set.addSelect(column);
				column.setVar(valueVar);
				column.setName(group.getDisplayName());
				set.addGroupBy(new Select().setVar(valueVar));
			}
		}
		select= new Select();
		set.addSelect(select);
		select.setName("id");
		select.setVar("id");
		select.setCount(true);


	}

	private void convertListReport(EQDOCListReport eqListReport, DataSet shape) throws DataFormatException, IOException {

		for (EQDOCListReport.ColumnGroups eqColGroups : eqListReport.getColumnGroups()) {
			EQDOCListColumnGroup eqColGroup = eqColGroups.getColumnGroup();
			DataSet selectGroup = new DataSet();
			shape.addSubset(selectGroup);
			convertListGroup(eqColGroup, selectGroup);
		}
	}


	private void convertListGroup(EQDOCListColumnGroup eqColGroup, DataSet group) throws DataFormatException, IOException {
		propertyVar= new HashMap<>();
		group.setName(eqColGroup.getDisplayName());
		String eqTable= eqColGroup.getLogicalTableName();
		String entityType= (String) dataMap.get(eqTable);
		Match mainMatch= new Match();
		group.setMatch(mainMatch);
		mainMatch.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient").setName("Patient"));

		if (eqColGroup.getCriteria()!=null){
			convertCriteria(eqColGroup.getCriteria(),mainMatch);
		}
		EQDOCListColumns eqCols= eqColGroup.getColumnar();
		for (EQDOCListColumn eqCol: eqCols.getListColumn()) {
			String eqDisplay = eqCol.getDisplayName();
			for (String eqColName : eqCol.getColumn()) {
				String predicatePath = dataMap.getProperty(eqTable + slash + eqColName);
				Select Select = new Select();
				group.addSelect(Select);
				Select.setName(eqDisplay);
				Select.setAlias(CaseUtils.toCamelCase(eqColName, false).replaceAll("[^a-zA-Z0-9_]", ""));
				String[] path= predicatePath.split("/");
				Match predicateMatch=null;
				if (path.length>1) {
						String subPath = path[0];
						String var = propertyVar.get(subPath);
						if (var!=null)
						 predicateMatch = varMatch.get(var);
				}
				String var = propertyVar.get(predicatePath);
				if (var == null) {
					if (predicateMatch != null) {
						Match subObject= new Match();
						subObject.setEntityType(getIri(path[1]));
						predicateMatch.setValueObject(subObject);
						subObject.setProperty(getIri(path[2]));
						varCounter++;
						subObject.setValueVar(path[2]+varCounter);
						var=(path[2]+varCounter);
					} else {
						Match columnMatch = new Match();
						mainMatch.addMay(columnMatch);
						columnMatch.setProperty(getIri(IM.NAMESPACE + predicatePath));
						varCounter++;
						var = predicatePath + varCounter;
						columnMatch.setValueVar(var);
					}
				}
				Select.setVar(var);
			}
		}
		storeGroup(eqColGroup,group);
	}




	private void storeGroup(EQDOCListColumnGroup eqColGroup,DataSet group) throws JsonProcessingException {
		TTIriRef iri= TTIriRef.iri("urn:uuid:"+eqColGroup.getId());
		if (!fieldGroups.contains(iri)){
			TTEntity fieldGroup= new TTEntity()
				.addType(IM.FIELD_GROUP)
				.setIri(iri.getIri())
				.setName(eqColGroup.getDisplayName());
			document.addEntity(fieldGroup);
			fieldGroup.addObject(IM.IS_CONTAINED_IN,fieldGroupFolder);
			fieldGroup.addObject(IM.DEFINITION,TTLiteral.literal(group.getasJson()));
			fieldGroups.add(iri);
		}

	}




	private TTIriRef getIri(String token) throws IOException {
		if (token.equals("label"))
			return RDFS.LABEL.setName("label");
		else {
			TTIriRef iri;
			if (!token.contains(":")) {
				return TTIriRef.iri(IM.NAMESPACE + token).setName(importMaps.getCoreName(IM.NAMESPACE+ token));
			}
			else
				return TTIriRef.iri(token).setName(importMaps.getCoreName(token));
		}
	}



	private void convertPopulation(EQDOCPopulation population, Match main) throws DataFormatException, IOException {
		Match orMatch= null;
		Match parentAnd=null;
		propertyVar= new HashMap<>();
		for (EQDOCCriteriaGroup eqGroup : population.getCriteriaGroup()) {
			VocRuleAction ifTrue = eqGroup.getActionIfTrue();
			VocRuleAction ifFalse = eqGroup.getActionIfFalse();

			if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.NEXT) {
				orMatch= new Match();
				main.addOr(orMatch);
				addToOr(eqGroup,orMatch);

			}
			else if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.REJECT) {
				if (orMatch!=null){
					Match lastOr= new Match();
					main.addOr(lastOr);
					addToOr(eqGroup,lastOr);
					orMatch=null;
				}
				else {
					addAnd(eqGroup,main);
				}
			}
			else if (ifTrue == VocRuleAction.NEXT && ifFalse == VocRuleAction.REJECT) {
				if (orMatch != null) {
					Match lastOr = new Match();
					main.addOr(lastOr);
					addToOr(eqGroup, lastOr);
					orMatch = null;
				} else {
					addAnd(eqGroup, main);
				}
			}
			else if (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.NEXT) {
				orMatch = new Match();
				main.addOr(orMatch);
				addToOr(eqGroup, orMatch);
			}
			else if (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.SELECT) {
				if (orMatch != null) {
					Match lastOr = new Match();
					main.addOr(lastOr);
					addToOr(eqGroup, lastOr);
					orMatch = null;
				} else {
					addAnd(eqGroup, main);
				}

			}
			else
					throw new DataFormatException("unrecognised action rule combination : " + activeReport);

			}
	}

	private void addAnd(EQDOCCriteriaGroup eqGroup, Match main) throws DataFormatException, IOException {
		VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
		boolean negation= false;
		if (eqGroup.getActionIfTrue()== VocRuleAction.REJECT)
			negation= true;
		if (memberOp== VocMemberOperator.AND) {
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match andMatch = new Match();
				main.addAnd(andMatch);
				if (negation)
					andMatch.setNotExist(true);
				convertCriteria(eqCriteria,andMatch);
			}
		}
		else {
			Match andMatch= new Match();
			main.addAnd(andMatch);
			if (negation)
				andMatch.setNotExist(true);
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match orMatch= new Match();
				andMatch.addOr(orMatch);
				convertCriteria(eqCriteria,orMatch);
			}
		}

	}

	private void addToOr(EQDOCCriteriaGroup eqGroup, Match orMatch) throws DataFormatException, IOException {

		VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
		int eqCount= eqGroup.getDefinition().getCriteria().size();
		if (eqCount==1){
			convertCriteria(eqGroup.getDefinition().getCriteria().get(0),orMatch);
		}
		else if (memberOp== VocMemberOperator.OR) {
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match subOrMatch = new Match();
				orMatch.addOr(subOrMatch);
				convertCriteria(eqCriteria, subOrMatch);
			}
		}
		else{
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				Match andMatch= new Match();
				orMatch.addAnd(andMatch);
				convertCriteria(eqCriteria,andMatch);
			}
		}


	}


	private void convertCriteria(EQDOCCriteria eqCriteria,
													 Match match) throws DataFormatException, IOException {

		dateMatch = null;
		needsDob=false;
		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			match.addSubsetOf(TTIri.iri("urn:uuid:" + srch.getReportGuid())
					.setName(reportNames.get(srch.getReportGuid())));
		}
		else {
			if (eqCriteria.getCriterion().getId()!=null) {
				match.setIri("urn:uuid:" + eqCriteria.getCriterion().getId());
			}
			convertCriterion(eqCriteria.getCriterion(),match,null);
		}

	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Match match, String linkField) throws DataFormatException, IOException {
		String eqTable = eqCriterion.getTable();
		String entityType = (String) dataMap.get(eqTable);

		if (!eqTable.equals("PATIENTS")){
			match.setProperty(TTIriRef.iri(IM.NAMESPACE+"isSubjectOf"));
			match.setValueObject(new Match());
			match= match.getValueObject();
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
		convertColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), match,
			linkField);
		if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			setRestriction(eqCriterion, match);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				List<EQDOCColumnValue> cvs = eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue();
				Match test = new Match();
				match.getSortLimit().addMust(test);
				setMainCriterion(eqTable, cvs.get(0), test);
				if (linkField != null && dateMatch == null) {
					Match subMatch = new Match();
					match.addMay(subMatch);
					addDateMatch(subMatch, eqTable, linkField);
				}
			}
		}
		if (needsDob){
				Match dobMatch= new Match();
				match.addAnd(dobMatch);
				dobMatch.setProperty(getIri(IM.NAMESPACE+"dateOfBirth"));
				dobMatch.setValueVar(dobVar);
				varMatch.put(dobVar,dobMatch);
		}

		if (eqCriterion.getLinkedCriterion()!=null) {
			convertLinkedCriterion(eqTable,eqCriterion.getLinkedCriterion(),
				match);
		}

	}



	private void convertColumns(EQDOCFilterAttribute filterAttribute, String eqTable,Match entity,
								String linkField) throws DataFormatException, IOException {

		if (filterAttribute.getColumnValue().size()==1 &&linkField==null){
			setMainCriterion(eqTable,filterAttribute.getColumnValue().get(0),entity);
		}
		else {
			for (EQDOCColumnValue cv : filterAttribute.getColumnValue()) {
				Match match = new Match();
				entity.addAnd(match);
				setMainCriterion(eqTable, cv, match);

			}
		}
		if (linkField != null && dateMatch == null) {
			Match dateMatch = new Match();
			entity.addAnd(dateMatch);
			addDateMatch(dateMatch, eqTable, linkField);
		}

	}

	private void addDateMatch(Match subMatch,String eqTable,String linkField) throws DataFormatException, IOException {
		String fieldPath= getMap(eqTable + slash + linkField);
		String date= fieldPath.substring(fieldPath.lastIndexOf("/")+1);
		subMatch.setProperty(getIri(IM.NAMESPACE+date));
		varCounter++;
		subMatch.setValueVar(date+varCounter);
		dateMatch= date+varCounter;
		propertyVar.put(fieldPath,date+varCounter);
		varMatch.put(date+varCounter,subMatch);

	}

	private void setRestriction(EQDOCCriterion eqCriterion,Match matchEntity) throws DataFormatException, IOException {
		String eqTable = eqCriterion.getTable();
		String linkColumn= eqCriterion.getFilterAttribute().getRestriction()
				.getColumnOrder().getColumns().get(0).getColumn().get(0);
		SortLimit sort= new SortLimit();
		matchEntity.setSortLimit(sort);
		String predicatePath= (String) dataMap.get(eqTable+slash+ linkColumn);
		sort.setOrderBy(getIri(IM.NAMESPACE+predicatePath));
		EQDOCFilterRestriction restrict = eqCriterion.getFilterAttribute().getRestriction();
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC)
			sort.setDirection(Order.ASCENDING);
		else
			sort.setDirection(Order.DESCENDING);
	}




	private void setMainCriterion(String eqTable,EQDOCColumnValue cv,Match match) throws DataFormatException, IOException {
		for (String eqColumn : cv.getColumn()) {
			setPropertyValue(cv, eqTable, eqColumn, match);
			if (eqColumn.contains("DATE"))
				dateMatch= match.getValueVar();
		}
	}


	private void setPropertyValue(EQDOCColumnValue cv,String eqTable,String eqColumn,
																Match match) throws DataFormatException, IOException {
		String predPath= getMap(eqTable + slash + eqColumn);
		String predicate= predPath.substring(predPath.lastIndexOf("/")+1);
		match.setProperty(getIri(IM.NAMESPACE+ predicate));
		VocColumnValueInNotIn in= cv.getInNotIn();
		varCounter++;
		match.setValueVar(predicate+varCounter);
		propertyVar.put(predPath,predicate+varCounter);
		varMatch.put(predicate+varCounter,match);
		boolean notIn= (in== VocColumnValueInNotIn.NOTIN);
		if (!cv.getValueSet().isEmpty()){
			for (EQDOCValueSet vs:cv.getValueSet()) {
				if (vs.getAllValues()!=null){
					match.setValueNotIn(getExceptionSet(vs.getAllValues()));
				}
				else {
					if (!notIn)
						match.addValueIn(getValueSet(vs));
					else
						match.addValueNotIn(getValueSet(vs));
				}
			}
		}
		else
		if (!CollectionUtils.isEmpty(cv.getLibraryItem())) {
			for (String vset : cv.getLibraryItem()) {
				String vsetName="Unknown code set";
				if (labels.get(vset)!=null)
					vsetName= (String) labels.get(vset);
				if (!notIn)
					match.addValueIn(TTIriRef.iri("urn:uuid:" + vset).setName(vsetName));
				else
					match.addValueNotIn(TTIriRef.iri("urn:uuid:" + vset).setName(vsetName));
			}
		}
		else if (cv.getRangeValue()!=null){
			setRangeValue(cv.getRangeValue(),match);
		}
	}


	private void setRangeValue(EQDOCRangeValue rv, Match match) throws InvalidClassException, DataFormatException {

		EQDOCRangeFrom rFrom= rv.getRangeFrom();
		EQDOCRangeTo rTo= rv.getRangeTo();
		if (rFrom != null) {
			if (rTo==null) {
				setCompareFrom(match,rFrom);
			}
			else {
				setRangeCompare(match,rFrom,rTo);
			}
		}
		if (rTo != null && rFrom == null) {
			setCompareTo(match, rTo);
		}

	}

	private void setCompareFrom(Match match, EQDOCRangeFrom rFrom) throws InvalidClassException {
		Comparison comp;
		if (rFrom.getOperator() != null)
			comp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rFrom.getValue().getValue();
		String units=null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();
		if (match.getProperty().equals(TTIriRef.iri(IM.NAMESPACE+"age"))){
			match.setValueCompare(addCompareAge(match,rFrom.getValue(),comp));

		}
		else {
			if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				Function function = getTimeDiff(units, match.getValueVar(), "$referenceDate");
				match.setValueCompare(new Compare()
					.setComparison(comp)
					.setValue(value));
					match.setFunction(function);
			} else {
				match.setValueCompare(new Compare()
					.setComparison(comp)
					.setValue(value));
			}
		}
	}

	private Compare addCompareAge(Match match, EQDOCValue valueCompare,Comparison comp) {

		Compare compare= new Compare();
		match.setFunction(ageFunction(valueCompare.getUnit().value()));
		compare.setValue(valueCompare.getValue());
		compare.setComparison(comp);
		return compare;
	}

	private Function ageFunction(String units){
		Function function = new Function();
		function.setId(TTIriRef.iri(IM.NAMESPACE+"AgeFunction"));
		function.addArgument(new Argument().setParameter("units").setValue(units));
		varCounter++;
		dobVar= "dateOfBirth"+ varCounter;
		needsDob=true;
		function.addArgument(new Argument().setParameter("fromDate").setValue(dobVar));
		function.addArgument(new Argument().setParameter("referenceDate").setValue("$referenceDate"));
		return function;
	}

	private void setCompareTo(Match match, EQDOCRangeTo rTo) throws InvalidClassException {
		Comparison comp;
		if (rTo.getOperator() != null)
			comp = (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rTo.getValue().getValue();
		String units=null;
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();
		if (match.getProperty().equals(TTIriRef.iri(IM.NAMESPACE+"age"))){
			match.setValueCompare(addCompareAge(match,rTo.getValue(),comp));
		}
		else {
			if (rTo.getValue().getRelation() != null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
				Function function = getTimeDiff(units, match.getValueVar(), "$referenceDate");
				match.setValueCompare(new Compare()
					.setComparison(comp)
					.setValue(value));
					match.setFunction(function);
			} else {
				match.setValueCompare(new Compare()
					.setComparison(comp)
					.setValue(value));
			}
		}
	}


	private Function getTimeDiff(String units,String firstDate,String compareAgainst){
		Function function=null;
		if (compareAgainst!=null) {
			function = new Function().setId(TTIriRef.iri(IM.NAMESPACE + "TimeDifference").setName("Time Difference"));
			function.addArgument(new Argument().setParameter("units").setValue(units));
			function.addArgument(new Argument().setParameter("firstDate").setValue(firstDate));
			function.addArgument(new Argument().setParameter("secondDate").setValue(compareAgainst));
		}
		return function;
	}

	private void setRangeCompare(Match match, EQDOCRangeFrom rFrom,EQDOCRangeTo rTo) throws DataFormatException {
		Range range= new Range();
		match.setValueRange(range);
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.EQUAL;
		String fromValue= rFrom.getValue().getValue();
		String units= null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();
		if (match.getProperty().equals(TTIriRef.iri(IM.NAMESPACE+"age"))){
			match.setFunction(ageFunction(units));
		}
			if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				Function function = getTimeDiff(units, match.getValueVar(), "$referenceDate");
				range.setFrom(new Compare()
					.setComparison(fromComp)
					.setValue(fromValue));
					match.setFunction(function);
		}
		else {
			range.setFrom(new Compare()
				.setComparison(fromComp)
				.setValue(fromValue));
		}

		Comparison toComp;
		if (rTo.getOperator()!=null)
			toComp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp= Comparison.EQUAL;
		String toValue= rTo.getValue().getValue();
		units= null;
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();
		if (rTo.getValue().getRelation()!=null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
			Function function = getTimeDiff(units, match.getValueVar(), "$referenceDate");
			range.setTo(new Compare()
				.setComparison(toComp)
				.setValue(toValue));
				match.setFunction(function);
		}
		else {
			range.setTo(new Compare()
				.setComparison(toComp)
				.setValue(toValue));
		}

	}


	private String getMap(String from) throws DataFormatException {
		Object target = dataMap.get(from);
		if (target == null)
			throw new DataFormatException("unknown map : " + from);
		return (String) target;
	}

	private void convertLinkedCriterion(String eqTable,EQDOCLinkedCriterion eqLinked, Match entity) throws DataFormatException, IOException {
		EQDOCCriterion eqTargetCriterion= eqLinked.getCriterion();
		Within within= new Within();
		Match linkTarget= new Match();
		within.setTargetMatch(linkTarget);
		convertCriterion(eqTargetCriterion,linkTarget,"DATE");
		linkTarget= linkTarget.getValueObject();
		EQDOCRelationship eqRel = eqLinked.getRelationship();

		Match linkProperty= findLinkProperty(eqTable,eqRel.getParentColumn(),entity);

		linkProperty.setValueWithin(within);
		String units= eqRel.getRangeValue().getRangeFrom().getValue().getUnit().value();
		VocRangeFromOperator eqOp= eqRel.getRangeValue().getRangeFrom().getOperator();
		String value= eqRel.getRangeValue().getRangeFrom().getValue().getValue();
		String firstDate= linkProperty.getValueVar();
		String secondDate= linkTarget.getAnd().get(0).getValueVar();
		Function function= getTimeDiff(units,firstDate,secondDate);
		within.setCompare(new Compare()
			.setComparison((Comparison) vocabMap.get(eqOp))
			.setValue(value));
			within.setFunction(function);
	}

	private Match findLinkProperty(String eqTable,String parentColumn,Match entity) throws IOException {
		String predicate= (String) dataMap.get(eqTable+slash+ parentColumn);
		TTIriRef property= getIri(IM.NAMESPACE+predicate);
		if (entity.getAnd()!=null){
			for (Match match:entity.getAnd()){
				if (match.getProperty().equals(property))
					return match;
			}
		}
		Match linkMatch= new Match();
		entity.addAnd(linkMatch);
		linkMatch.setProperty(property);
		varCounter++;
		linkMatch.setValueVar(predicate+varCounter);
		return linkMatch;
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

	private List<TTIriRef> getExceptionSet(EQDOCException set) throws DataFormatException, IOException {
		List<TTIriRef> valueSet = new ArrayList<>();
		VocCodeSystemEx scheme= set.getCodeSystem();
		for (EQDOCExceptionValue ev:set.getValues()) {
			Set<TTIriRef> values = getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
			if (values != null) {
				valueSet.addAll(new ArrayList<>(values));
			} else
				System.err.println("Missing exception sets\t" + ev.getValue() + "\t " + ev.getDisplayName());
		}

		return valueSet;
	}

	private TTIriRef getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTIriRef> setContent = new ArrayList<>();
		StringBuilder vsetName = new StringBuilder();
		VocCodeSystemEx scheme = vs.getCodeSystem();
		int i = 0;
		for (EQDOCValueSetValue ev : vs.getValues()) {
			i++;
			if (i ==1) {
				if (vsetName.length()<1) {
					if (ev.getDisplayName() != null) {
						vsetName.append(ev.getDisplayName());
					}
				}
			}
			else if (i==2){
				vsetName.append(".. and more ...");
			}
			Set<TTIriRef> concepts = getValue(scheme, ev);
			if (concepts != null) {
				setContent.addAll(new ArrayList<>(concepts));
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}
		if (labels.get(vs.getId())!=null)
			vsetName.append((String) labels.get(vs.getId()));
		if (vs.getDescription() != null)
			vsetName = new StringBuilder(vs.getDescription());
		storeValueSet(vs,setContent,vsetName.toString());
		return TTIriRef.iri("urn:uuid:"+vs.getId()).setName(vsetName.toString());
	}

	private void storeValueSet(EQDOCValueSet vs, List<TTIriRef> valueSet,String vSetName) {
		if (vs.getId()!=null){
			TTIriRef iri= TTIriRef.iri("urn:uuid:"+vs.getId()).setName(vSetName);
			if (!valueSets.keySet().contains(iri)){
				TTEntity conceptSet= new TTEntity()
					.setIri(iri.getIri())
					.addType(IM.CONCEPT_SET)
					.setName(vSetName);
				conceptSet.addObject(IM.IS_CONTAINED_IN,valueSetFolder);
				TTNode ors= new TTNode();
				conceptSet.addObject(IM.DEFINITION,ors);
				for (TTIriRef member:valueSet)
					ors.addObject(SHACL.OR,member);
				document.addEntity(conceptSet);
				valueSets.put(iri,conceptSet);
			}
			valueSets.get(iri).addObject(IM.USED_IN,TTIriRef.iri("urn:uuid:"+ activeReport));
		}
	}

	/*
	private TTIriRef getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
		TTEntity valueSet = new TTEntity();
		TTIriRef iri = TTIriRef.iri("urn:uuid:" + UUID.randomUUID());
		StringBuilder vsetName = new StringBuilder();
		if (vs.getDescription()!=null)
			vsetName = new StringBuilder(vs.getDescription());
		valueSet.setIri(iri.getIri());
		valueSet.addType(IM.CONCEPT_SET);
		VocCodeSystemEx scheme = vs.getCodeSystem();
		if (vs.getValues().size() == 1) {
			if (vsetName.length() == 0)
				vsetName.append(vs.getValues().get(0).getDisplayName());
			Set<TTIriRef> concepts= getValue(scheme,vs.getValues().get(0));
			if (concepts!=null) {
				if (concepts.size()==1)
					valueSet.addObject(IM.DEFINITION, concepts.stream().findFirst().get());
				else {
					TTNode ors= new TTNode();
					valueSet.addObject(IM.DEFINITION,ors);
					concepts.forEach(v-> ors.addObject(SHACL.OR,v));
				}
			}
		} else {
			int i=0;
			for (EQDOCValueSetValue ev : vs.getValues()) {
				i++;
				if (i < 10) {
					if (vsetName.length() != 0)
						vsetName.append(", ");
					vsetName.append(ev.getDisplayName());
				} else if (i == 10)
					vsetName.append("..more");
				Set<TTIriRef> concepts = getValue(scheme, ev);
				if (concepts!=null) {
					if (concepts.size()==1)
						valueSet.addObject(IM.DEFINITION, concepts.stream().findFirst().get());
					else {
						TTNode ors= new TTNode();
						valueSet.addObject(IM.DEFINITION,ors);
						concepts.forEach(v-> ors.addObject(SHACL.OR,v));
					}
				}
				else
					System.err.println("Missing \t"+ ev.getValue()+"\t " + ev.getDisplayName());
			}
		}
		if (vsetName.length() > 0) {
			iri.setName(vsetName.toString());
			valueSet.setName(iri.getName());
		}

		TTEntity duplicateOf = getDuplicateSet(valueSet);
		if (duplicateOf!=null){
			iri= TTIriRef.iri(duplicateOf.getIri());
			iri.setName(duplicateOf.getName());
			return iri;
		}
		valueSet.addObject(IM.USED_IN,TTIriRef.iri(reportMap.get(activeReport).getIri()));
		document.addEntity(valueSet);
		valueSets.add(valueSet);
		return iri;
	}
	*/

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme,EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(),ev.getDisplayName(),ev.getLegacyValue());
	}

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme, String originalCode,
																 String originalTerm,String legacyCode) throws DataFormatException, IOException {
		if (scheme== VocCodeSystemEx.EMISINTERNAL) {
			String key = "EMISINTERNAL/" + originalCode;
			Object mapValue = dataMap.get(key);
			if (mapValue != null) {
				TTIriRef iri= getIri(mapValue.toString());
				String name= importMaps.getCoreName(iri.getIri());
				if (name!=null)
					iri.setName(name);
				Set<TTIriRef> result= new HashSet<>();
				result.add(iri);
				return result;
			}
			else
				throw new DataFormatException("unmapped emis internal code : "+key);
		}
		else if (scheme== VocCodeSystemEx.SNOMED_CONCEPT || scheme.value().contains("SCT")){
			List<String> schemes= new ArrayList<>();
			schemes.add(SNOMED.NAMESPACE);
			schemes.add(IM.CODE_SCHEME_EMIS.getIri());
			Set<TTIriRef> snomed= valueMap.get(originalCode);
			if (snomed==null) {
				snomed= getCoreFromCode(originalCode,schemes);
				if (snomed==null)
					if (legacyCode!=null)
						snomed=getCoreFromCode(legacyCode,schemes);
				if (snomed == null)
					if (originalTerm != null)
						snomed= getCoreFromLegacyTerm(originalTerm);
				if (snomed==null)
					snomed= getCoreFromCodeId(originalCode);
				if (snomed==null)
					snomed= getLegacyFromTermCode(originalCode);

				if (snomed != null)
					valueMap.put(originalCode, snomed);
			}
			return snomed;
		}
		else
			throw new DataFormatException("code scheme not recognised : "+scheme.value());

	}



	/*
	private Set<TTIriRef> getValue(VocCodeSystemEx scheme, String originalCode,
																 String originalTerm,String legacyCode) throws DataFormatException {
		if (scheme== VocCodeSystemEx.EMISINTERNAL) {
			String key = "EMISINTERNAL/" + originalCode;
			Object mapValue = dataMap.get(key);
			if (mapValue != null) {
				Set<TTIriRef> result= new HashSet<>();
				result.add(TTIriRef.iri(mapValue.toString()));
				return result;
			}
			else
				throw new DataFormatException("unmapped emis internal code : "+key);
		}
		else if (scheme== VocCodeSystemEx.SNOMED_CONCEPT || scheme.value().contains("SCT")){
			List<String> schemes= new ArrayList<>();
			schemes.add(SNOMED.NAMESPACE);
			schemes.add(IM.CODE_SCHEME_EMIS.getIri());
			Set<TTIriRef> snomed= valueMap.get(originalCode);
			if (snomed==null) {
				snomed= getCoreFromCode(originalCode,schemes);
				if (snomed==null)
					if (legacyCode!=null)
						snomed=getCoreFromCode(legacyCode,schemes);
				if (snomed == null)
					if (originalTerm != null)
						snomed= getCoreFromLegacyTerm(originalTerm);
			if (snomed==null)
				snomed= getCoreFromCodeId(originalCode);
				if (snomed==null)
					snomed= getLegacyFromTermCode(originalCode);


				if (snomed != null)
					valueMap.put(originalCode, snomed);
			}
			return snomed;
		}
		else
			throw new DataFormatException("code scheme not recognised : "+scheme.value());

	}

	*/
	private Set<TTIriRef> getCoreFromCodeId(String originalCode){

		try {
			return importMaps.getCoreFromCodeId(originalCode, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e){
			System.err.println("unable to retrieve iri from term code "+ e.getMessage());
			e.printStackTrace();
			return null;
		}
	}



	private Set<TTIriRef> getLegacyFromTermCode(String originalCode) {
		try {
			return importMaps.getLegacyFromTermCode(originalCode, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e) {
			System.err.println("unable to retrieve iri from term code "+ e.getMessage());
			return null;
		}
	}

	private Set<TTIriRef> getCoreFromLegacyTerm(String originalTerm) {
		try {
			if (originalTerm.contains("s disease of lymph nodes of head, face AND/OR neck"))
				System.out.println("!!");

			return importMaps.getCoreFromLegacyTerm(originalTerm, IM.CODE_SCHEME_EMIS.getIri());
		} catch (Exception e) {
			System.err.println("unable to retrieve iri from term "+ e.getMessage());
			return null;
		}
	}

	private Set<TTIriRef> getCoreFromCode(String originalCode, List<String> schemes) {
		return importMaps.getCoreFromCode(originalCode,schemes);
	}




	private void setFrom(Match match, TTIriRef parent, String parentName) {
		if (parentName!=null)
			parent.setName(parentName);
		match.addSubsetOf(new TTIri(parent).setDescription("Based on the cohort"));
	}

	/*
	private void setParent(Match clause, TTIriRef parent, String parentName) {
		if (parentName!=null)
			clause.setName(parentName);
		clause.setProperty(IM.HAS_PROFILE)
			.addValueIn(parent);
	}

	 */

	private void setVocabMaps() {
		vocabMap.put(VocRangeFromOperator.GTEQ, Comparison.GREATER_THAN_OR_EQUAL);
		vocabMap.put(VocRangeFromOperator.GT, Comparison.GREATER_THAN);
		vocabMap.put(VocRangeToOperator.LT, Comparison.LESS_THAN);
		vocabMap.put(VocRangeToOperator.LTEQ, Comparison.LESS_THAN_OR_EQUAL);
		vocabMap.put(VocOrderDirection.DESC, SortOrder.DESCENDING);
		vocabMap.put(VocOrderDirection.ASC, SortOrder.ASCENDING);
	}

}
