package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.collections4.CollectionUtils;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.hql.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTCompare;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import javax.swing.*;
import java.io.IOException;
import java.io.InvalidClassException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToTT {
	private static  Map<String,String> reportNames;
	private static final Set<TTEntity> valueSets = new HashSet<>();
	private static final Set<String> roles= new HashSet<>();
	private TTIriRef owner;
	private Properties dataMap;
	private Properties criteriaLabels;
	private int varCounter;
	private String activeReport;
	private final Map<String,TTEntity> reportMap= new HashMap<>();
	private TTDocument document;
	private final String slash = "/";
	private final Map<String,Set<TTIriRef>> valueMap= new HashMap<>();
	private final ImportMaps importMaps = new ImportMaps();
	String dateMatch;
	private final Map<Object, Object> vocabMap = new HashMap<>();

	public void convertDoc(TTDocument document, TTIriRef mainFolder, EnquiryDocument eqd, TTIriRef owner, Properties dataMap,
						   Properties criteriaLabels) throws DataFormatException, IOException {
		this.owner = owner;
		this.dataMap = dataMap;
		this.document= document;
		this.criteriaLabels= criteriaLabels;
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

			if (eqReport.getPopulation() != null) {
				TTEntity qry= convertReport(eqReport);
				document.addEntity(qry);
				setProvenance(qry.getIri(),"CEG");
			}
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
		TTEntity entity= new TTEntity().addType(IM.PROFILE);
		reportMap.put(activeReport,entity);
		entity.setIri("urn:uuid:" + eqReport.getId());
		entity.setName(eqReport.getName());
		entity.setDescription(eqReport.getDescription());
		if (eqReport.getFolder()!=null)
			entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri("urn:uuid:"+ eqReport.getFolder()));
		if (eqReport.getCreationTime()!=null)
			setProvenance(entity.getIri(), null);

		if (eqReport.getPopulation() != null) {
			Profile profile= new Profile();
			profile.setName(entity.getName());
			profile.setId(TTIriRef.iri(entity.getIri()));
			profile.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Person"));
			Match parentClause= profile.addAnd();
			parentClause.setId(TTIriRef.iri("urn:uuid:"+ UUID.randomUUID()));
			if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
				setParent(parentClause, TTIriRef.iri(IM.NAMESPACE+"Q_RegisteredGMS"), "Registered with GP for GMS services on the reference date");
			}
			if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
				String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
				setParent(parentClause,TTIriRef.iri("urn:uuid:" + id), reportNames.get(id));
			}
			convertPopulation(eqReport.getPopulation(), profile);
			entity.set(IM.DEFINITION,TTLiteral.literal(profile.getasJson()));
		}
		return entity;
	}

	private void convertPopulation(EQDOCPopulation population, Profile main) throws DataFormatException, IOException {
		Match parentOr=null;
		Match andNotClause=null;
		for (EQDOCCriteriaGroup eqGroup : population.getCriteriaGroup()) {
			VocRuleAction ifTrue = eqGroup.getActionIfTrue();
			VocRuleAction ifFalse = eqGroup.getActionIfFalse();
			Match thisMatch;
			Operator groupOp;

			if (ifTrue == VocRuleAction.SELECT && ifFalse == VocRuleAction.NEXT) {
				andNotClause=null;
				thisMatch = new Match();
				if (parentOr==null) {
					parentOr = new Match();
					main.addAnd(parentOr);
				}
				parentOr.addOr(thisMatch);
				groupOp= Operator.OR;
			}
			else if (ifTrue== VocRuleAction.SELECT && ifFalse == VocRuleAction.REJECT){
				andNotClause=null;
				if (getLastActionIfFalse(population,eqGroup)==VocRuleAction.NEXT && parentOr!=null){
					thisMatch= new Match();
					parentOr.addOr(thisMatch);
					groupOp= Operator.OR;
				}
				else {
					thisMatch = new Match();
					main.addAnd(thisMatch);
					groupOp= Operator.AND;
					parentOr=null;
				}
			}
			else if (ifTrue== VocRuleAction.NEXT && ifFalse == VocRuleAction.REJECT){
				andNotClause=null;
				thisMatch = new Match();
				main.addAnd(thisMatch);
				groupOp= Operator.AND;
				parentOr=null;
			}
			else if ((ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.NEXT) || (ifTrue == VocRuleAction.REJECT && ifFalse == VocRuleAction.SELECT)) {
				if (andNotClause==null)
					andNotClause= main.addAnd();
				thisMatch= new Match();
				andNotClause.addNot(thisMatch);
				groupOp= Operator.NOT;
				parentOr=null;
			}
			else
				throw new DataFormatException("unrecognised action rule combination : "+ activeReport);

			if (eqGroup.getId()!=null)
				 thisMatch.setId(TTIriRef.iri("urn:uuid:"+eqGroup.getId()));
			VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
			int eqCount= eqGroup.getDefinition().getCriteria().size();
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				dateMatch=null;
				if (eqCount == 1) {
					setCriteria(eqCriteria, thisMatch);
				}
				else if (memberOp == VocMemberOperator.OR) {
						Match orMatch = new Match();
					if (groupOp== Operator.OR) {
						parentOr.addOr(orMatch);
					}
					else {
						thisMatch.addOr(orMatch);
					}
						setCriteria(eqCriteria, orMatch);
					}
				else if (memberOp== VocMemberOperator.AND){
						Match andMatch = new Match();
						thisMatch.addAnd(andMatch);
						setCriteria(eqCriteria, andMatch);
				}
				else
					throw new DataFormatException("unsupported member operator "+ activeReport);

				}
		}
	}

	private VocRuleAction getLastActionIfFalse(EQDOCPopulation population,EQDOCCriteriaGroup eqGroup){
		int index= population.getCriteriaGroup().indexOf(eqGroup);
		if (index==0)
			return null;
		else
			return population.getCriteriaGroup().get(index-1).getActionIfFalse();
	}

	private void setCriteria(EQDOCCriteria eqCriteria,
													 Match match) throws DataFormatException, IOException {
		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			match
				.setProperty(IM.HAS_PROFILE)
				.addValueIn(TTIriRef.iri("urn:uuid:" + srch.getReportGuid())
					.setName(reportNames.get(srch.getReportGuid())));
		}
		else {
			if (eqCriteria.getCriterion().getId()!=null)
				match.setId(TTIriRef.iri("urn:uuid:"+ eqCriteria.getCriterion().getId()));
			if (eqCriteria.getCriterion().getLinkedCriterion()!=null){
				Match subMatch= new Match();
				match.addAnd(subMatch);
				String linkColumn= eqCriteria.getCriterion().getLinkedCriterion()
					.getRelationship().getParentColumn();
				convertCriterion(eqCriteria.getCriterion(), subMatch,linkColumn);

				convertLinkedCriterion(eqCriteria.getCriterion().getLinkedCriterion(),
					match,linkColumn);
			}
			else {
				convertCriterion(eqCriteria.getCriterion(), match,null);
			}
		}
	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Match match, String linkField) throws DataFormatException, IOException {
		String eqTable = eqCriterion.getTable();
		if (criteriaLabels.get(eqCriterion.getId()) != null) {
			match.setName(criteriaLabels.get(eqCriterion.getId()).toString());
		}
		if (eqCriterion.isNegation()) {
			match.setNotExist(true);
		}
		if (eqCriterion.getDescription() != null)
			match.setDescription(eqCriterion.getDescription());
		if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			setRestriction(eqCriterion, match);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				Match testMatch= new Match();
				match.setTest(testMatch);
				List<EQDOCColumnValue> cvs = eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue();
				if (cvs.size() == 1&&linkField==null) {
					setMainCriterion(eqTable, cvs.get(0), testMatch);
				} else {
					for (EQDOCColumnValue cv : cvs) {
						Match subMatch = new Match();
						testMatch.addAnd(subMatch);
						setMainCriterion(eqTable, cv, subMatch);
					}
					if (linkField!=null && dateMatch==null){
						Match subMatch= new Match();
						testMatch.addAnd(subMatch);
						addDateMatch(subMatch,eqTable,linkField);
					}
				}
			}
		} else {
			processColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(),match,
				false,linkField);
		}
	}

	private void processColumns(EQDOCFilterAttribute filterAttribute, String eqTable,Match match,
								boolean noSubject,String linkField) throws DataFormatException, IOException {
		Map<Match, List<EQDOCColumnValue>> columnMap = getColumnMap(filterAttribute, eqTable);
		if (columnMap.entrySet().size() == 1) {
			for (Map.Entry<Match, List<EQDOCColumnValue>> entry : columnMap.entrySet()) {
				for (EQDOCColumnValue cv : entry.getValue()) {
					Match key = entry.getKey();
					if (!noSubject) {
						match.setPathTo(key.getPathTo());
						match.setEntityType(key.getEntityType());
					}
					Match subMatch = match;
					if (entry.getValue().size() > 1 || (linkField != null)) {
						subMatch = new Match();
						match.addAnd(subMatch);
					}
					setMainCriterion(eqTable, cv, subMatch);
					if (linkField != null && dateMatch == null) {
						Match dateMatch = new Match();
						match.addAnd(dateMatch);
						addDateMatch(dateMatch, eqTable, linkField);
					}
				}
			}
		} else {
			for (Map.Entry<Match, List<EQDOCColumnValue>> entry : columnMap.entrySet()) {
				match.addAnd(entry.getKey());
				for (EQDOCColumnValue cv : entry.getValue()) {
					Match subMatch = entry.getKey();
					if (entry.getValue().size() > 1 || (linkField != null)) {
						subMatch = new Match();
						entry.getKey().addAnd(subMatch);
					}
					setMainCriterion(eqTable, cv, subMatch);
				}
			}
		}
		if (linkField!=null &&dateMatch==null){
			Match subMatch= new Match();
			match.addAnd(subMatch);
			addDateMatch(subMatch,eqTable,linkField);
		}
	}

	private void addDateMatch(Match subMatch,String eqTable,String linkField) throws DataFormatException {
		String fieldPath= getMap(eqTable + slash + linkField);
		String date= fieldPath.substring(fieldPath.lastIndexOf("/")+1);
		subMatch.setProperty(TTIriRef.iri(IM.NAMESPACE+date));
		varCounter++;
		subMatch.setValueVar(date+varCounter);
		dateMatch= date+varCounter;

	}

	private void setRestriction(EQDOCCriterion eqCriterion,Match match) throws DataFormatException, IOException {
		String eqTable = eqCriterion.getTable();
		Map<Match,List<EQDOCColumnValue>> columnMap= getColumnMap(eqCriterion.getFilterAttribute(),eqTable);
		if (columnMap.size()>1)
			throw new DataFormatException("only one entity supported in restrictions "+ activeReport);
		Match firstMatch;
		Optional<Map.Entry<Match,List<EQDOCColumnValue>>> first = columnMap.entrySet().stream().findFirst();
		if (first.isPresent())
			firstMatch= first.get().getKey();
		else
			throw new DataFormatException("unknown column maps for "+ activeReport);
		String linkColumn= eqCriterion.getFilterAttribute().getRestriction()
				.getColumnOrder().getColumns().get(0).getColumn().get(0);
		match.setPathTo(firstMatch.getPathTo());
		match.setEntityType(firstMatch.getEntityType());
		EQDOCFilterRestriction restrict = eqCriterion.getFilterAttribute().getRestriction();
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC)
			match.setSort(new Sort()
					.setDirection(Order.ASCENDING));
		else
			match.setSort(new Sort()
				.setDirection(Order.DESCENDING));
		String eqColumn = restrict.getColumnOrder().getColumns().get(0).getColumn().get(0);
		String fieldPath = getMap(eqTable + slash + eqColumn);
		String field=fieldPath.substring(fieldPath.lastIndexOf("/")+1);
		match.getSort()
				.setOrderBy(TTIriRef.iri(IM.NAMESPACE + field));
		match.getSort().setCount(restrict.getColumnOrder().getRecordCount());
		processColumns(eqCriterion.getFilterAttribute(), eqTable, match,true,linkColumn);
	}


	private Map<Match,List<EQDOCColumnValue>> getColumnMap(EQDOCFilterAttribute eqAtt,String eqTable) throws DataFormatException {
		Map<Match,List<EQDOCColumnValue>> columnMap = new HashMap<>();
		String lastPath = null;
		Match match= new Match();
		for (EQDOCColumnValue cv : eqAtt.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				String entityPath = getEntityPath(eqTable + slash + eqColumn);
				if (!entityPath.equals(lastPath)) {
					match = new Match();
					String thisPath= getMap(eqTable+ slash + eqColumn);
					String[] pathMap= thisPath.split("/");
					match.setPathTo(TTIriRef.iri(IM.NAMESPACE+pathMap[0]));
					match.setEntityType(TTIriRef.iri(IM.NAMESPACE+pathMap[1]));
					columnMap.put(match, new ArrayList<>());
					columnMap.get(match).add(cv);
					lastPath= pathMap[0]+ slash + pathMap[1];
				} else
					columnMap.get(match).add(cv);

			}
		}
		return columnMap;
	}

	private void setMainCriterion(String eqTable,EQDOCColumnValue cv,Match match) throws DataFormatException, IOException {
		for (String eqColumn : cv.getColumn()) {
			setPropertyValue(cv, eqTable, eqColumn, match);
			if (eqColumn.contains("DATE"))
				dateMatch= match.getValueVar();
		}
	}

	private String getEntityPath(String eqPath) throws DataFormatException {
		String fullPath= getMap(eqPath);
		return fullPath.substring(0,fullPath.lastIndexOf("/"));
	}

	private void setPropertyValue(EQDOCColumnValue cv,String eqTable,String eqColumn,
																Match match) throws DataFormatException, IOException {
		String predPath= getMap(eqTable + slash + eqColumn);
		String predicate= predPath.substring(predPath.lastIndexOf("/")+1);
		match.setProperty(TTIriRef.iri(IM.NAMESPACE+ predicate));
		VocColumnValueInNotIn in= cv.getInNotIn();
		varCounter++;
		match.setValueVar(predicate+varCounter);
		boolean notIn= (in== VocColumnValueInNotIn.NOTIN);
		if (!cv.getValueSet().isEmpty()){
			for (EQDOCValueSet vs:cv.getValueSet()) {
				if (vs.getAllValues()!=null){
					match.addValueNotIn(getExceptionSet(vs.getAllValues()));
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
				if (!notIn)
					match.addValueIn(TTIriRef.iri("urn:uuid:" + vset));
				else
					match.addValueNotIn(TTIriRef.iri("urn:uuid:" + vset));
			}
		}
		else if (cv.getRangeValue()!=null){
			setRangeValue(cv.getRangeValue(),match,null);
		}
	}


	private void setRangeValue(EQDOCRangeValue rv, Match match,String compareAgainst) throws InvalidClassException {

		EQDOCRangeFrom rFrom= rv.getRangeFrom();
		EQDOCRangeTo rTo= rv.getRangeTo();
		if (rFrom != null) {
			if (rTo==null) {
				setCompareFrom(match,rFrom,compareAgainst);
			}
			else {
				setRangeCompare(match,rFrom,rTo,compareAgainst);
			}
		}
		if (rTo != null && rFrom == null) {
			setCompareTo(match, rTo,compareAgainst);
		}

	}

	private void setCompareFrom(Match match, EQDOCRangeFrom rFrom,String compareAgainst) throws InvalidClassException {
		Comparison comp;
		if (rFrom.getOperator() != null)
			comp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE && compareAgainst == null) {
			compareAgainst = "$referenceDate";
		}
		String units=null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();
		setCompare(match, comp, value,units,compareAgainst);
	}

	private void setCompareTo(Match match,EQDOCRangeTo rTo,String compareAgainst) throws InvalidClassException {
		Comparison comp;
		if (rTo.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp= Comparison.EQUAL;
		String value= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null && rTo.getValue().getRelation() == VocRelation.RELATIVE && compareAgainst == null) {
			compareAgainst = "$referenceDate";
		}
		String units=null;
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();
		setCompare(match, comp, value,units,compareAgainst);
	}

	private void setCompare(Match match, Comparison comp,String value,String units,String compareAgainst) {
			match.setValueCompare(comp,value);
			if (compareAgainst!=null){
				Function function = getTimeDiff(units, compareAgainst, match.getValueVar());
				if (function != null) {
					match.setValueFunction(function);
				}
			}
			if (match.getProperty().equals(TTIriRef.iri(IM.NAMESPACE+"age"))){
				Function function = new Function();
				match.setValueFunction(function);
				function.setId(TTIriRef.iri(IM.NAMESPACE+"AgeFunction"));
				function.addArgument(new Argument().setParameter("units").setValueData(units));
			}
	}

	private Function getTimeDiff(String units,String firstDate,String compareAgainst){
		Function function=null;
		if (compareAgainst!=null) {
			function = new Function().setId(TTIriRef.iri(IM.NAMESPACE + "TimeDifference"));
			function.addArgument(new Argument().setParameter("units").setValueData(units));
			function.addArgument(new Argument().setParameter("firstDate").setValueData(firstDate));
			function.addArgument(new Argument().setParameter("secondDate").setValueData(compareAgainst));
		}
		return function;
	}

	private void setRangeCompare(Match match, EQDOCRangeFrom rFrom,EQDOCRangeTo rTo,String compareAgainst) {
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.EQUAL;
		String fromValue= rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation()!=null && rFrom.getValue().getRelation() == VocRelation.RELATIVE && compareAgainst == null) {
			compareAgainst = "$referenceDate";
		}
		String units= null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();
		if (compareAgainst==null)
			compareAgainst="$referenceDate";
		Function fromFunction= getTimeDiff(units,compareAgainst,match.getValueVar());
		Comparison toComp;
		if (rTo.getOperator()!=null)
			toComp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp= Comparison.EQUAL;
		String toValue= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null) {
			if (rTo.getValue().getRelation() == VocRelation.RELATIVE) {
				if (compareAgainst == null)
					compareAgainst = "$referenceDate";
			}
		}
		units= null;
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();
		if (compareAgainst==null)
			compareAgainst="$referenceDate";
		Function toFunction= getTimeDiff(units,compareAgainst,match.getValueVar());
		setRangeCompareFilter(match,fromComp,fromValue,fromFunction,
			toComp,toValue,toFunction);


	}

	private void setRangeCompareFilter (Match match, Comparison fromComp, String fromValue, Function fromFunction, Comparison toComp, String toValue, Function toFunction){

		match
			.setValueRange(new Range()
				.setFrom(new Compare().setComparison(fromComp)
				.setValue(fromValue)
				.setFunction(fromFunction))
			.setTo(new Compare().setComparison(toComp)
				.setValue(toValue)
				.setFunction(toFunction)));
	}

	private String getMap(String from) throws DataFormatException {
		Object target = dataMap.get(from);
		if (target == null)
			throw new DataFormatException("unknown map : " + from);
		return (String) target;
	}

	private void convertLinkedCriterion(EQDOCLinkedCriterion eqLinked, Match match,String linkField) throws DataFormatException, IOException {

		Match subMatch= new Match();
		match.addAnd(subMatch);
		String firstDate= dateMatch;
		dateMatch=null;
		convertCriterion(eqLinked.getCriterion(), subMatch,linkField);
		if (dateMatch == null)
			throw new DataFormatException("no date restriction or date test for linked criterion in " + activeReport);
		EQDOCRelationship eqRel = eqLinked.getRelationship();
		if (!eqRel.getParentColumn().contains("DATE"))
			throw new DataFormatException("Only date columns supported in linked criteria : " + activeReport);
		subMatch= new Match();
		match.addAnd(subMatch);
		String units= eqRel.getRangeValue().getRangeFrom().getValue().getUnit().value();
		VocRangeFromOperator eqOp= eqRel.getRangeValue().getRangeFrom().getOperator();
		String value= eqRel.getRangeValue().getRangeFrom().getValue().getValue();
		Function function= getTimeDiff(units,firstDate,dateMatch);
		subMatch.setValueFunction(function);
		subMatch.setValueCompare(new Compare().setComparison((Comparison) vocabMap.get(eqOp))
			.setValueData(value));
	}

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
		/*
		TTEntity duplicateOf = getDuplicateSet(valueSet);
		if (duplicateOf!=null){
			iri= TTIriRef.iri(duplicateOf.getIri());
			iri.setName(duplicateOf.getName());
			return iri;
		}

		 */
		valueSet.addObject(IM.USED_IN,TTIriRef.iri(reportMap.get(activeReport).getIri()));
		document.addEntity(valueSet);
		valueSets.add(valueSet);
		return iri;
	}
	private Set<TTIriRef> getValue(VocCodeSystemEx scheme,EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(),ev.getDisplayName(),ev.getLegacyValue());
	}

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

	private void setParent(Match clause, TTIriRef parent, String parentName) {
		if (parentName!=null)
			clause.setName(parentName);
		clause.setProperty(IM.HAS_PROFILE)
			.addValueIn(parent);
	}

	private void setVocabMaps() {
		vocabMap.put(VocRangeFromOperator.GTEQ, Comparison.GREATER_THAN_OR_EQUAL);
		vocabMap.put(VocRangeFromOperator.GT, Comparison.GREATER_THAN);
		vocabMap.put(VocRangeToOperator.LT, Comparison.LESS_THAN);
		vocabMap.put(VocRangeToOperator.LTEQ, Comparison.LESS_THAN_OR_EQUAL);
		vocabMap.put(VocOrderDirection.DESC, SortOrder.DESCENDING);
		vocabMap.put(VocOrderDirection.ASC, SortOrder.ASCENDING);
	}

}
