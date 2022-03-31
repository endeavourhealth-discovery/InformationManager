package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.collections4.CollectionUtils;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.query.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import javax.swing.*;
import java.io.IOException;
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
	String dateVar;
	Match dateMatch;
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
		TTEntity entity= new TTEntity().addType(IM.QUERY);
		reportMap.put(activeReport,entity);
		entity.setIri("urn:uuid:" + eqReport.getId());
		entity.setName(eqReport.getName());
		entity.setDescription(eqReport.getDescription());
		if (eqReport.getFolder()!=null)
			entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri("urn:uuid:"+ eqReport.getFolder()));
		if (eqReport.getCreationTime()!=null)
			setProvenance(entity.getIri(), null);

		if (eqReport.getPopulation() != null) {
			Query profile= new Query();
			profile.setName(entity.getName());
			profile.setId(TTIriRef.iri(entity.getIri()));
			profile.setDescription(entity.getDescription());
			Return ret= new Return();
			ret.addField(new ReturnField().setPath("id"));
			profile.setReturn(ret);
			Match match= profile.setMatch();
			match.setEntityType(TTIriRef.iri(IM.NAMESPACE+"Person"));
			Match parentClause= match.addAnd();
			parentClause.setId(TTIriRef.iri("urn:uuid:"+ UUID.randomUUID()));
			if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
				setParent(parentClause, TTIriRef.iri(IM.NAMESPACE+"Q_RegisteredGMS"), "Registered with GP for GMS services on the reference date");
			}
			if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
				String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
				setParent(parentClause,TTIriRef.iri("urn:uuid:" + id), reportNames.get(id));
			}
			convertPopulation(eqReport.getPopulation(), match);
			entity.set(IM.DEFINITION,TTLiteral.literal(IMQLFactory.getJson(profile)));
		}
		return entity;
	}

	private void convertPopulation(EQDOCPopulation population, Match main) throws DataFormatException, IOException {
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
				andNotClause.addOr(thisMatch);
				thisMatch.setNotExists(true);
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
				dateVar=null;
				if (eqCount == 1) {
					setCriteria(eqCriteria, thisMatch,main);
				}
				else if (memberOp == VocMemberOperator.OR) {
						Match orMatch = new Match();
					if (groupOp== Operator.OR) {
						parentOr.addOr(orMatch);
					}
					else {
						thisMatch.addOr(orMatch);
					}
						setCriteria(eqCriteria, orMatch,null);
					}
				else if (memberOp== VocMemberOperator.AND){
						Match andMatch = new Match();
						thisMatch.addAnd(andMatch);
						setCriteria(eqCriteria, andMatch,main);
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
													 Match match,Match parentMatch) throws DataFormatException, IOException {
		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			match
				.setProperty(IM.IN_RESULT_SET)
				.addValueIn(TTIriRef.iri("urn:uuid:" + srch.getReportGuid())
					.setName(reportNames.get(srch.getReportGuid())));
		}
		else {
			if (eqCriteria.getCriterion().getId() != null)
				match.setId(TTIriRef.iri("urn:uuid:" + eqCriteria.getCriterion().getId()));
			if (eqCriteria.getCriterion().getLinkedCriterion() == null) {
				convertCriterion(eqCriteria.getCriterion(), match,
					eqCriteria.getCriterion().getFilterAttribute().getRestriction() != null, parentMatch);
			} else {
				Match subMatch;
				if (parentMatch != null)
					subMatch = parentMatch.addAnd();
				else
					subMatch = match.addAnd();
				convertCriterion(eqCriteria.getCriterion(), subMatch,
					true, parentMatch);
				convertLinkedCriterion(eqCriteria.getCriterion().getLinkedCriterion(),
					match, parentMatch);
			}
		}
	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Match match,boolean needsDate,
																Match parentMatch) throws DataFormatException, IOException {
		if (criteriaLabels.get(eqCriterion.getId()) != null) {
			match.setName(criteriaLabels.get(eqCriterion.getId()).toString());
		}
		if (eqCriterion.isNegation()) {
			match.setNotExists(true);
		}
		if (eqCriterion.getDescription() != null)
			match.setDescription(eqCriterion.getDescription());
		processColumns(eqCriterion.getFilterAttribute(), eqCriterion.getTable(), match,
				needsDate,parentMatch);
	}




	private void setRestriction(String eqTable,EQDOCFilterRestriction restrict,Match match) throws DataFormatException, IOException {
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC)
			match.setSort(new Sort()
					.setDirection(Order.ASCENDING));
		else
			match.setSort(new Sort()
				.setDirection(Order.DESCENDING));
		String eqColumn = restrict.getColumnOrder().getColumns().get(0).getColumn().get(0);
		String fieldPath = dataMap.getProperty(eqTable + slash + eqColumn);
		String field=fieldPath.substring(fieldPath.lastIndexOf("/")+1);
		match.getSort()
				.setOrderBy(TTIriRef.iri(IM.NAMESPACE + field));
		match.getSort().setCount(restrict.getColumnOrder().getRecordCount());
		if (restrict.getTestAttribute() != null) {
			Match testMatch= new Match();
			match.setTest(testMatch);
			List<EQDOCColumnValue> cvs = restrict.getTestAttribute().getColumnValue();
			if (cvs.size() == 1) {
				setMainCriterion(eqTable, cvs.get(0), testMatch);
			} else {
				for (EQDOCColumnValue cv : cvs) {
					Match subMatch = new Match();
					testMatch.addAnd(subMatch);
					setMainCriterion(eqTable, cv, subMatch);
				}
			}
		}
	}

	private void processColumns(EQDOCFilterAttribute eqAtt, String eqTable,Match match,
															boolean needsDate,Match parentMatch) throws DataFormatException, IOException {
		Map<String,Map<String,EQDOCColumnValue>> pathEntities= new HashMap<>();
		for (EQDOCColumnValue cv : eqAtt.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				String[] entityPath = dataMap.getProperty(eqTable + slash + eqColumn).split("/");
				String pathEntity= entityPath[0]+ slash+entityPath[1];
				String property= entityPath[2];
				pathEntities.computeIfAbsent(pathEntity, m-> new HashMap<>());
				pathEntities.get(pathEntity).put(property,cv);
			}
		}
		if (pathEntities.size()==1) {
			String pathEntity = pathEntities.keySet().stream().findFirst().get();
			match.setProperty(getIri(pathEntity.split("/")[0]));
			Match entityMatch = new Match();
			match.setObject(entityMatch);
			entityMatch.setEntityType(getIri(pathEntity.split("/")[1]));
			Map<String, EQDOCColumnValue> columnMap = pathEntities.get(pathEntity);
			processColumnMap(eqTable,entityMatch,columnMap,needsDate);
			if (eqAtt.getRestriction()!=null){
				setRestriction(eqTable,eqAtt.getRestriction(),entityMatch);
			}
		}
		else{
			if (eqAtt.getRestriction()!=null)
				throw new DataFormatException("No pattern for multi table restrictions "+ activeReport);
				int i = 0;
				for (Map.Entry<String, Map<String, EQDOCColumnValue>> entry : pathEntities.entrySet()) {
					i++;
					String pathEntity = entry.getKey();
					String path = pathEntity.split("/")[0];
					String entity = pathEntity.split("/")[1];
					Match pathMatch;
					if (parentMatch != null) {
						if (i > 1) {
							pathMatch = parentMatch.addAnd();
							pathMatch.setId(TTIriRef.iri("urn:uuid:" + UUID.randomUUID()));
						} else
							pathMatch = match;
					} else {
						pathMatch = match.addAnd();
						pathMatch.setId(TTIriRef.iri("urn:uuid:" + UUID.randomUUID()));
					}
					pathMatch.setProperty(getIri(path));
					Match entityMatch = new Match();
					pathMatch.setObject(entityMatch);
					entityMatch.setEntityType(getIri(entity));
					Map<String, EQDOCColumnValue> columnMap = pathEntities.get(pathEntity);
					processColumnMap(eqTable,entityMatch,columnMap,needsDate);
				}
			}
		}

	private void processColumnMap(String eqTable, Match entityMatch, Map<String, EQDOCColumnValue> columnMap,
																boolean needsDate) throws DataFormatException, IOException {
		if (columnMap.size() == 1) {
			String property = columnMap.keySet().stream().findFirst().get();
			EQDOCColumnValue cv = columnMap.get(property);
			if (needsDate) {
				Match subMatch = entityMatch.addAnd();
				subMatch.setProperty(getIri(property));
				setMainCriterion(eqTable, cv, subMatch);
				dateMatch = entityMatch.addAnd();
				EQDOCColumnValue colVal = new EQDOCColumnValue();
				colVal.getColumn().add("DATE");
				setMainCriterion(eqTable, colVal, dateMatch);
			} else
				setMainCriterion(eqTable, cv, entityMatch);
		} else {
			for (Map.Entry<String, EQDOCColumnValue> entry : columnMap.entrySet()) {
				String property = entry.getKey();
				EQDOCColumnValue cv = entry.getValue();
				Match subMatch = entityMatch.addAnd();
				subMatch.setProperty(getIri(property));
				if (property.equals("effectiveDate"))
					dateMatch= subMatch;
				setMainCriterion(eqTable, cv, subMatch);
			}
			if (needsDate && dateMatch == null) {
				dateMatch = entityMatch.addAnd();
				EQDOCColumnValue colVal = new EQDOCColumnValue();
				colVal.getColumn().add("DATE");
				setMainCriterion(eqTable, colVal, dateMatch);
			}
		}
	}



	private TTIriRef getIri(String lname) throws IOException {
	 TTIriRef result= TTIriRef.iri(IM.NAMESPACE+lname);
	 String name= importMaps.getCoreName(result.getIri());
	 if (name!=null)
		 result.setName(name);
	 return result;

	}



	private void setMainCriterion(String eqTable,EQDOCColumnValue cv,Match match) throws DataFormatException, IOException {
		for (String eqColumn : cv.getColumn()) {
			setPropertyValue(cv, eqTable, eqColumn, match);
			if (eqColumn.contains("DATE"))
				dateVar= match.getVar();
		}
	}


	private void setPropertyValue(EQDOCColumnValue cv,String eqTable,String eqColumn,
																Match match) throws DataFormatException, IOException {
		String predPath= dataMap.getProperty(eqTable + slash + eqColumn);
		String predicate= predPath.substring(predPath.lastIndexOf("/")+1);
		match.setProperty(TTIriRef.iri(IM.NAMESPACE+ predicate));
		if (match.getProperty().getIri().equals(IM.NAMESPACE+"age")){
			match.addArgument(new Argument()
				.setParameter("from")
				.setValue(TTIriRef.iri(IM.NAMESPACE+"dateOfBirth")));
			match.addArgument(new Argument()
				.setParameter("to")
				.setValue("$referenceDate"));
			if (cv.getRangeValue()!=null)
				if (cv.getRangeValue().getRangeFrom()!=null) {
					if (cv.getRangeValue().getRangeFrom().getValue().getUnit() != null)
						match.addArgument(new Argument()
							.setParameter("units")
							.setValue(cv.getRangeValue().getRangeFrom().getValue().getUnit().value()));
				}
			  else {
					if (cv.getRangeValue().getRangeTo()!=null)
						if (cv.getRangeValue().getRangeTo().getValue().getUnit()!=null)
							match.addArgument(new Argument()
								.setParameter("units")
								.setValue(cv.getRangeValue().getRangeTo().getValue().getUnit().value()));
				}

		}
		VocColumnValueInNotIn in= cv.getInNotIn();
		varCounter++;
		match.setVar(predicate+varCounter);
		boolean notIn= (in== VocColumnValueInNotIn.NOTIN);
		if (!cv.getValueSet().isEmpty()){
			for (EQDOCValueSet vs:cv.getValueSet()) {
				if (vs.getAllValues()!=null){
					match.setValueNotIn(getExceptionSet(vs.getAllValues()));
				}
				else {
					if (!notIn)
						match.setValueIn(getValueSet(vs));
					else
						match.setValueNotIn(getValueSet(vs));
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
			setRangeValue(cv.getRangeValue(),match);
		}
	}


	private void setRangeValue(EQDOCRangeValue rv, Match match) {

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

	private void setCompareFrom(Match match, EQDOCRangeFrom rFrom) {
		Comparison comp;
		String compareAgainst=null;
		if (rFrom.getOperator() != null)
			comp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp = Comparison.EQUAL;
		String value = rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
			compareAgainst = "$referenceDate";
		}
		String units=null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();
		setCompare(match, comp, value,units,compareAgainst);
	}

	private void setCompareTo(Match match,EQDOCRangeTo rTo) {
		Comparison comp;
		String compareAgainst= null;
		if (rTo.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp= Comparison.EQUAL;
		String value= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
			compareAgainst = "$referenceDate";
		}
		String units=null;
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();
		setCompare(match, comp, value,units,compareAgainst);
	}

	private void setCompare(Match match, Comparison comp,String value,String units,String compareAgainst) {
			match.setCompare(comp,value);
			match.getValueIs().setUnits(units);
			if (compareAgainst!=null){
				match.getValueIs().setCompareWith(compareAgainst);
			}
	}



	private void setRangeCompare(Match match, EQDOCRangeFrom rFrom,EQDOCRangeTo rTo){
		String compareAgainst=null;
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.EQUAL;
		String fromValue= rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation()!=null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
			compareAgainst = "$referenceDate";
		}
		String units= null;
		if (rFrom.getValue().getUnit()!=null)
			units= rFrom.getValue().getUnit().value();

		Comparison toComp;
		if (rTo.getOperator()!=null)
			toComp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp= Comparison.EQUAL;
		String toValue= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null) {
			if (rTo.getValue().getRelation() == VocRelation.RELATIVE) {
					compareAgainst = "$referenceDate";
			}
		}
		if (rTo.getValue().getUnit()!=null)
			units= rTo.getValue().getUnit().value();

		match
			.setValueRange(new Range()
				.setFrom(new Compare().setComparison(fromComp)
					.setUnits(units)
				.setValue(fromValue)));
			if (compareAgainst!=null)
				match.getValueRange().getFrom().setCompareWith(compareAgainst);
			match.getValueRange().setTo(new Compare().setComparison(toComp)
					.setUnits(units)
				.setValue(toValue));
			if (compareAgainst!=null)
				match.getValueRange().getTo().setCompareWith(compareAgainst);
	}



	private void convertLinkedCriterion(EQDOCLinkedCriterion eqLinked, Match match,Match parentMatch) throws IOException, DataFormatException {
		EQDOCRangeValue eqRange= eqLinked.getRelationship().getRangeValue();
		Comparison comp= (Comparison) vocabMap.get(eqRange.getRangeFrom().getOperator());
		String eqTable= eqLinked.getCriterion().getTable();
		EQDOCColumnValue cv= eqLinked.getCriterion().getFilterAttribute().getColumnValue().get(0);
		String eqColumn= cv.getColumn().get(0);
		String[] pep= dataMap.getProperty(eqTable+slash+ eqColumn).split("/");
		Match pathMatch;
		if (parentMatch!=null){
			pathMatch= parentMatch.addAnd();
		}
		else
		  pathMatch= match.addAnd();
		pathMatch.setProperty(getIri(pep[0]));
		Match entityMatch= new Match();
		pathMatch.setObject(entityMatch);
		entityMatch.setEntityType(getIri(pep[1]));
		dateMatch = entityMatch.addAnd();
		EQDOCColumnValue colVal = new EQDOCColumnValue();
		colVal.getColumn().add("DATE");
		setMainCriterion(eqTable, colVal, dateMatch);
		Match subMatch= entityMatch.addAnd();
		subMatch.setProperty(getIri(pep[2]));
		setMainCriterion(eqTable,cv,subMatch);
		subMatch.setValueWithin(new Within());
		subMatch.getValueWithin().setComparison(comp)
			.setValue(eqRange.getRangeFrom().getValue().getValue()
				)
			.setUnits(eqRange.getRangeFrom().getValue().getUnit().value())
			.setOf(dateMatch.getVar());
	}

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


	private List<TTIriRef> getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTIriRef> valueSet = new ArrayList<>();
		StringBuilder vsetName = new StringBuilder();
		if (vs.getDescription() != null)
			vsetName = new StringBuilder(vs.getDescription());
		VocCodeSystemEx scheme = vs.getCodeSystem();
		int i = 0;
		for (EQDOCValueSetValue ev : vs.getValues()) {
			i++;
			if (i < 10) {
				if (ev.getDisplayName() != null) {
					if (vsetName.length() != 0)
						vsetName.append(", ");
					vsetName.append(ev.getDisplayName());
				}
			}
			Set<TTIriRef> concepts = getValue(scheme, vs.getValues().get(0));
			if (concepts != null) {
				valueSet.addAll(new ArrayList<>(concepts));
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());


		}
		return valueSet;
	}

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme,EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(),ev.getDisplayName(),ev.getLegacyValue());
	}

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme, String originalCode,
																 String originalTerm,String legacyCode) throws DataFormatException, IOException {
		if (scheme== VocCodeSystemEx.EMISINTERNAL) {
			String key = "EMISINTERNAL/" + originalCode;
			Object mapValue = dataMap.get(key);
			if (mapValue != null) {
				TTIriRef iri= TTIriRef.iri(mapValue.toString());
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
		if (parentName!=null) {
			clause.setName(parentName);
			parent.setName(parentName);
		}
		clause.setProperty(IM.IN_RESULT_SET)
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
