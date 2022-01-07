package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.eqd.*;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToQuery {
	public static Properties duplicates;
	private int varCounter;
	private String activeReport;
	private Properties dataMap;
	private Properties criteriaLabels;
	private String mainSubject;
	private TTDocument document;
	private Map<String,String> reportNames;
	private final Set<String> clauseIris= new HashSet<>();
	private final Map<Object, Object> vocabMap = new HashMap<>();


	public Query convertReport(EQDOCReport eqReport, TTDocument document,
														 Properties dataMap, Properties criteriaLabels,
														 Map<String,String> reportNames) throws DataFormatException, JsonProcessingException {
		this.dataMap = dataMap;
		this.document = document;
		this.criteriaLabels = criteriaLabels;
		this.reportNames = reportNames;
		setVocabMaps();
		activeReport = eqReport.getId();
		Query qry = new Query();
		qry.setIri("urn:uuid" + eqReport.getId());
		qry.setType(IM.QUERY);
		qry.setName(eqReport.getName());
		reportNames.put(eqReport.getId(), eqReport.getName());
		qry.setDescription(eqReport.getDescription());
		qry.setMainEntityType(TTIriRef.iri(IM.NAMESPACE+"Patient"));
		qry.setMainEntityVar("?patient");
		mainSubject="?patient";
		qry.setOperator(Operator.AND);
		Select select = new Select();
		qry.addSelect(select);
		select.setVar("?patient");
		if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
			Clause parentClause = new Clause();
			qry.addClause(parentClause);
			setParent(parentClause, TTIriRef.iri("Q_RegisteredGMS"), "Registered with GP for GMS services on the reference date");
		}
		if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
			Clause parentClause = new Clause();
			qry.addClause(parentClause);
			String id = eqReport.getParent().getSearchIdentifier().getReportGuid();
			setParent(parentClause, TTIriRef.iri("urn:uuid:" + id), "is in cohort : " + reportNames.get(id));
		}
		if (eqReport.getPopulation() != null)
			convertPopulation(eqReport.getPopulation(), qry);
		return qry;
	}

	private void convertPopulation(EQDOCPopulation population, Query qry) throws DataFormatException, JsonProcessingException {
		Operator lastGroupOp = null;
		for (EQDOCCriteriaGroup eqGroup : population.getCriteriaGroup()) {
			VocRuleAction ifTrue = eqGroup.getActionIfTrue();
			VocRuleAction ifFalse = eqGroup.getActionIfFalse();
			Clause groupClause;
			Clause thisClause;
			if (ifFalse == VocRuleAction.REJECT & lastGroupOp != Operator.OR){
				groupClause = new Clause();
				thisClause = groupClause;
				qry.addClause(groupClause);
			}
			else if (ifFalse == VocRuleAction.REJECT & (lastGroupOp == Operator.OR)) {
				groupClause = qry.getClause().get(qry.getClause().size() - 1);
				thisClause = new Clause();
				groupClause.addClause(thisClause);
			}

			else if (ifFalse == VocRuleAction.NEXT & ifTrue == VocRuleAction.REJECT) {
				if (lastGroupOp==Operator.NOTOR){
					groupClause = qry.getClause().get(qry.getClause().size() - 1);
				}
				else {
					groupClause= new Clause();
					qry.addClause(groupClause);
					groupClause.setOperator(Operator.NOTOR);
				}
				thisClause = new Clause();
				groupClause.addClause(thisClause);
				lastGroupOp= Operator.NOTOR;
			}
			else if (ifFalse == VocRuleAction.NEXT & ifTrue == VocRuleAction.SELECT) {
				if (lastGroupOp!= Operator.OR){
					groupClause= new Clause();
					qry.addClause(groupClause);
					groupClause.setOperator(Operator.OR);
				}
				else {
					groupClause = qry.getClause().get(qry.getClause().size() - 1);
				}
				thisClause= new Clause();
				groupClause.addClause(thisClause);
				lastGroupOp= Operator.OR;
			}
			else if (ifFalse == VocRuleAction.SELECT & ifTrue == VocRuleAction.REJECT) {
				groupClause= new Clause();
				groupClause.setOperator(Operator.NOTAND);
				thisClause = new Clause();
				groupClause.addClause(thisClause);
			}
			else if (ifFalse == VocRuleAction.SELECT & (ifTrue == VocRuleAction.NEXT)) {
				if (lastGroupOp==Operator.NOTOR){
					groupClause = qry.getClause().get(qry.getClause().size() - 1);
				}
				else {
					groupClause= new Clause();
					qry.addClause(groupClause);
					groupClause.setOperator(Operator.NOTOR);
				}
				thisClause = new Clause();
				groupClause.addClause(thisClause);
				lastGroupOp= Operator.NOTOR;
			}
			else
				throw new DataFormatException("unrecognised action rule combination : "+ activeReport);
			VocMemberOperator memberOp = eqGroup.getDefinition().getMemberOperator();
			int eqCount= eqGroup.getDefinition().getCriteria().size();
			for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
				boolean notExist=false;
				if (eqCriteria.getCriterion()!=null) {
					if (eqCriteria.getCriterion().isNegation()) {
						notExist = true;
					}
				}

				if (eqCount>1){
					thisClause.setOperator(memberOp== VocMemberOperator.AND ?Operator.AND : Operator.OR);
					Clause newClause= new Clause();
					if (notExist)
						newClause.setNotExist(true);
					thisClause.addClause(newClause);
					processCriteria(eqCriteria,newClause);
				}
				else {
					if (notExist)
						thisClause.setNotExist(true);
					processCriteria(eqCriteria,thisClause);
				}
			}
		}
	}




	private void processCriteria(EQDOCCriteria eqCriteria,Clause clause) throws DataFormatException, JsonProcessingException {
				if ((eqCriteria.getPopulationCriterion() != null)) {
					EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
					clause.addWhere (new Where()
						.setEntityVar("?patient")
						.setProperty(IM.IN_DATASET)
						.setValueEntity(TTIriRef.iri("urn:uuid:" + srch.getReportGuid()).setName("is in cohort : "+ reportNames.get(srch.getReportGuid()))));
				} else {
					convertCriterion(eqCriteria.getCriterion(), clause);
				}
	}


	private String setWheres(EQDOCColumnValue cv,Clause clause, String thisPath,String lastPath,Map<String,String> restrictionMap) throws DataFormatException {
		String entityVar;
		TTIriRef thisEntity= null;
		if (mainSubject!=null){
			entityVar=mainSubject;
		}
		else {
			thisEntity= TTIriRef.iri(IM.NAMESPACE+"Patient");
			entityVar="?patient";
		}

		String lastSubject=null;
		String thisSubject= thisPath.substring(0,thisPath.lastIndexOf("/"));
		if (lastPath!=null)
			lastSubject= lastPath.substring(0,lastPath.lastIndexOf("/"));
		String[] path= thisPath.split("/");
		if (thisSubject.equals(lastSubject)){
			entityVar= clause.getWhere().get(clause.getWhere().size()-1).getEntityVar();
		}
		else {
				for (int i = 0; i < path.length - 2; i = i + 2) {
					String var = null;
					if (path[i].contains("~")) {
						String thisEntityVar= "?"+ path[i].split("~")[1];
						if (thisEntityVar.equals(mainSubject))
							var= mainSubject;
						else {
							varCounter++;
							var = "?" + path[i].split("~")[1] + varCounter;
						}
					}
					Where where = new Where();
					clause.addWhere(where);
					if (var!=null) {
						if (!var.equals(mainSubject))
							where.setEntity(TTIriRef.iri(IM.NAMESPACE + path[i].split("~")[0]));
						where.setEntityVar(var);
					}
					String predicate = path[i + 1];
					where.setProperty(TTIriRef.iri(IM.NAMESPACE + predicate));
					String valueEntity = path[i + 2];
					varCounter++;
					String valueVar = "?" + predicate + varCounter;
					if (valueEntity.contains("~")){
						valueVar= "?"+ valueEntity.split("~")[1]+varCounter;
						valueEntity= valueEntity.split("~")[0];
					}
					where.setValueEntity(TTIriRef.iri(IM.NAMESPACE + valueEntity));
					where.setValueVar(valueVar);
					entityVar = valueVar;
				}
		}
		Where where=new Where();
		clause.addWhere(where);
		where.setEntityVar(entityVar);
		if (thisEntity!=null)
			where.setEntity(thisEntity);
		String predicate= path[path.length-1];
		where.setProperty(TTIriRef.iri(IM.NAMESPACE+predicate));
		varCounter++;
		String valueVar="?"+ predicate+varCounter;
		where.setValueVar(valueVar);
		restrictionMap.put(predicate,valueVar);
		convertFilter(cv, where);
		return entityVar;

	}

	private String getMap(String from) throws DataFormatException {
		Object target = dataMap.get(from);
		if (target == null)
			throw new DataFormatException("unknown map : " + from);
		return (String) target;
	}

	private String deDuplicate(String iri){
		if (EqdToQuery.duplicates!=null){
			Object dupl= EqdToQuery.duplicates.get(iri);
			if (dupl!=null)
				return dupl.toString();
		}
		return iri;

	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Clause clause) throws DataFormatException, JsonProcessingException {
		Map<String,String> restrictionMap= new HashMap<>();
		String eqTable = eqCriterion.getTable();
		Clause superClause=null;

		EQDOCFilterAttribute eqAtt = eqCriterion.getFilterAttribute();
		if (eqAtt.getRestriction() != null) {
			superClause = clause;
			Clause subClause = new Clause();
			superClause.addClause(subClause);
			superClause.setIri("urn:uuid:"+ deDuplicate(eqCriterion.getId()));
			clause = subClause;
			if (criteriaLabels.get(eqCriterion.getId()) != null) {
				superClause.setName(criteriaLabels.get(eqCriterion.getId()).toString());
			}
		}
		else {
			clause.setIri("urn:uuid:"+ deDuplicate(eqCriterion.getId()));
			if (criteriaLabels.get(eqCriterion.getId())!=null) {
				clause.setName(criteriaLabels.get(eqCriterion.getId()).toString());
			}
		}
		if (eqCriterion.isNegation()){
			clause.setNotExist(true);
		}
		String entityVar= null;
		String lastPath=null;

		if (eqCriterion.getDescription()!=null)
			clause.setDescription(eqCriterion.getDescription());
		for (EQDOCColumnValue cv : eqAtt.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				String path = getMap(eqTable + "/" + eqColumn);
				entityVar= setWheres(cv,clause,path,lastPath,restrictionMap);
				lastPath=path;
			}
		}
		if (eqAtt.getRestriction() != null) {
			addRestriction(eqAtt, clause, entityVar, restrictionMap);
			if (eqAtt.getRestriction().getTestAttribute()!=null){
				addTest(eqAtt, eqTable,superClause,entityVar,restrictionMap);
			}
			else if (eqCriterion.getLinkedCriterion()!=null){
				convertLinkedCriterion(eqCriterion.getLinkedCriterion(),superClause,restrictionMap);
			}
			if (superClause!=null)
				setNewClauseEntity(superClause);
			else
				setNewClauseEntity(clause);
		}
		else
			setNewClauseEntity(clause);
	}

	private void setNewClauseEntity(Clause clause) throws JsonProcessingException {
		if (!clauseIris.contains(clause.getIri())){
			TTEntity ttClause= new TTEntity()
				.setIri(clause.getIri());
			if (clause.getName()!=null)
				ttClause.setName(clause.getName());
			ttClause.addType(IM.QUERY_CLAUSE);
			document.addEntity(ttClause);
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json = objectMapper.writeValueAsString(clause);
			ttClause.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));
			clauseIris.add(clause.getIri());
		}
	}

	private void addRestriction(EQDOCFilterAttribute eqAtt,
															Clause clause, String entityVar,
															Map<String,String> restrictionMap) throws DataFormatException {

		for (EQDOCColumnOrder.Columns col:eqAtt.getRestriction().getColumnOrder().getColumns()) {
			VocOrderDirection direction = col.getDirection();
			for (String column : col.getColumn()) {
				if (column.contains("DATE")) {
					if (restrictionMap.get("effectiveDate")==null) {
						varCounter++;
						clause.addWhere(new Where()
							.setEntityVar(entityVar)
							.setProperty(TTIriRef.iri(IM.NAMESPACE + "effectiveDate"))
							.setValueVar("?effectiveDate"+varCounter));
						restrictionMap.put("effectiveDate","?effectiveDate"+ varCounter);
					}
					clause.addGroupSort(new GroupSort()
						.setSortBy((SortBy) vocabMap.get(direction))
						.setGroupBy(mainSubject)
						.setField(restrictionMap.get("effectiveDate")));
					clause.addSelect(new Select().setVar("?patient"));
					clause.addSelect(new Select().setVar(restrictionMap.get("effectiveDate")));
				} else
					throw new DataFormatException("only date column restrictions supported id=" + activeReport);
			}
		}

	}

	private void addTest(EQDOCFilterAttribute eqAtt,String eqTable,Clause clause,
											 String entityVar,Map<String,String>   restrictionMap) throws DataFormatException {
		EQDOCTestAttribute eqTest= eqAtt.getRestriction().getTestAttribute();
			for (EQDOCColumnValue cv: eqTest.getColumnValue()) {
				for (String eqColumn : cv.getColumn()) {
				 Where testWhere = new Where();
				 clause.addWhere(testWhere);
				 String[] path = getMap(eqTable + "/" + eqColumn).split("/");
				 testWhere.setEntityVar(entityVar);
				 String testPredicate = path[path.length - 1];
				 testWhere.setProperty(TTIriRef.iri(IM.NAMESPACE + testPredicate));
				 String valueVar = restrictionMap.get(testPredicate);
				 testWhere.setValueVar(valueVar);
				 convertFilter(cv, testWhere);
				 varCounter++;
			 }
		}
	}

	private void convertLinkedCriterion(EQDOCLinkedCriterion eqLinked, Clause clause, Map<String,String>   restrictionMap) throws DataFormatException, JsonProcessingException {
		convertCriterion(eqLinked.getCriterion(),clause);
		EQDOCRelationship eqRel= eqLinked.getRelationship();
		if (!eqRel.getParentColumn().contains("DATE"))
			throw new DataFormatException("Only date columns supported in linked criteria : "+ activeReport);
		Where linkWhere= clause.getWhere().get(clause.getWhere().size()-1);
		Where getDate= new Where();
		clause.addWhere(getDate);
		getDate.setEntityVar(linkWhere.getEntityVar());
		getDate.setProperty(TTIriRef.iri(IM.NAMESPACE+"effectiveDate"));
		varCounter++;
		getDate.setValueVar("?effectiveDate"+varCounter);
		Filter relationship= new Filter();
		getDate.addFilter(relationship);
		convertRangeValue(relationship,"?effectiveDate"+ varCounter,eqRel.getRangeValue(),restrictionMap.get("effectiveDate"));


	}

	private void convertFilter(EQDOCColumnValue cv,
														 Where where) throws DataFormatException {
		VocColumnValueInNotIn in= cv.getInNotIn();
		if (!cv.getValueSet().isEmpty()){
			Filter filter= new Filter();
			where.addFilter(filter);
			for (EQDOCValueSet vs:cv.getValueSet()) {
				if (vs.getAllValues()!=null){
					filter.addNotIn(getExceptionSet(vs.getAllValues()));

				}
				else {
					if (in == VocColumnValueInNotIn.IN)
						filter.addIn(getValueSet(vs));
					else
						filter.addNotIn(getValueSet(vs));
				}
			}
		}
		else
		if (!ImportUtils.isEmpty(cv.getLibraryItem())) {
			Filter filter= new Filter();
			where.addFilter(filter);
			for (String vset : cv.getLibraryItem()) {
				if (in == VocColumnValueInNotIn.IN)
					filter.addIn(TTIriRef.iri("urn:uuid:" + vset));
				else
					filter.addNotIn(TTIriRef.iri("urn:uuid:" + vset));
			}
		}
		else if (cv.getRangeValue()!=null){
			Filter filter= new Filter();
			where.addFilter(filter);
			convertRangeValue(filter,where.getValueVar(),cv.getRangeValue(),null);
		}
		varCounter++;
	}

	private TTIriRef getExceptionSet(EQDOCException set) throws DataFormatException {
		TTEntity valueSet = new TTEntity();
		String iri = "urn:uuid:" + UUID.randomUUID();
		valueSet.setIri(iri);
		valueSet.addType(IM.CONCEPT_SET);
		document.addEntity(valueSet);
		VocCodeSystemEx scheme= set.getCodeSystem();
		for (EQDOCExceptionValue ev:set.getValues()){
			valueSet.addObject(IM.DEFINITION,getValue(scheme,ev.getValue()));
		}
		return TTIriRef.iri(iri);
	}

	private TTIriRef getValueSet(EQDOCValueSet vs) throws DataFormatException {
		TTEntity valueSet = new TTEntity();
		TTIriRef iri = TTIriRef.iri("urn:uuid:" + UUID.randomUUID());
		String vsetName=null;
		if (vs.getDescription()!=null)
			vsetName= vs.getDescription();
		valueSet.setIri(iri.getIri());
		valueSet.addType(IM.CONCEPT_SET);
		document.addEntity(valueSet);
		VocCodeSystemEx scheme = vs.getCodeSystem();
		if (vs.getValues().size() == 1) {
			if (vsetName==null)
				vsetName= vs.getValues().get(0).getDisplayName()+ " ....";
			valueSet.addObject(IM.DEFINITION, getValue(scheme, vs.getValues().get(0)));
		} else {
			TTNode orSet = new TTNode();
			valueSet.addObject(IM.DEFINITION, orSet);
			for (EQDOCValueSetValue ev : vs.getValues()) {
				if (vsetName==null)
					vsetName= ev.getDisplayName()+ " ....";
				orSet.addObject(SHACL.OR, getValue(scheme, ev));
			}
		}
		if (vsetName!=null) {
			iri.setName(vsetName);
			valueSet.setName(iri.getName());
		}
		return iri;
	}
	private TTIriRef getValue(VocCodeSystemEx scheme,EQDOCValueSetValue ev) throws DataFormatException {
		return getValue(scheme, ev.getValue());
	}

	private TTIriRef getValue(VocCodeSystemEx scheme, String originalCode) throws DataFormatException {
			if (scheme== VocCodeSystemEx.EMISINTERNAL) {
					String key = "EMISINTERNAL/" + originalCode;
					Object mapValue = dataMap.get(key);
					if (mapValue != null) {
						if (mapValue.toString().startsWith("sn:")){
							mapValue= SNOMED.NAMESPACE+mapValue.toString().substring(3);
						}
						else
							mapValue=IM.NAMESPACE+mapValue;
						return TTIriRef.iri(mapValue.toString());
					}
					else
						throw new DataFormatException("unmapped emis internal code : "+key);
			}
			else if (scheme==VocCodeSystemEx.SNOMED_CONCEPT| scheme.value().contains("SCT")){
				return TTIriRef.iri("sn:"+ originalCode);
			}
			else
				throw new DataFormatException("code scheme not recognised : "+scheme.value());

	}

	private void setCompare(Filter filter, String valueVar,EQDOCRangeFrom rFrom,String compareAgainst){
		Comparison comp;
		if (rFrom.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp= Comparison.equal;
		String value= rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation()!=null) {
			if (rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				if (compareAgainst == null)
					compareAgainst = "$referenceDate";
			}
		}
		Function function=null;
		List<Argument> arguments = null;
		if (compareAgainst!=null) {
			function = new Function().setName(TTIriRef.iri(IM.NAMESPACE + "TimeDifference"));
			arguments= new ArrayList<>();
			arguments.add(new Argument().setParameter("units").setValue(rFrom.getValue().getUnit().value()));
			arguments.add(new Argument().setParameter("firstDate").setValue(valueVar));
			arguments.add(new Argument().setParameter("secondDate").setValue(compareAgainst));
		}
		setCompareFilter(filter,comp,value,function,arguments);
	}

	private void setCompare(Filter filter,String valueVar,EQDOCRangeTo rTo,String compareAgainst){
		Comparison comp;
		if (rTo.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp= Comparison.equal;
		String value= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null) {
			if (rTo.getValue().getRelation() == VocRelation.RELATIVE) {
				if (compareAgainst == null)
					compareAgainst = "$referenceDate";
			}
		}
		Function function=null;
		List<Argument> arguments = null;
		if (compareAgainst!=null) {
			function = new Function().setName(TTIriRef.iri(IM.NAMESPACE + "TimeDifference"));
			arguments= new ArrayList<>();
			arguments.add(new Argument().setParameter("units").setValue(rTo.getValue().getUnit().value()));
			arguments.add(new Argument().setParameter("firstDate").setValue(valueVar));
			arguments.add(new Argument().setParameter("secondDate").setValue(compareAgainst));
		}
		setCompareFilter(filter,comp,value,function,arguments);
	}


	private void setCompareFilter (Filter filter,Object comparison,String value,Function function,
																 List<Argument> arguments){
		filter.setValueTest((Comparison) comparison,value);
		if (function!=null) {
			filter.setFunction(function);
			function.setArgument(arguments);
		}
	}
	private void setRangeCompare(Filter filter,String valueVar,
															 EQDOCRangeFrom rFrom,EQDOCRangeTo rTo,String compareAgainst) {
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.equal;
		String fromValue = rFrom.getValue().getValue();
		if (rFrom.getValue().getRelation()!=null) {
			if (rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				if (compareAgainst == null)
					compareAgainst = "$referenceDate";
			}
		}
		Function fromFunction = null;
		List<Argument> fromArguments = null;
		if (compareAgainst!= null) {
			fromFunction = new Function().setName(TTIriRef.iri(IM.NAMESPACE + "TimeDifference"));
			fromArguments= new ArrayList<>();
			fromArguments.add(new Argument().setParameter("units").setValue(rFrom.getValue().getUnit().value()));
			fromArguments.add(new Argument().setParameter("firstDate").setValue(valueVar));
			fromArguments.add(new Argument().setParameter("secondDate").setValue(compareAgainst));
		}
		Comparison toComp;
		if (rTo.getOperator()!=null)
			toComp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp= Comparison.equal;
		String toValue= rTo.getValue().getValue();
		if (rTo.getValue().getRelation()!=null) {
			if (rTo.getValue().getRelation() == VocRelation.RELATIVE) {
				if (compareAgainst == null)
					compareAgainst = "$referenceDate";
			}
		}
		Function toFunction=null;
		List<Argument> toArguments=null;
		if (compareAgainst!=null){
			toFunction = new Function().setName(TTIriRef.iri(IM.NAMESPACE + "TimeDifference"));
			toArguments= new ArrayList<>();
			toArguments.add(new Argument().setParameter("units").setValue(rTo.getValue().getUnit().value()));
			toArguments.add(new Argument().setParameter("firstDate").setValue(valueVar));
			toArguments.add(new Argument().setParameter("secondDate").setValue(compareAgainst));
		}
		setRangeCompareFilter(filter,fromComp,fromValue,fromFunction,fromArguments,
			toComp,toValue,toFunction,toArguments);


	}
	private void setRangeCompareFilter (Filter filter,
																			Comparison fromComp,String fromValue,
																			Function fromFunction,List<Argument> fromArguments,
																			Comparison toComp,String toValue,
																			Function toFunction,List<Argument> toArguments){

		if (fromFunction!=null)
			fromFunction.setArgument(fromArguments);
		if (toFunction!=null)
			toFunction.setArgument(toArguments);
		filter.setRange(new Range()
			.setFrom(new Compare().setComparison(fromComp)
				.setValue(fromValue)
				.setFunction(fromFunction))
			.setTo(new Compare().setComparison(toComp)
				.setValue(toValue)
				.setFunction(toFunction)));
	}



	private void convertRangeValue(Filter filter, String valueVar, EQDOCRangeValue rv,String compareAgainst) {
		EQDOCRangeFrom rFrom= rv.getRangeFrom();
		EQDOCRangeTo rTo= rv.getRangeTo();
		if (rFrom != null) {
			if (rTo==null) {
				setCompare(filter, valueVar,rFrom,compareAgainst);
			}
			else {
				setRangeCompare(filter,valueVar,
					rFrom,rTo,compareAgainst);
			}
		}
		if (rTo != null) {
			if (rFrom == null) {
				setCompare(filter, valueVar, rTo,compareAgainst);
			}
		}
	}



	private void setParent(Clause clause,TTIriRef parent,String parentName) {
		clause.addWhere( new Where()
			.setEntityVar("?patient")
			.setProperty(IM.IN_DATASET)
				.setValueEntity(parent));
		clause.setName(parentName);
	}


	private void setVocabMaps() {
		vocabMap.put(VocRangeFromOperator.GTEQ, Comparison.greaterThanOrEqual);
		vocabMap.put(VocRangeFromOperator.GT, Comparison.greaterThan);
		vocabMap.put(VocRangeToOperator.LT, Comparison.lessThan);
		vocabMap.put(VocRangeToOperator.LTEQ, Comparison.lessThanOrEqual);
		vocabMap.put(VocOrderDirection.DESC, SortBy.LATEST);
		vocabMap.put(VocOrderDirection.ASC, SortBy.EARLIEST);
	}
}
