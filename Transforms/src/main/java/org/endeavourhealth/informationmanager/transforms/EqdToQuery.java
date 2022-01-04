package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTNode;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.transforms.eqd.*;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToQuery {
	private int varCounter;
	private String activeReport;
	private Properties dataMap;
	private Properties criteriaLabels;
	private Properties duplicateOrs;
	private String mainSubject;
	private TTDocument document;
	private Map<String,String> reportNames;
	private Map<Clause,List<Clause>> orAndOrs = new HashMap<>();
	private final List<Clause> ands = new ArrayList<>();
	private final Clause ors = new Clause();
	private final Clause nots = new Clause();
	private final Map<Clause, List<EQDOCCriteria>> clauseCriteria = new HashMap<>();
	private final Map<Object, Object> vocabMap = new HashMap<>();
	private final Map<String,Clause> duplicateMap= new HashMap<>();

	private enum StepPattern {AND, OR, NOTOR,NOTAND}

	public Query convertReport(EQDOCReport eqReport, TTDocument document,
														 Properties dataMap, Properties duplicateOrs,Properties criteriaLabels,
														 Map<String,String> reportNames) throws DataFormatException, JsonProcessingException {
		this.dataMap = dataMap;
		this.document = document;
		this.criteriaLabels= criteriaLabels;
		this.reportNames= reportNames;
		this.duplicateOrs= duplicateOrs;
		setVocabMaps();
		activeReport = eqReport.getId();
		Query qry = new Query();
		qry.setIri("urn:uuid" + eqReport.getId());
		qry.setType(IM.QUERY);
		qry.setName(eqReport.getName());
		reportNames.put(eqReport.getId(),eqReport.getName());
		qry.setDescription(eqReport.getDescription());
		qry.setOperator(Operator.AND);
		Select select = new Select();
		qry.addSelect(select);
		select.setVar("?patient");
		if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
			Clause parentClause= new Clause();
			ands.add(parentClause);
			setParent(parentClause, TTIriRef.iri("Q_RegisteredGMS"),"Registered with GP for GMS services on the reference date");
		}
		if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
			Clause parentClause= new Clause();
			ands.add(parentClause);
			String id= eqReport.getParent().getSearchIdentifier().getReportGuid();
			setParent(parentClause, TTIriRef.iri("urn:uuid:" + id),"is in cohort : "+ reportNames.get(id));
		}
		setStepPattern(eqReport.getPopulation());
		if (!orAndOrs.isEmpty()){
			for (Map.Entry<Clause,List<Clause>> entry: orAndOrs.entrySet()){
				Clause orAnd= new Clause();
				ors.getClause().add(0,orAnd);
				orAnd.setOperator(Operator.AND);
				Clause and= entry.getKey();
				orAnd.addClause(and);
				Clause moreOrs= new Clause();
				orAnd.addClause(moreOrs);
				moreOrs.setOperator(Operator.OR);
				for (Clause or:entry.getValue()){
					moreOrs.addClause(or);
				}
			}
		}
		if (!ands.isEmpty()) {
			for (Clause and:ands) {
				qry.addClause(and);
				processClause(and,false);
			}
		}

		if (!ImportUtils.isEmpty(ors.getClause()) |
			(!ImportUtils.isEmpty(clauseCriteria.get(ors)))) {
			qry.addClause(ors);
			processClause(ors,false);
		}
		if (!ImportUtils.isEmpty(nots.getClause()) |
			(!ImportUtils.isEmpty(clauseCriteria.get(nots)))) {
			qry.addClause(nots);
			if (nots.getClause()!=null)
				if (nots.getClause().size()>1)
					nots.setOperator(Operator.NOTOR);
			processClause(nots,true);
		}
		return qry;

	}




	private void processClause(Clause clause,boolean notExist) throws DataFormatException {
		if (!ImportUtils.isEmpty(clauseCriteria.get(clause))) {
			if (notExist){
				clause.setNotExist(true);
			}
			for (EQDOCCriteria eqCriteria : clauseCriteria.get(clause)) {
				if ((eqCriteria.getPopulationCriterion() != null)) {
					EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
					setParent(clause, TTIriRef.iri("urn:uuid:" + srch.getReportGuid()),"is in cohort : "+ reportNames.get(srch.getReportGuid()));
				} else {
					convertCriterion(eqCriteria.getCriterion(), clause);
				}
			}
		}
		if (!ImportUtils.isEmpty(clause.getClause())) {
			for (Clause childClause : clause.getClause()) {
				processClause(childClause,notExist);
			}
		}
	}

	private void setStepPattern(EQDOCPopulation eqPop) {
		ors.setOperator(Operator.OR);
		StepPattern lastPattern = StepPattern.AND;
		for (EQDOCCriteriaGroup gp : eqPop.getCriteriaGroup()) {
			VocMemberOperator eqMemberOp = gp.getDefinition().getMemberOperator();
			StepPattern thisPattern = getStepPattern(lastPattern, gp);
			lastPattern = thisPattern;
			if (thisPattern == StepPattern.AND & eqMemberOp == VocMemberOperator.AND) {
				stepAndAnd(gp.getDefinition());
			} else if (thisPattern == StepPattern.AND & eqMemberOp == VocMemberOperator.OR) {
				stepAndOr(gp.getDefinition());
			} else if (thisPattern == StepPattern.OR & eqMemberOp == VocMemberOperator.OR) {
				stepOrOr(gp.getDefinition(), ors);
			} else if (thisPattern == StepPattern.OR & eqMemberOp == VocMemberOperator.AND) {
				stepOrAnd(gp.getDefinition(), ors);
			} else if (thisPattern == StepPattern.NOTOR & eqMemberOp == VocMemberOperator.AND) {
				stepNotAnd(gp.getDefinition(), nots);
			} else if (thisPattern == StepPattern.NOTOR & eqMemberOp == VocMemberOperator.OR) {
				stepNotOr(gp.getDefinition(), nots);
			}
		}
	}

	private void stepNotOr(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size() == 1) {
			addCriteriaToNode(definition, clause);
		} else {
			Clause orClause = new Clause();
			orClause.setOperator(Operator.OR);
			clause.addClause(orClause);
			addCriteriaToNode(definition, orClause);
		}
	}

	private void stepNotAnd(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size() == 1) {
			Clause not= new Clause();
			nots.addClause(not);
			addCriteriaToNode(definition, not);
		} else {
			Clause andClause = new Clause();
			andClause.setOperator(Operator.AND);
			clause.addClause(andClause);
			addCriteriaToNode(definition, andClause);
		}
	}

	private void stepOrAnd(EQDOCCriteriaGroupDefinition definition, Clause ors) {
		if (definition.getCriteria().size() == 1) {
			addCriteriaToNode(definition, ors);
		} else {
			boolean isDuplicateAnd= stepDuplicate(definition);
			 if (!isDuplicateAnd) {
				 Clause and = new Clause();
				 ors.addClause(and);
				 and.setOperator(Operator.AND);
				 for (EQDOCCriteria eqCriteria:definition.getCriteria()){
					 Clause subAnd= new Clause();
					 and.addClause(subAnd);
					 addCriterionToNode(eqCriteria,subAnd);
				 }
			 }
		}
	}

	private boolean stepDuplicate(EQDOCCriteriaGroupDefinition definition) {
		Clause orAnd = null;
		boolean isDuplicateAnd = false;
		for (EQDOCCriteria eqCriteria : definition.getCriteria()) {
			if (eqCriteria.getCriterion() != null) {
				String id = eqCriteria.getCriterion().getId();
				if (duplicateOrs.get(id) != null) {
					String duplicate = duplicateOrs.get(id).toString();
					if (duplicate.equals(id)) {
						orAnd = new Clause();
						orAnd.setOperator(Operator.AND);
						orAndOrs.put(orAnd, new ArrayList<>());
						duplicateMap.put(id, orAnd);
						addCriterionToNode(eqCriteria, orAnd);
						isDuplicateAnd= true;
					} else {
						isDuplicateAnd = true;
						orAnd = duplicateMap.get(duplicate);
					}
				} else {
					if (isDuplicateAnd) {
						Clause andOr = new Clause();
						orAndOrs.get(orAnd).add(andOr);
						addCriterionToNode(eqCriteria, andOr);
					}
				}
			}
		}
		return isDuplicateAnd;
	}

	private void stepOrOr(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		addCriteriaToNode(definition, clause);
	}
	private void stepAndAnd(EQDOCCriteriaGroupDefinition definition) {
		Clause and= new Clause();
		ands.add(and);
		addCriteriaToNode(definition, and);
	}

	private void stepAndOr(EQDOCCriteriaGroupDefinition definition) {
		Clause andOrs= new Clause();
		ands.add(andOrs);
		if (definition.getCriteria().size() == 1) {
			addCriteriaToNode(definition, andOrs);
		} else {
			andOrs.setOperator(Operator.OR);
			for (EQDOCCriteria eqCriteria : definition.getCriteria()) {
				Clause orClause = new Clause();
				andOrs.addClause(orClause);
				addCriterionToNode(eqCriteria, orClause);
			}
		}


	}

	private void addCriteriaToNode(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		List<EQDOCCriteria> linkedCriteria = clauseCriteria.computeIfAbsent(clause, e -> new ArrayList<>());
		linkedCriteria.addAll(definition.getCriteria());
	}

	private void addCriterionToNode(EQDOCCriteria eqCriteria, Clause clause) {
		List<EQDOCCriteria> linkedCriteria = clauseCriteria.computeIfAbsent(clause, e -> new ArrayList<>());
		linkedCriteria.add(eqCriteria);
	}




	private void setSubject(Where where, String[] path) {
		for (int i = 0; i < path.length - 1; i++) {
			String var = null;
			if (path[i].contains("~")) {
				var = "?" + path[i].split("~")[1] + varCounter;
				varCounter++;
			}
			String iri = path[i].split("~")[0];
			if (iri.equals("Patient"))
				where.addEntityVar("?patient");
			else
				where.addEntity(TTIriRef.iri(iri), var);

		}
	}

	private String getMap(String from) throws DataFormatException {
		Object target = dataMap.get(from);
		if (target == null)
			throw new DataFormatException("unknown map : " + from);
		return (String) target;
	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Clause clause) throws DataFormatException {
		Map<String,String> restrictionMap= new HashMap<>();
		String eqTable = eqCriterion.getTable();
		String entityVar = null;
		Clause superClause=null;

		EQDOCFilterAttribute eqAtt = eqCriterion.getFilterAttribute();
		if (eqAtt.getRestriction() != null) {
			superClause = clause;
			Clause subClause = new Clause();
			superClause.addClause(subClause);
			clause = subClause;
			if (criteriaLabels.get(eqCriterion.getId()) != null) {
				superClause.setName(criteriaLabels.get(eqCriterion.getId()).toString());
			}
		}
		else {
			if (criteriaLabels.get(eqCriterion.getId())!=null) {
				clause.setName(criteriaLabels.get(eqCriterion.getId()).toString());
			}
		}
		if (eqCriterion.isNegation()){
			clause.setNotExist(true);
		}
		for (EQDOCColumnValue cv : eqAtt.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				String[] path = getMap(eqTable + "/" + eqColumn).split("/");
				Where where = new Where();
				clause.addWhere(where);
				setSubject(where, path);
				entityVar = ((IriVar) where.getEntity().get(where.getEntity().size() - 1)).getVar();
				String predicate = path[path.length - 1];
				where.setProperty(TTIriRef.iri(IM.NAMESPACE + predicate));
				varCounter++;
				String valueVar = "?" + predicate + varCounter;
				restrictionMap.put(predicate,valueVar);
				where.setValueVar(valueVar);
				convertFilter(cv, where);

			}
		}
		if (eqAtt.getRestriction() != null) {
			addRestriction(eqAtt, clause, entityVar, eqTable,restrictionMap);
			if (eqAtt.getRestriction().getTestAttribute()!=null){
				addTest(eqAtt, eqTable,superClause,entityVar,restrictionMap);
			}
			else if (eqCriterion.getLinkedCriterion()!=null){
				convertLinkedCriterion(eqCriterion.getLinkedCriterion(),superClause,restrictionMap);
			}
		}
	}

	private void addRestriction(EQDOCFilterAttribute eqAtt,
															Clause clause, String entityVar,
															String eqTable,Map<String,String> restrictionMap) throws DataFormatException {

		for (EQDOCColumnOrder.Columns col:eqAtt.getRestriction().getColumnOrder().getColumns()) {
			VocOrderDirection direction = col.getDirection();
			for (String column : col.getColumn()) {
				if (column.contains("DATE")) {
					if (restrictionMap.get("effectiveDate")==null) {
						varCounter++;
						clause.addWhere(new Where()
							.addEntityVar(entityVar)
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
				 testWhere.addEntityVar(entityVar);
				 String testPredicate = path[path.length - 1];
				 testWhere.setProperty(TTIriRef.iri(IM.NAMESPACE + testPredicate));
				 String valueVar = restrictionMap.get(testPredicate);
				 testWhere.setValueVar(valueVar);
				 convertFilter(cv, testWhere);
				 varCounter++;
			 }
		}
	}

	private void convertLinkedCriterion(EQDOCLinkedCriterion eqLinked, Clause clause, Map<String,String>   restrictionMap) throws DataFormatException {
		convertCriterion(eqLinked.getCriterion(),clause);
		EQDOCRelationship eqRel= eqLinked.getRelationship();
		if (!eqRel.getParentColumn().contains("DATE"))
			throw new DataFormatException("Only date columns supported in linked criteria : "+ activeReport);
		Where linkWhere= clause.getWhere().get(clause.getWhere().size()-1);
		Where getDate= new Where();
		clause.addWhere(getDate);
		getDate.addEntityVar(linkWhere.getEntity().get(linkWhere.getEntity().size()-1).getVar());
		getDate.addProperty(TTIriRef.iri(IM.NAMESPACE+"effectiveDate"));
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
				if (in == VocColumnValueInNotIn.IN)
					filter.addIn(getValueSet(vs));
				else
					filter.addNotIn(getValueSet(vs));
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
			String compareAgainst= null;
			convertRangeValue(filter,where.getValueVar(),cv.getRangeValue(),null);
		}
		varCounter++;
	}

	private TTIriRef getValueSet(EQDOCValueSet vs) throws DataFormatException {
		TTEntity valueSet = new TTEntity();
		String iri = "urn:uuid:" + UUID.randomUUID();
		valueSet.setIri(iri);
		valueSet.addType(IM.CONCEPT_SET);
		document.addEntity(valueSet);
		VocCodeSystemEx scheme = vs.getCodeSystem();
		if (vs.getValues().size() == 1) {
			valueSet.addObject(IM.DEFINITION, getValue(scheme, vs.getValues().get(0)));
		} else {
			TTNode orSet = new TTNode();
			valueSet.addObject(IM.DEFINITION, orSet);
			for (EQDOCValueSetValue ev : vs.getValues()) {
				orSet.addObject(SHACL.OR, getValue(scheme, ev));
			}
		}
		return TTIriRef.iri(iri);
	}
	private TTIriRef getValue(VocCodeSystemEx scheme,EQDOCValueSetValue ev) throws DataFormatException {
			if (scheme== VocCodeSystemEx.EMISINTERNAL) {
					String key = "EMISINTERNAL/" + ev.getValue();
					Object mapValue = dataMap.get(key);
					if (mapValue != null) {
						return TTIriRef.iri(IM.NAMESPACE+mapValue);
					}
					else
						throw new DataFormatException("unmapped emis internal code : "+key);
			}
			else if (scheme==VocCodeSystemEx.SNOMED_CONCEPT| scheme.value().contains("SCT")){
				return TTIriRef.iri("sn:"+ ev.getValue());
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

		Range range= new Range();
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
			.addEntityVar("?patient")
			.setProperty(IM.IN_DATASET)
			.addFilter(new Filter().addIn(parent)));
		clause.setName(parentName);
		mainSubject="?patient";
	}

	private StepPattern getStepPattern(StepPattern lastOperator, EQDOCCriteriaGroup eqGroup){
		if (eqGroup.getActionIfFalse()==VocRuleAction.NEXT) {
			if (eqGroup.getActionIfTrue()==VocRuleAction.REJECT){
				return StepPattern.NOTOR;
			}
			else
				return StepPattern.OR;
		}
		else if (eqGroup.getActionIfFalse()==VocRuleAction.REJECT) {
			if (lastOperator == StepPattern.OR)
				return StepPattern.OR;
			else
				return StepPattern.AND;
		}
		else if (eqGroup.getActionIfTrue() == VocRuleAction.REJECT) {
				return StepPattern.NOTAND;
			}
		else
				return StepPattern.AND;
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
