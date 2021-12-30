package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTNode;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.transforms.eqd.*;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToQuery {
	private int varCounter;
	private String activeReport;
	private Properties dataMap;
	private String mainSubject;
	private TTDocument document;
	private final Clause ands = new Clause();
	private final Clause ors= new Clause();
	private final Clause nots= new Clause();
	private final Map<Clause,List<EQDOCCriteria>> clauseCriteria = new HashMap<>();
	private final Map<Object, Object> vocabMap = new HashMap<>();
	private enum StepPattern {AND, OR,NOT}

	public void convertPopulation(TTDocument document,EQDOCReport eqReport,Query qry,Properties dataMap) throws DataFormatException {
		this.document= document;
		this.dataMap = dataMap;
		setVocabMaps();
		activeReport = eqReport.getId();
		qry.setOperator(Operator.AND);
		if (eqReport.getParent().getParentType() == VocPopulationParentType.ACTIVE) {
			qry.addClause(ands);
			setParent(ands, TTIriRef.iri("Q_RegisteredGMS"));
		}
		if (eqReport.getParent().getParentType() == VocPopulationParentType.POP) {
			qry.addClause(ands);
			setParent(ands, TTIriRef.iri("urn:uuid:" +
				eqReport.getParent()
					.getSearchIdentifier()
					.getReportGuid()));
		}
		setStepPattern(eqReport.getPopulation());
		if (!CollectionUtils.isEmpty(ands.getClause())|
			(!CollectionUtils.isEmpty(clauseCriteria.get(ands)))) {
			if (!qry.getClause().contains(ands))
				qry.addClause(ands);
			processClause(ands);
		}
		if (!CollectionUtils.isEmpty(ors.getClause())|
			(!CollectionUtils.isEmpty(clauseCriteria.get(ors)))) {
			qry.addClause(ors);
			processClause(ors);
		}
		if (!CollectionUtils.isEmpty(nots.getClause())|
			(!CollectionUtils.isEmpty(clauseCriteria.get(nots)))) {
			qry.addClause(nots);
			processClause(nots);
		}

	}

	private void processClause(Clause clause) throws DataFormatException {
		if (!CollectionUtils.isEmpty(clauseCriteria.get(clause))){
			for (EQDOCCriteria eqCriteria:clauseCriteria.get(clause)){
				if ((eqCriteria.getPopulationCriterion()!=null)){
					EQDOCSearchIdentifier srch= eqCriteria.getPopulationCriterion();
					setParent(clause, TTIriRef.iri("urn:uuid:" + srch.getReportGuid()));
				}
				else {
					convertCriterion(eqCriteria.getCriterion(), clause);
				}
			}
		}
		if (!CollectionUtils.isEmpty(clause.getClause())){
			for (Clause childClause:clause.getClause()){
				processClause(childClause);
			}
		}
	}

	private void setStepPattern(EQDOCPopulation eqPop) {
		ands.setOperator(Operator.AND);
		ors.setOperator(Operator.OR);
		nots.setOperator(Operator.OR);
		StepPattern lastPattern = StepPattern.AND;
		for (EQDOCCriteriaGroup gp : eqPop.getCriteriaGroup()) {
			VocMemberOperator eqMemberOp = gp.getDefinition().getMemberOperator();
			StepPattern thisPattern = getStepPattern(lastPattern, gp);
			lastPattern = thisPattern;
			if (thisPattern == StepPattern.AND & eqMemberOp == VocMemberOperator.AND) {
				stepAndAnd(gp.getDefinition());
			}
			else if (thisPattern == StepPattern.AND & eqMemberOp == VocMemberOperator.OR) {
				stepAndOr(gp.getDefinition(),ands);
			}
			else if (thisPattern == StepPattern.OR & eqMemberOp == VocMemberOperator.OR) {
				stepOrOr(gp.getDefinition(),ors);
			}
			else if (thisPattern == StepPattern.OR & eqMemberOp == VocMemberOperator.AND) {
				stepOrAnd(gp.getDefinition(),ors);
			}
			else if (thisPattern==StepPattern.NOT & eqMemberOp==VocMemberOperator.AND) {
				stepNotAnd(gp.getDefinition(), nots);
			}
			else if (thisPattern==StepPattern.NOT & eqMemberOp==VocMemberOperator.OR) {
				stepNotOr(gp.getDefinition(), nots);
			}
		}
	}

	private void stepNotOr(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size()==1) {
			addCriteriaToNode(definition,clause);
		}
		else {
			Clause orClause= new Clause();
			orClause.setOperator(Operator.OR);
			clause.addClause(orClause);
			addCriteriaToNode(definition,orClause);
		}
	}

	private void stepNotAnd(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size()==1) {
			addCriteriaToNode(definition,clause);
		}
		else {
			Clause andClause= new Clause();
			andClause.setOperator(Operator.AND);
			clause.addClause(andClause);
			addCriteriaToNode(definition,andClause);
		}
	}

	private void stepOrAnd(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size()==1) {
			addCriteriaToNode(definition,clause);
		}
		else {
			Clause andClause= new Clause();
			andClause.setOperator(Operator.AND);
			clause.addClause(andClause);
			for (EQDOCCriteria eqCriteria:definition.getCriteria()){
				addCriterionToNode(eqCriteria,andClause);
			}
		}
	}

	private void stepOrOr(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		addCriteriaToNode(definition, clause);
	}

	private void stepAndOr(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		if (definition.getCriteria().size()==1) {
			addCriteriaToNode(definition, clause);
		}
		else {
			Clause orClause= new Clause();
			orClause.setOperator(Operator.OR);
			clause.addClause(orClause);
			for (EQDOCCriteria eqCriteria:definition.getCriteria()){
				addCriterionToNode(eqCriteria,orClause);
			}
		}


	}

	private void addCriteriaToNode(EQDOCCriteriaGroupDefinition definition, Clause clause) {
		List<EQDOCCriteria> linkedCriteria= clauseCriteria.computeIfAbsent(clause, e -> new ArrayList<>());
		linkedCriteria.addAll(definition.getCriteria());
	}
	private void addCriterionToNode(EQDOCCriteria eqCriteria, Clause clause) {
		List<EQDOCCriteria> linkedCriteria= clauseCriteria.computeIfAbsent(clause, e -> new ArrayList<>());
		linkedCriteria.add(eqCriteria);
	}

	private void stepAndAnd(EQDOCCriteriaGroupDefinition definition) {
		addCriteriaToNode(definition, ands);
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
		Object target= dataMap.get(from);
		if (target==null)
			throw new DataFormatException("unknown map : "+ from);
		return (String) target;
	}

	private void convertCriterion(EQDOCCriterion eqCriterion, Clause superClause) throws DataFormatException {
		String eqTable = eqCriterion.getTable();
		varCounter++;
		String entityVar=null;
		Clause clause = superClause;
		EQDOCFilterAttribute eqAtt = eqCriterion.getFilterAttribute();
		if (eqAtt.getRestriction() != null) {
			Clause testClause = new Clause();
			superClause.addClause(testClause);
			superClause= testClause;
			Clause subClause= new Clause();
			superClause.addSubQuery(subClause);
			clause= subClause;

		}
		for (EQDOCColumnValue cv : eqAtt.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				String[] path = getMap(eqTable + "/" + eqColumn).split("/");
				Where where = new Where();
				clause.addWhere(where);
				setSubject(where, path);
				entityVar= where.getEntity().get(where.getEntity().size()-1).getVar();
				String predicate = path[path.length - 1];
				where.setProperty(TTIriRef.iri("im:" + predicate));
				String valueVar= "?"+predicate+varCounter;
				where.setValueVar(valueVar);
				convertFilter(cv, where);
				varCounter++;
			}
		}
		if (eqAtt.getRestriction() != null) {
			if (eqAtt.getRestriction().getColumnOrder()
				.getColumns().get(0).getColumn().get(0).equals("DATE")) {
				addRestriction(eqAtt, superClause,clause, entityVar,eqTable, "DATE");
			}
			else
				throw new DataFormatException("unrecognised column in restriction in report id" + activeReport);
		}
	}

	private void addRestriction(EQDOCFilterAttribute eqAtt,Clause superClause,
															Clause clause, String entityVar,
															String eqTable,String column) throws DataFormatException {
		VocOrderDirection direction = eqAtt.getRestriction()
			.getColumnOrder().getColumns().get(0).getDirection();
		varCounter++;
		if (column.equals("DATE")) {
			clause.addWhere( new Where()
				.addEntityVar(entityVar)
				.setProperty(TTIriRef.iri("im:" + "effectiveDate"))
				.setValueVar("?effectiveDate"+varCounter));
			clause.addGroupSort(new GroupSort()
				.setSortBy((SortBy) vocabMap.get(direction))
				.setGroupBy(mainSubject)
				.setField("?effectiveDate"+varCounter));
		} else
			throw new DataFormatException("only date column restrictions supported id=" + activeReport);
		addTest(eqAtt, eqTable,superClause,entityVar);
	}

	private void addTest(EQDOCFilterAttribute eqAtt,String eqTable,Clause clause,
											 String entityVar) throws DataFormatException {
		EQDOCTestAttribute eqTest= eqAtt.getRestriction().getTestAttribute();
		for (EQDOCColumnValue cv: eqTest.getColumnValue()) {
			for (String eqColumn : cv.getColumn()) {
				Where testWhere= new Where();
				clause.addWhere(testWhere);
				String[] path = getMap(eqTable + "/" + eqColumn).split("/");
				testWhere.addEntityVar(entityVar);
				String testPredicate = path[path.length-1];
				testWhere.setProperty(TTIriRef.iri("im:"+testPredicate));
				String valueVar="?"+ testPredicate+varCounter;
				testWhere.setValueVar(valueVar);
				convertFilter(cv,testWhere);
				varCounter++;
			}
		}
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
		if (!CollectionUtils.isEmpty(cv.getLibraryItem())) {
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
			convertRangeValue(filter,where.getValueVar(),cv.getRangeValue());
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
						return TTIriRef.iri("im:"+mapValue);
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

	private void setCompare(Filter filter, String valueVar,EQDOCRangeFrom rFrom){
		Comparison comp;
		if (rFrom.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rFrom.getOperator());
		else
			comp= Comparison.equal;
		String value= rFrom.getValue().getValue();
		TTIriRef function=null;
		List<String> arguments=null;
		if (rFrom.getValue().getRelation()!=null) {
			if (rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				function = TTIriRef.iri("im:TimeDifference");
				arguments = new ArrayList<>();
				arguments.add(rFrom.getValue().getUnit().value());
				arguments.add(valueVar);
				arguments.add("$referenceDate");
			}
		}
		else {
			if (rFrom.getValue().getUnit()!=null){
				arguments= new ArrayList<>();
				arguments.add(valueVar);
				arguments.add(rFrom.getValue().getUnit().value());
			}
		}
		setCompareFilter(filter,comp,value,function,arguments);
	}

	private void setCompare(Filter filter,String valueVar,EQDOCRangeTo rTo){
		Comparison comp;
		if (rTo.getOperator()!=null)
			comp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			comp= Comparison.equal;
		String value= rTo.getValue().getValue();
		TTIriRef function=null;
		List<String> arguments = new ArrayList<>();
		if (rTo.getValue().getRelation()!=null) {
			if (rTo.getValue().getRelation() == VocRelation.RELATIVE) {
				function = TTIriRef.iri("im:TimeDifference");
				arguments.add(rTo.getValue().getUnit().value());
				arguments.add(valueVar);
				arguments.add("$referenceDate");
			}
		}
		setCompareFilter(filter,comp,value,function,arguments);
	}
	private void setCompareFilter (Filter filter,Object comparison,String value,TTIriRef function,
																 List<String> arguments){
		filter.setValueTest((Comparison) comparison,value);
		if (function!=null) {
			filter.setFunction(function);
		}
		if (arguments != null)
			for (String argument : arguments)
				filter.addArgument(argument);
	}
	private void setRangeCompare(Filter filter,String valueVar,
															 EQDOCRangeFrom rFrom,EQDOCRangeTo rTo) {
		Comparison fromComp;
		if (rFrom.getOperator() != null)
			fromComp = (Comparison) vocabMap.get(rFrom.getOperator());
		else
			fromComp = Comparison.equal;
		String fromValue = rFrom.getValue().getValue();
		TTIriRef fromFunction = null;
		List<String> fromArguments = null;
		if (rFrom.getValue().getRelation() != null) {
			if (rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
				fromFunction = TTIriRef.iri("im:TimeDifference");
				fromArguments = new ArrayList<>();
				fromArguments.add(rFrom.getValue().getUnit().value());
				fromArguments.add(valueVar);
				fromArguments.add("$referenceDate");
			}
		}
		Comparison toComp;
		if (rTo.getOperator()!=null)
			toComp= (Comparison) vocabMap.get(rTo.getOperator());
		else
			toComp= Comparison.equal;
		String toValue= rTo.getValue().getValue();
		TTIriRef toFunction=null;
		List<String> toArguments=null;
		if (rTo.getValue().getRelation()!=null){
			if (rTo.getValue().getRelation()== VocRelation.RELATIVE) {
				toFunction = TTIriRef.iri("im:TimeDifference");
				toArguments = new ArrayList<>();
				toArguments.add(rTo.getValue().getUnit().value());
				toArguments.add(valueVar);
				toArguments.add("$referenceDate");
			}
		}
		setRangeCompareFilter(filter,fromComp,fromValue,fromFunction,fromArguments,
			toComp,toValue,toFunction,toArguments);


	}
	private void setRangeCompareFilter (Filter filter,
																			Comparison fromComp,String fromValue,
																			TTIriRef fromFunction,List<String> fromArguments,
																			Comparison toComp,String toValue,
																			TTIriRef toFunction,List<String> toArguments){

		filter.setRange(new Range()
			.setFrom(new Compare().setComparison(fromComp)
				.setValue(fromValue)
				.setFunction(fromFunction)
				.setArgument(fromArguments))
			.setTo(new Compare().setComparison(toComp)
				.setValue(toValue)
				.setFunction(toFunction)
				.setArgument(toArguments)));
	}



	private void convertRangeValue(Filter filter, String valueVar, EQDOCRangeValue rv) {
		EQDOCRangeFrom rFrom= rv.getRangeFrom();
		EQDOCRangeTo rTo= rv.getRangeTo();
		if (rFrom != null) {
			if (rTo==null) {
				setCompare(filter, valueVar,rFrom);
			}
			else {
				setRangeCompare(filter,valueVar,
					rFrom,rTo);
			}
		}
		if (rTo != null) {
			if (rFrom == null) {
				setCompare(filter, valueVar, rTo);
			}
		}
	}



	private void setParent(Clause clause,TTIriRef parent) {
		clause.addWhere( new Where()
			.addEntityVar("?patient")
			.setProperty(IM.IN_DATASET)
			.addFilter(new Filter().addIn(parent)));
		mainSubject="?patient";
	}

	private StepPattern getStepPattern(StepPattern lastOperator, EQDOCCriteriaGroup eqGroup){
		if (eqGroup.getActionIfFalse()==VocRuleAction.NEXT)
			return StepPattern.OR;
		else
		if (eqGroup.getActionIfFalse()==VocRuleAction.REJECT) {
			if (lastOperator == StepPattern.OR)
				return StepPattern.OR;
			else
				return StepPattern.AND;
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
