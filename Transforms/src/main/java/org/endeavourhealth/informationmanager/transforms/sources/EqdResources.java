package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.collections4.CollectionUtils;
import org.endeavourhealth.imapi.model.iml.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.*;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

public class EqdResources {
	Map<String, String> reportNames = new HashMap<>();
	private final ImportMaps importMaps = new ImportMaps();
	private final Map<Object, String> vocabMap = new HashMap<>();
	private TTIriRef valueSetFolder;
	private Properties dataMap;
	private Properties labels;
	private String activeReport;
	private TTDocument document;
	private TTContext prefixes= TTManager.getDefaultContext();
	private final Map<String,Set<TTIriRef>> valueMap= new HashMap<>();
	private Query query;
	private int counter=0;

	public Query getQuery() {
		return query;
	}

	public ImportMaps getImportMaps() {
		return importMaps;
	}

	public EqdResources setQuery(Query query) {
		this.query = query;
		return this;
	}

	public TTDocument getDocument() {
		return document;
	}

	public EqdResources setDocument(TTDocument document) {
		this.document = document;
		return this;
	}

	public void setActiveReport(String activeReport) {
		this.activeReport = activeReport;
	}

	public void setValueSetFolder(TTIriRef valueSetFolder) {
		this.valueSetFolder = valueSetFolder;
	}

	public EqdResources() {
		setVocabMaps();
	}

	public Properties getDataMap() {
		return dataMap;
	}

	public void setDataMap(Properties dataMap) {
		this.dataMap = dataMap;
	}

	public EqdResources setLabels(Properties labels) {
		this.labels = labels;
		return this;
	}

	private void setVocabMaps() {
		vocabMap.put(VocRangeFromOperator.GTEQ, ">=");
		vocabMap.put(VocRangeFromOperator.GT, ">");
		vocabMap.put(VocRangeToOperator.LT, "<");
		vocabMap.put(VocRangeToOperator.LTEQ, "<=");
		vocabMap.put(VocOrderDirection.DESC, "DESC");
		vocabMap.put(VocOrderDirection.ASC, "ASC");
	}

	public TTIriRef getIri(String token) throws IOException {
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
	public void setWith(Query query, TTIriRef parent) {
		query.setWith(new With());
		Query withQuery= new Query();
		withQuery.setIri(parent.getIri());
		if (parent.getName()!=null)
			withQuery.setName(parent.getName());
		query.getWith().setQuery(withQuery);

	}




	public void convertCriteria(EQDOCCriteria eqCriteria,
															 Where where) throws DataFormatException, IOException {

		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			where.setProperty(IM.IN_RESULT_SET);
			where.setIs(new TTAlias().setIri("urn:uuid:" + srch.getReportGuid())
				.setName(reportNames.get(srch.getReportGuid())));
		} else {
			convertCriterion(eqCriteria.getCriterion(),where);
		}
	}


		private String getAlias(String term){
		return term.replaceAll(" ","")
			.replaceAll(">","Over")
			.replaceAll("<","Under");
		}




	private void convertCriterion(EQDOCCriterion eqCriterion, Where match) throws DataFormatException, IOException {
		if (eqCriterion.isNegation()){
			Where notWhere= new Where();
			match.setNotExist(notWhere);
			match= notWhere;
		}

		if (eqCriterion.getLinkedCriterion() != null) {
			convertLinkedCriterion(eqCriterion, match);
		}
		else if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			convertRestrictionCriterion(eqCriterion, match);
		}
		else {
			convertColumns(eqCriterion, match);
		}
	}



	private void convertLinkedCriterion(EQDOCCriterion eqCriterion, Where topWhere) throws DataFormatException, IOException {
		Where targetWhere = new Where();
		topWhere.addAnd(targetWhere);
		Where bestTarget= targetWhere;
		convertColumns(eqCriterion, targetWhere);
		if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			setRestriction(eqCriterion, targetWhere);
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				Where testWhere= new Where();
				testWhere.setFrom(targetWhere.getAlias());
				List<EQDOCColumnValue> cvs= eqCriterion.getFilterAttribute().getRestriction().getTestAttribute().getColumnValue();
				if (cvs.size()==1){
					Where pv = new Where();
					testWhere.setWhere(pv);
					setMainCriterion(eqCriterion.getTable(), cvs.get(0), pv);
				}
				else {

					for (EQDOCColumnValue cv : cvs) {
						Where pv = new Where();
						testWhere.addAnd(pv);
						setMainCriterion(eqCriterion.getTable(), cv, pv);
					}
				}
				testWhere.setAlias(targetWhere.getAlias() + testWhere.getAlias());
				bestTarget = testWhere;
			}
		}
		String targetPath;
		Where linkWhere= new Where();
		topWhere.addAnd(linkWhere);
		EQDOCRelationship eqRel = eqCriterion.getLinkedCriterion().getRelationship();
		if (eqRel.getParentColumn().contains("DATE")){
			targetPath= bestTarget.getAlias()+ "->date";
		}
		else
			throw new DataFormatException("Only date link fields supported at the moment");

		EQDOCCriterion eqTargetCriterion = eqCriterion.getLinkedCriterion().getCriterion();
		convertColumns(eqCriterion, linkWhere);
		if (eqTargetCriterion.getFilterAttribute().getRestriction() != null) {
			restrictionTest(eqCriterion, topWhere, linkWhere);
		}
		Where compareWhere= new Where();
		topWhere.addAnd(compareWhere);
		Where pv= new Where();
		compareWhere.setWhere(pv);
		Compare compare= new Compare()
			.setTarget(targetPath)
			.setComparison(vocabMap.get(eqRel.getRangeValue().getRangeFrom().getOperator()))
			.setSource("date");
		pv.setCompare(compare);

	}


	private void convertRestrictionCriterion(EQDOCCriterion eqCriterion, Where topWhere) throws DataFormatException, IOException {
		Where restrictionWhere= new Where();
		restrictionWhere.setAlias(getLabel(eqCriterion));
		topWhere.addAnd(restrictionWhere);
		if (eqCriterion.getDescription() != null)
			restrictionWhere.setName(eqCriterion.getDescription());
		convertColumns(eqCriterion, restrictionWhere);
		restrictionTest(eqCriterion, topWhere, restrictionWhere);
	}

	private void restrictionTest(EQDOCCriterion eqCriterion, Where topWhere, Where restrictionWhere) throws IOException, DataFormatException {
		setRestriction(eqCriterion, restrictionWhere);
		EQDOCTestAttribute testAtt= eqCriterion.getFilterAttribute().getRestriction().getTestAttribute();
		if (testAtt != null) {
				Where testWhere= new Where();
				topWhere.addAnd(testWhere);
				testWhere.setFrom(restrictionWhere.getAlias());
				List<EQDOCColumnValue> cvs= testAtt.getColumnValue();
				if (cvs.size()==1){
					setMainCriterion(eqCriterion.getTable(), cvs.get(0), testWhere);
					if (testWhere.getAlias()==null)
						testWhere.setAlias(summarise(testWhere,restrictionWhere,testAtt));
				}
				else {
					for (EQDOCColumnValue cv : cvs) {
						Where pv = new Where();
						testWhere.addAnd(pv);
						setMainCriterion(eqCriterion.getTable(), cv, pv);
						if (pv.getAlias()==null)
							pv.setAlias(summarise(pv,restrictionWhere,testAtt));
					}
				}

			}
	}


	private void convertColumns(EQDOCCriterion eqCriterion, Where match) throws DataFormatException, IOException {
		match.setAlias(getLabel(eqCriterion));
		EQDOCFilterAttribute filterAttribute = eqCriterion.getFilterAttribute();
		TTAlias entityPath= getPath(eqCriterion.getTable());
		if (entityPath!=null)
			match.setProperty(entityPath);
		if (labels.get(eqCriterion.getId()) != null) {
				match.setAlias(getAlias(labels.get(eqCriterion.getId()).toString()));
		}
		List<EQDOCColumnValue> cvs= filterAttribute.getColumnValue();
		if (cvs.size()==1){
			Where pv;
			if (entityPath==null)
				pv= match;
			else {
				pv = new Where();
				match.setWhere(pv);
			}
			setMainCriterion(eqCriterion.getTable(), cvs.get(0),pv);
			if (pv.getAlias()==null)
				if (labels.get(cvs.get(0).getId())!=null)
					pv.setAlias(labels.get(cvs.get(0).getId()).toString());
		}
		else {
			for (EQDOCColumnValue cv : filterAttribute.getColumnValue()) {
				Where pv = new Where();
				match.addAnd(pv);
				setMainCriterion(eqCriterion.getTable(), cv, pv);
				if (pv.getAlias()==null)
				 if (labels.get(cv.getId())!=null)
				  	pv.setAlias(labels.get(cv.getId()).toString());
			}
		}

	}




	private void setMainCriterion(String eqTable, EQDOCColumnValue cv, Where pv) throws DataFormatException, IOException {
		String eqColumn= String.join("/",cv.getColumn());
		setWhere(cv, eqTable, eqColumn, pv);
	}



	private void setWhere(EQDOCColumnValue cv, String eqTable, String eqColumn,
																Where pv) throws DataFormatException, IOException {
		pv.setProperty(getPath(eqTable+"/"+ eqColumn));
		VocColumnValueInNotIn in = cv.getInNotIn();
		boolean notIn = (in == VocColumnValueInNotIn.NOTIN);
		if (!cv.getValueSet().isEmpty()) {
			for (EQDOCValueSet vs : cv.getValueSet()) {
				if (vs.getId()!=null)
					if (labels.get(vs.getId())!=null) {
						String alias = labels.get(vs.getId()).toString();
						pv.setAlias(pv.getAlias() == null ? alias : pv.getAlias() + " " + alias);
					}
				if (vs.getAllValues() != null) {
					pv.setNot(true);
					pv.setIn(getExceptionSet(vs.getAllValues()));
				} else {
					if (!notIn) {
						if (isValueSet(vs)) {
							pv.setIn(getValueSet(vs));
						} else
							pv.setIn(getInlineValues(vs));
					}
					else {
						if (isValueSet(vs)) {
							pv.setNot(true);
							pv.setIn(getValueSet(vs));
						}
						else {
							pv.setNot(true);
							pv.setIn(getInlineValues(vs));
						}
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
					pv.addIn(iri);
				else {
					pv.setNot(true);
					pv.addIn(iri);
					storeLibraryItem(iri);
				}
			}
		} else if (cv.getRangeValue() != null) {
			setRangeValue(cv.getRangeValue(), pv);
		}

	}

	public TTAlias getPath(String eqdPath) throws DataFormatException {
		Object target = dataMap.get(eqdPath);
		if (target == null)
			throw new DataFormatException("unknown map : " + eqdPath);
		String targetPath= (String) target;
		if (targetPath.equals(""))
			return null;
		String[] paths = targetPath.split("/");
		List<String> pathList = Arrays.stream(paths).map(p-> IM.NAMESPACE+p).collect(Collectors.toList());
		if (pathList.size()==1){
			return new TTAlias().setIri(pathList.get(0));
		}
		else {
			return new TTAlias().setPath(String.join(" ",pathList));
		}
	}


	private void setRestriction(EQDOCCriterion eqCriterion, Where match) throws DataFormatException {
		String eqTable = eqCriterion.getTable();
		String linkColumn = eqCriterion.getFilterAttribute().getRestriction()
			.getColumnOrder().getColumns().get(0).getColumn().get(0);

		String predicatePath = ((String) dataMap.get(eqTable + "/" + linkColumn)).replaceAll("/"," ");
		match.addOrderBy(getPath(eqTable + "/" + linkColumn));
		match.setLimit(1);
		EQDOCFilterRestriction restrict = eqCriterion.getFilterAttribute().getRestriction();
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC) {
			match.setDirection("ASC");
			match.setAlias("Earliest"+ match.getAlias());
		}
		else {
			match.setDirection("DESC");
			match.setAlias("Latest" + match.getAlias());
		}
	}


	private void setRangeValue(EQDOCRangeValue rv, Where pv) throws DataFormatException {

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

	private void setCompareFrom(Where pv, EQDOCRangeFrom rFrom) throws DataFormatException {
		String comp;
		if (rFrom.getOperator() != null)
			comp = vocabMap.get(rFrom.getOperator());
		else
			comp = "=";
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

	private void setCompare(Where pv, String comp, String value, String units, VocRelation relation,
													boolean from) throws DataFormatException {
		if (relation == VocRelation.RELATIVE) {
			String first = from ? "$this" : "$referenceDate";
			String second = from ? "$referenceDate" : "$this";
			Function function = getTimeDiff(units, first, second);
			pv.setFunction(function);
			if (from) {
				comp = reverseComp(comp);
				value = String.valueOf(-Integer.parseInt(value));
			}
			pv
				.setComparison(comp)
				.setValue(value);

		} else {
			pv
				.setComparison(comp)
				.setValue(value);
			if (pv.getProperty().getIri().contains("age")) {
				if (units == null)
					throw new DataFormatException("missing units from age");
			}
		}
		if (units!=null)
				pv.addArgument(new Argument()
					.setParameter("units")
					.setValueData(units));

	}

	private String reverseComp(String comp) {
		switch (comp) {
			case ">=":
				return "<=";
			case ">":
				return "<";
			case "<":
				return ">";
			case "<=":
				return ">=";
		}
		return comp;
	}


	private void setCompareTo(Where pv, EQDOCRangeTo rTo) throws DataFormatException {
		String comp;
		if (rTo.getOperator() != null)
			comp = vocabMap.get(rTo.getOperator());
		else
			comp = "=";
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

	private void setRangeCompare(Where pv, EQDOCRangeFrom rFrom, EQDOCRangeTo rTo) throws DataFormatException {
		Range range = new Range();
		pv.setRange(range);
		String fromComp;
		if (rFrom.getOperator() != null)
			fromComp = vocabMap.get(rFrom.getOperator());
		else
			fromComp = "=";
		String fromValue = rFrom.getValue().getValue();
		String units = null;
		if (rFrom.getValue().getUnit() != null)
			units = rFrom.getValue().getUnit().value();
		if (rFrom.getValue().getRelation() != null && rFrom.getValue().getRelation() == VocRelation.RELATIVE) {
			Function function = getTimeDiff(units, "$this", "$referenceDate");
			range.setFrom(new Value()
				.setComparison(fromComp)
				.setValue(fromValue));
			pv.setFunction(function);
		} else {
			range.setFrom(new Value()
				.setComparison(fromComp)
				.setValue(fromValue));
			if (pv.getProperty().getIri().contains("age")) {
				if (units == null)
					throw new DataFormatException("missing units from age");
				pv.addArgument(new Argument()
					.setParameter("units")
					.setValueData(units));
			}
		}

		String toComp;
		if (rTo.getOperator() != null)
			toComp = vocabMap.get(rTo.getOperator());
		else
			toComp = "=";
		String toValue = rTo.getValue().getValue();
		units = null;
		if (rTo.getValue().getUnit() != null)
			units = rTo.getValue().getUnit().value();
		if (rTo.getValue().getRelation() != null && rTo.getValue().getRelation() == VocRelation.RELATIVE) {
			Function function = getTimeDiff(units, "$referenceDate", "$this");
			range.setTo(new Value()
				.setComparison(toComp)
				.setValue(toValue));
			pv.setFunction(function);
		} else {
			range.setTo(new Value()
				.setComparison(toComp)
				.setValue(toValue));
		}

	}





	private List<TTIriRef> getExceptionSet(EQDOCException set) throws DataFormatException, IOException {
		List<TTIriRef> valueSet = new ArrayList<>();
		VocCodeSystemEx scheme = set.getCodeSystem();
		for (EQDOCExceptionValue ev : set.getValues()) {
			Set<TTIriRef> values = getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
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

	private List<TTIriRef> getInlineValues(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTIriRef> setContent = new ArrayList<>();
		VocCodeSystemEx scheme = vs.getCodeSystem();
		for (EQDOCValueSetValue ev : vs.getValues()) {
			Set<TTIriRef> concepts = getValue(scheme, ev);
			if (concepts != null) {
				for (TTIriRef iri : concepts) {
					TTIriRef conRef = new TTIriRef(iri.getIri(), iri.getName());
					setContent.add(conRef);
				}
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}
		return setContent;
	}


	private List<TTIriRef> getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
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
			Set<TTIriRef> concepts = getValue(scheme, ev);
			if (concepts != null) {
				setContent.addAll(new ArrayList<>(concepts));
					if (i==1) {
						if (ev.getDisplayName() != null) {
							vsetName.append(ev.getDisplayName());
						} else
							vsetName.append(concepts.stream().findFirst().get().getName());
					}
					if (i==2)
						vsetName.append("AndMore");
				}
			else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}

		storeValueSet(vs, setContent, vsetName.toString());
		setContent.add(TTIriRef.iri("urn:uuid:" + vs.getId()).setName(vsetName.toString()));
		return setContent;
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
			CEGImporter.valueSets.put(TTIriRef.iri(iri.getIri()), conceptSet);
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

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme, EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
	}

	private Set<TTIriRef> getValue(VocCodeSystemEx scheme, String originalCode,
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
				return result;
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
			return snomed;
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

	private String getLabel(EQDOCCriterion eqCriterion){

		List<EQDOCColumnValue> cvs= eqCriterion.getFilterAttribute().getColumnValue();
		if (cvs!=null) {
			for (EQDOCColumnValue cv : cvs) {
				if (cv.getValueSet() != null) {
					StringBuilder setIds = new StringBuilder();
					int i = 0;
					for (EQDOCValueSet vs : cv.getValueSet()) {
						i++;
						if (i > 1)
							setIds.append(",");
						setIds.append(vs.getId());
					}
					if (labels.get(setIds.toString()) != null)
						return (String) labels.get(setIds.toString());
					else {
						i = 0;
						for (EQDOCValueSet vs : cv.getValueSet()) {
							i++;
							if (vs.getValues() != null)
								if (vs.getValues().get(0).getDisplayName() != null) {
									counter++;
									return (vs.getValues().get(0).getDisplayName().split(" ")[0] + "_" + counter);
								}
						}
					}
				}
				else if (cv.getLibraryItem()!=null){
					StringBuilder setIds = new StringBuilder();
					int i = 0;
					for (String item:cv.getLibraryItem()){
						i++;
						if (i>1)
							setIds.append(",");
						setIds.append(item);
					}
					if (labels.get(setIds.toString()) != null)
						return (String) labels.get(setIds.toString());

				}
			}
		}
		if (labels.get(eqCriterion.getId())!=null)
			return (String) labels.get(eqCriterion.getId());
		counter++;
		return "NoAlias_"+counter;
	}

	private String summarise(Where match,Where context,EQDOCTestAttribute testAtt){
		StringBuilder summary= new StringBuilder();
		for (EQDOCColumnValue cv:testAtt.getColumnValue()){
			if (cv.getValueSet() != null) {
				StringBuilder setIds = new StringBuilder();
				int i = 0;
				for (EQDOCValueSet vs : cv.getValueSet()) {
					i++;
					if (i > 1)
						setIds.append(",");
					setIds.append(vs.getId());
				}
				if (labels.get(setIds.toString()) != null)
					summary.append((String) labels.get(setIds.toString()));
			}
			if (cv.getLibraryItem()!=null){
				StringBuilder setIds = new StringBuilder();
				int i = 0;
				for (String item:cv.getLibraryItem()){
					i++;
					if (i>1)
						setIds.append(",");
					setIds.append(item);
				}
				if (labels.get(setIds.toString()) != null)
					summary.append((String) labels.get(setIds.toString()));
			}
		}
		if (match.getWhere()!=null) {
				summariseWhere(match.getWhere(),summary,context);
				if (match.getWhere().getAnd()!=null){
				for (Where where : match.getWhere().getAnd()) {
					summariseWhere(where, summary, context);
				}
			}
			if (summary.toString().isEmpty())
					summary.append("No Label yet");
			}
			return summary.toString();
	}

	private void summariseWhere(Where where,StringBuilder summary,Where context) {
		if (where.getValue() != null) {
			if (summary.toString().isEmpty())
				summary.append(context.getAlias());
			summary.append(compTerm(where.getComparison(), where.getValue()));
		}
		else if (where.getRange() != null) {
			if (summary.toString().isEmpty())
				summary.append(context.getAlias());
			summary.append("From").append(compTerm(where.getRange().getFrom().getComparison(),
				where.getRange().getFrom().getValue()));
			summary.append("To").append(compTerm(where.getRange().getTo().getComparison(),
				where.getRange().getTo().getValue()));
		}
	}

	private String compTerm(String comp,String value) {
		switch (comp) {
			case ">=":
				return value+"OrOver";
			case ">":
				return "Over"+value;
			case "<":
				return "LessThan"+value;
			case "<=":
				return value+"OrLess";
		}
		return comp;
	}





}
