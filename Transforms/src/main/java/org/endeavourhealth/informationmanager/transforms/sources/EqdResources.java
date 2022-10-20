package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.collections4.CollectionUtils;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.model.iml.*;
import org.endeavourhealth.imapi.model.tripletree.*;
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
	private final Map<String,Set<TTIriRef>> valueMap= new HashMap<>();
	private int counter=0;
	private int whereCount=0;




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



	public void setDataMap(Properties dataMap) {
		this.dataMap = dataMap;
	}

	public void setLabels(Properties labels) {
		this.labels = labels;
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
	public void setFrom(Query query, TTIriRef parent) {
		TTAlias from = new TTAlias();
		from.setIsSet(true);
		from.setIri(parent.getIri());
		if (parent.getName()!=null)
			from.setName(parent.getName());
		query.addFrom(from);
	}




	public void convertCriteria(EQDOCCriteria eqCriteria,
															 Where where) throws DataFormatException, IOException {

		if ((eqCriteria.getPopulationCriterion() != null)) {
			EQDOCSearchIdentifier srch = eqCriteria.getPopulationCriterion();
			where.from(f->f
					.setIsSet(true)
					.setIri("urn:uuid:" + srch.getReportGuid())
					.setName(reportNames.get(srch.getReportGuid()))
				);
		} else {
			convertCriterion(eqCriteria.getCriterion(),where);
		}
	}





	private void convertCriterion(EQDOCCriterion eqCriterion, Where match) throws DataFormatException, IOException {
		Where negatationWhere=match;
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
		if (eqCriterion.isNegation()){
			negatationWhere.setAlias(summarise(negatationWhere,null));
		}

	}



	private void convertLinkedCriterion(EQDOCCriterion eqCriterion, Where topWhere) throws DataFormatException, IOException {
		Where targetWhere;
		if (eqCriterion.getFilterAttribute().getRestriction() != null) {
			if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				convertRestrictionCriterion(eqCriterion, topWhere);
			}
			else {
				targetWhere= new Where();
				topWhere.addAnd(targetWhere);
				convertRestrictionCriterion(eqCriterion,targetWhere);
			}
		}
		else {
			targetWhere = new Where();
			topWhere.addAnd(targetWhere);
			convertColumns(eqCriterion, targetWhere);
		}
		targetWhere= topWhere.getAnd().get(topWhere.getAnd().size()-1);
		Where linkWhere;
		EQDOCLinkedCriterion eqLinked= eqCriterion.getLinkedCriterion();
		EQDOCCriterion eqLinkedCriterion= eqLinked.getCriterion();
		if (eqLinkedCriterion.getFilterAttribute().getRestriction() != null) {
			if (eqLinkedCriterion.getFilterAttribute().getRestriction().getTestAttribute() != null) {
				convertRestrictionCriterion(eqLinkedCriterion, topWhere);

			} else {
				linkWhere = new Where();
				topWhere.addAnd(linkWhere);
				convertColumns(eqCriterion, linkWhere);
			}
		}
		else {
			linkWhere = new Where();
			topWhere.addAnd(linkWhere);
			convertColumns(eqCriterion, linkWhere);
		}
		Where relationWhere;
		Where lastLinkWhere= topWhere.getAnd().get(topWhere.getAnd().size()-1);
		if (lastLinkWhere.getAnd()==null){
			topWhere.getAnd().remove(lastLinkWhere);
			relationWhere= new Where();
			topWhere.addAnd(relationWhere);
			relationWhere.setPath(lastLinkWhere.getPath());
			relationWhere.setFrom(lastLinkWhere.getFrom());
			lastLinkWhere.setPath(null);
			lastLinkWhere.setFrom(null);
			relationWhere.addAnd(lastLinkWhere);
		}
		else
			relationWhere= lastLinkWhere;
		EQDOCRelationship eqRel = eqLinked.getRelationship();
		if (eqRel.getParentColumn().contains("DATE")){
			Where compareWhere= new Where();
			relationWhere.addAnd(compareWhere);
			compareWhere.setProperty(new TTAlias().setIri(IM.NAMESPACE+"date"));
			Value value= new Value();
			compareWhere.setValue(value);
			Where finalTargetWhere = targetWhere;
			value
			.setComparison(vocabMap.get(eqRel.getRangeValue().getRangeFrom().getOperator()))
				.setValue(eqRel.getRangeValue().getRangeFrom().getValue().getValue())
				.relativeTo(c ->c
					.setAlias(finalTargetWhere.getAlias())
			.setProperty(new TTAlias().setIri(IM.NAMESPACE+"date")));
			compareWhere.addArgument(new Argument()
				.setParameter("units")
				.setValueData(eqRel.getRangeValue().getRangeFrom().getValue().getUnit().value()));
			compareWhere.setAlias(summarise(compareWhere,null));
		}
		else
			throw new DataFormatException("Only date link fields supported at the moment");


	}


	private void convertRestrictionCriterion(EQDOCCriterion eqCriterion, Where topWhere) throws DataFormatException, IOException {
		Where restrictionWhere;
		if (eqCriterion.getFilterAttribute().getRestriction().getTestAttribute()!=null) {
			restrictionWhere = new Where();
			topWhere.addAnd(restrictionWhere);
		}
		else
			restrictionWhere= topWhere;
		if (eqCriterion.getDescription() != null)
			restrictionWhere.setDescription(eqCriterion.getDescription());
		convertColumns(eqCriterion, restrictionWhere);
		restrictionTest(eqCriterion, topWhere, restrictionWhere);
	}

	private void restrictionTest(EQDOCCriterion eqCriterion, Where topWhere, Where restrictionWhere) throws IOException, DataFormatException {
		setRestriction(eqCriterion, restrictionWhere);
		EQDOCTestAttribute testAtt= eqCriterion.getFilterAttribute().getRestriction().getTestAttribute();
		if (testAtt != null) {
				Where testWhere= new Where();
				topWhere.addAnd(testWhere);
				testWhere.from(f->f
					.setAlias(restrictionWhere.getAlias()));
				List<EQDOCColumnValue> cvs= testAtt.getColumnValue();
				if (cvs.size()==1){
					setMainCriterion(eqCriterion.getTable(), cvs.get(0), testWhere,"");
					if (testWhere.getAlias()==null)
						testWhere.setAlias(summarise(testWhere,testAtt.getColumnValue().get(0)));
				}
				else {
					for (EQDOCColumnValue cv : cvs) {
						Where pv = new Where();
						testWhere.addAnd(pv);
						setMainCriterion(eqCriterion.getTable(), cv, pv,"");
						if (pv.getAlias()==null)
							pv.setAlias(summarise(pv,cv));
					}
					testWhere.setAlias(summarise(testWhere,null));
				}

			}
	}


	private void convertColumns(EQDOCCriterion eqCriterion, Where match) throws DataFormatException, IOException {
		match.setAlias(getLabel(eqCriterion));
		EQDOCFilterAttribute filterAttribute = eqCriterion.getFilterAttribute();
		String entityPath= getPath(eqCriterion.getTable());
		List<EQDOCColumnValue> cvs= filterAttribute.getColumnValue();
		if (cvs.size()==1){
			setMainCriterion(eqCriterion.getTable(), cvs.get(0),match,entityPath);
			match.setAlias(summarise(match,cvs.get(0)));
		}
		else {
			if (!entityPath.equals(""))
				match.setPath(entityPath);
			for (EQDOCColumnValue cv : filterAttribute.getColumnValue()) {
				Where pv = new Where();
				match.addAnd(pv);
				setMainCriterion(eqCriterion.getTable(), cv, pv,"");
				pv.setAlias(summarise(pv,cv));
			}
		}
	}




	private void setMainCriterion(String eqTable, EQDOCColumnValue cv, Where pv,String mainPath) throws DataFormatException, IOException {
		String eqColumn= String.join("/",cv.getColumn());
		setWhere(cv, eqTable, eqColumn, pv,mainPath);
	}



	private void setWhere(EQDOCColumnValue cv, String eqTable, String eqColumn,
																Where pv,String mainPath) throws DataFormatException, IOException {
		String subPath= getPath(eqTable+"/"+ eqColumn);
		if (!mainPath.equals(""))
			subPath= mainPath+" "+ subPath;
		if (subPath.contains(" ")) {
			pv.setPath(subPath.substring(0, subPath.lastIndexOf(" ") ));
			pv.setProperty(new TTAlias().setIri(subPath.substring(subPath.indexOf(" ")+1)));
		}
		else
			pv.setProperty(new TTAlias().setIri(subPath));
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
							pv.setIn(getInlineValues(vs));
					}
					else {
							pv.setNot(true);
							pv.setIn(getInlineValues(vs));
						}
				}
			}
		} else if (!CollectionUtils.isEmpty(cv.getLibraryItem())) {
			for (String vset : cv.getLibraryItem()) {
				String vsetName = "Unknown code set";
				if (labels.get(vset) != null)
					vsetName = (String) labels.get(vset);
				TTAlias iri = TTAlias.iri("urn:uuid:" + vset).setName(vsetName);
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

	public String getPath(String eqdPath) throws DataFormatException {
		Object target = dataMap.get(eqdPath);
		if (target == null)
			throw new DataFormatException("unknown map : " + eqdPath);
		String targetPath= (String) target;
		if (targetPath.equals(""))
			return "";
		String[] paths = targetPath.split("/");
		List<String> pathList = Arrays.stream(paths).map(p-> IM.NAMESPACE+p).collect(Collectors.toList());
		if (pathList.size()==1){
			return pathList.get(0);
		}
		else {
			return String.join(" ",pathList);
		}
	}


	private void setRestriction(EQDOCCriterion eqCriterion, Where match) throws DataFormatException {
		String eqTable = eqCriterion.getTable();
		String linkColumn = eqCriterion.getFilterAttribute().getRestriction()
			.getColumnOrder().getColumns().get(0).getColumn().get(0);

		match.setLimit(1);
		EQDOCFilterRestriction restrict = eqCriterion.getFilterAttribute().getRestriction();
		String direction;
		if (restrict.getColumnOrder().getColumns().get(0).getDirection() == VocOrderDirection.ASC) {
			direction= "ASC";
			match.setAlias("Earliest"+ match.getAlias());
		}
		else {
			direction= "DESC";
			match.setAlias("Latest" + match.getAlias());
		}
		match.addOrderBy(new OrderBy()
			.setIri(getPath(eqTable + "/" + linkColumn))
			.setDirection(direction));
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
			Function function = getTimeDiff();
			pv.addArgument(new Argument().setParameter("units").setValueData(units));
			pv.setFunction(function);
			if (from) {
				comp = reverseComp(comp);
				value = String.valueOf(-Integer.parseInt(value));
			}
			String finalComp = comp;
			String finalValue = value;
			pv
				.value(v->v
				.setComparison(finalComp)
				.setValue(finalValue)
					.setRelativeTo(new Compare().setVariable("$referenceDate")));
		} else {
			String finalComp1 = comp;
			String finalValue1 = value;
			pv
				.value(v -> v
					.setComparison(finalComp1)
					.setValue(finalValue1));
			if (pv.getProperty().getIri().contains("age")) {
				if (units == null)
					throw new DataFormatException("missing units from age");
				pv.addArgument(new Argument()
					.setParameter("units")
					.setValueData(units));
			}
		}

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


	private Function getTimeDiff() {
		return new Function().setIri(TTIriRef.iri(IM.NAMESPACE + "TimeDifference")
			.setName("Time Difference"));
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

			Function function = getTimeDiff();
			pv.addArgument(new Argument().setParameter("units").setValueData(units));
			range.setFrom(new Value()
				.setComparison(fromComp)
				.setValue(fromValue)
				.setRelativeTo(new Compare().setVariable("$referenceDate")));
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

			range.setTo(new Value()
				.setComparison(toComp)
				.setValue(toValue)
				.setRelativeTo(new Compare().setVariable("$referenceDate")));
			Function function = getTimeDiff();
			pv.addArgument(new Argument().setParameter("units").setValueData(units));
			pv.setFunction(function);
		} else {
			range.setTo(new Value()
				.setComparison(toComp)
				.setValue(toValue));
		}

	}





	private List<TTAlias> getExceptionSet(EQDOCException set) throws DataFormatException, IOException {
		List<TTAlias> valueSet = new ArrayList<>();
		VocCodeSystemEx scheme = set.getCodeSystem();
		for (EQDOCExceptionValue ev : set.getValues()) {
			Set<TTAlias> values = getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
			if (values != null) {
				valueSet.addAll(new ArrayList<>(values));
			} else
				System.err.println("Missing exception sets\t" + ev.getValue() + "\t " + ev.getDisplayName());
		}

		return valueSet;
	}



	private List<TTAlias> getInlineValues(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTAlias> setContent = new ArrayList<>();
		VocCodeSystemEx scheme = vs.getCodeSystem();
		for (EQDOCValueSetValue ev : vs.getValues()) {
			Set<TTAlias> concepts = getValue(scheme, ev);
			if (concepts != null) {
				for (TTAlias iri : concepts) {
					TTAlias conRef = new TTAlias().setIri(iri.getIri()).setName(iri.getName());
					setContent.add(conRef);
				}
			} else
				System.err.println("Missing \t" + ev.getValue() + "\t " + ev.getDisplayName());

		}
		return setContent;
	}


	private List<TTAlias> getValueSet(EQDOCValueSet vs) throws DataFormatException, IOException {
		List<TTAlias> setContent = new ArrayList<>();
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
			Set<TTAlias> concepts = getValue(scheme, ev);
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
		setContent.add(TTAlias.iri("urn:uuid:" + vs.getId()).setName(vsetName.toString()));
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

	private void storeValueSet(EQDOCValueSet vs, List<TTAlias> valueSet, String vSetName) throws JsonProcessingException {
		if (vs.getId() != null) {
			TTIriRef iri = TTIriRef.iri("urn:uuid:" + vs.getId()).setName(vSetName);
			if (!CEGImporter.valueSets.containsKey(iri)) {
				TTEntity conceptSet = new TTEntity()
					.setIri(iri.getIri())
					.addType(IM.CONCEPT_SET)
					.setName(vSetName);
				conceptSet.addObject(IM.IS_CONTAINED_IN, valueSetFolder);
				Query definition= new Query();

				for (TTAlias member : valueSet)
					definition.addFrom(member);
				conceptSet.set(IM.DEFINITION,TTLiteral.literal(definition) );
				document.addEntity(conceptSet);
				CEGImporter.valueSets.put(iri, conceptSet);
			}
			CEGImporter.valueSets.get(iri).addObject(IM.USED_IN, TTIriRef.iri("urn:uuid:" + activeReport));
		}
	}

	private Set<TTAlias> getValue(VocCodeSystemEx scheme, EQDOCValueSetValue ev) throws DataFormatException, IOException {
		return getValue(scheme, ev.getValue(), ev.getDisplayName(), ev.getLegacyValue());
	}

	private Set<TTAlias> getValue(VocCodeSystemEx scheme, String originalCode,
																	 String originalTerm, String legacyCode) throws DataFormatException, IOException {
		if (scheme == VocCodeSystemEx.EMISINTERNAL) {
			String key = "EMISINTERNAL/" + originalCode;
			Object mapValue = dataMap.get(key);
			if (mapValue != null) {
				TTAlias iri = new TTAlias(getIri(mapValue.toString()));
				String name = importMaps.getCoreName(iri.getIri());
				if (name != null)
					iri.setName(name);
				Set<TTAlias> result = new HashSet<>();
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
			if (snomed!=null)
				return snomed.stream().map(TTAlias::new).collect(Collectors.toSet());
			else return null;
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

	private String summarise(Where match,EQDOCColumnValue cv){
		StringBuilder summary= new StringBuilder();
		if (cv!=null) {
			if (labels.get(cv.getId()) != null) {
				return labels.get(cv.getId()).toString();
			}
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
			if (cv.getLibraryItem() != null) {
				StringBuilder setIds = new StringBuilder();
				int i = 0;
				for (String item : cv.getLibraryItem()) {
					i++;
					if (i > 1)
						setIds.append(",");
					setIds.append(item);
				}
				if (labels.get(setIds.toString()) != null)
					summary.append((String) labels.get(setIds.toString()));
			}
		}
			if (summary.toString().isEmpty())
					summariseWhere(match,summary);
			if (summary.toString().isEmpty()) {
				whereCount++;
				summary.append("Criteria_").append(whereCount);
			}
			return summary.toString();
	}

	private void summariseWhere(Where where,StringBuilder summary) {
		if (where.getNotExist()!=null) {
			summary.append("Not ");
			summary.append(where.getNotExist().getAlias());
		}
		if (where.isNot())
			summary.append("not in ");
		String path="";
		if (where.getPath()!=null)
			path= localName(where.getPath());
		if (where.getProperty()!=null) {
			String property = localName(where.getProperty().getIri());
			String fullPath = (!path.equals("")) ? path + " " + property : property;
			if (where.getValue() != null) {
				summary.append(fullPath);
				summary.append(" ").append(summariseValue(where.getValue()));
				if (where.getArgument()!=null){
					if (where.getArgument().get(0).getValueData()!=null)
					 summary.append(" ").append(where.getArgument().get(0).getValueData().toLowerCase(Locale.ROOT));
				}
				if (where.getValue().getRelativeTo()!=null)
					summary.append(summariseCompare(where.getValue().getRelativeTo()));
			} else if (where.getRange() != null) {
				if (summary.toString().isEmpty())
					summary.append(fullPath).append(" ");
				summary.append("From").append(summariseValue(where.getRange().getFrom()));
				summary.append(" To").append(summariseValue(where.getRange().getTo()));
				if (where.getRange().getRelativeTo()!=null)
					summary.append(summariseCompare(where.getRange().getRelativeTo()));
			}
			if (where.getArgument()!=null)
				summary.append(summariseArguments(where.getArgument()));


			if (where.getIn()!=null){
				int i=0;
				for (TTIriRef in:where.getIn()){
					i++;
					if (i==1){
						if (in.getName()!=null)
							summary.append(in.getName());
					}
					if (i==2)
						summary.append(" (and more) ");
				}

			}


		}
		else if( where.getAnd()!=null){
			List<String> ands= where.getAnd().stream().map(Where::getAlias).collect(Collectors.toList());
			summary.append(String.join("+",ands));
		}
	}

	private String summariseCompare(Compare compare) {
		if (compare.getVariable()!=null){
			if (compare.getVariable().equals("$referenceDate"))
				return " between date and ref date";
		}
		return "";
	}

	private String summariseArguments(List<Argument> arguments) {
		StringBuilder summary= new StringBuilder();
		for (Argument arg:arguments){
			if (arg.getParameter().equals("units"))
				summary.append(" ").append(arg.getParameter().toLowerCase(Locale.ROOT));
		}
		return summary.toString();

	}


	private String localName(String path){
		if (!path.contains(" "))
			return path.substring(path.lastIndexOf("#")+1);
		else {
			String[] iris= path.split(" ");
			List<String> locals= Arrays.stream(iris).sequential().map(i-> i.substring(i.lastIndexOf("#")+1)).collect(Collectors.toList());
			return String.join(", ",locals);
		}
	}


	private String summariseValue(Value value){
		return value.getComparison() + " " +
			value.getValue();
	}







}
