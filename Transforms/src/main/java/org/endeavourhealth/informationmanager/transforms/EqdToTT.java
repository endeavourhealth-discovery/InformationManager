package org.endeavourhealth.informationmanager.transforms;

import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.query.Query;
import org.endeavourhealth.imapi.query.Step;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.PROV;
import org.endeavourhealth.informationmanager.transforms.eqd.*;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToTT {
	private String baseIri;
	private TTIriRef owner;
	private Properties dataMap;
	private List<TTEntity> entities= new ArrayList<>();
	private final Map<String,String> folderMap= new HashMap<>();
	private final Map<String,String> reportMap= new HashMap<>();
	private final Map<String,TTEntity> agentMap= new HashMap<>();
	private enum StepPattern {SEQUENCE,AND,OR}

	public List<TTEntity> convertDoc(EnquiryDocument eqd,
																	 String baseIri,
																	 TTIriRef owner,
																	 Properties dataMap) throws DataFormatException {
		this.baseIri= baseIri;
		this.owner= owner;
		this.dataMap= dataMap;
		convertFolders(eqd);
		convertReports(eqd);
		return entities;
	}

	private void convertReports(EnquiryDocument eqd) throws DataFormatException {
		for (EQDOCReport eqReport: Objects.requireNonNull(eqd.getReport())){
			if (eqReport.getId()==null)
				throw new DataFormatException("No report id");
			if (eqReport.getName()==null)
				throw new DataFormatException("No report name");
			String iri= baseIri+"/report#"+ CaseUtils.toCamelCase(eqReport.getName().replace(" ",""),true);

			TTEntity report= new TTEntity()
				.setIri(iri)
				.addType(IM.QUERY)
				.setName(eqReport.getName())
				.setDescription(eqReport.getDescription());
			if (eqReport.getFolder()!=null)
				report.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(folderMap.get(eqReport.getFolder())));
			if (eqReport.getAuthor()!=null)
				report.set(IM.WAS_AUTHORED_BY,getPersonInRole(eqReport.getAuthor().getAuthorName()));
			Query qry= new Query();
			if (eqReport.getPopulationType()== VocPopulationType.PATIENT)
				qry.setMainSubject(IM.NAMESPACE+"Patient");
			Boolean activeOnly=false;
			if (eqReport.getParent().getParentType()== VocPopulationParentType.ACTIVE) {
				activeOnly = true;
				if (eqReport.getPopulation()!=null)
					convertPopulation(eqReport.getPopulation(),qry,activeOnly);
			}

		}
	}

	private void convertPopulation(EQDOCPopulation population, Query qry, Boolean activeOnly) {
		StepPattern pattern = getStepPattern(population);
		for (EQDOCCriteriaGroup eqGroup : Objects.requireNonNull(population.getCriteriaGroup())) {
			if (pattern == StepPattern.SEQUENCE) {
				for (EQDOCCriteria eqCriteria : eqGroup.getDefinition().getCriteria()) {
					Step step = qry.addStep();
				}
			}
		}
	}
	private StepPattern getStepPattern(EQDOCPopulation population){
		StepPattern pattern= StepPattern.SEQUENCE;
		for (EQDOCCriteriaGroup eqGroup: Objects.requireNonNull(population.getCriteriaGroup())){
			if (eqGroup.getActionIfFalse()==VocRuleAction.NEXT)
				pattern= StepPattern.OR;
		}
		return pattern;
	}

	private void setProvenance(EQDOCReport eqReport, TTEntity report){
		if (eqReport.getCreationTime()!=null){
			TTEntity activity= new TTEntity()
				.setIri(report.getIri()+"_prov")
				.addType(PROV.ACTIVITY)
					.setName("Authored")
						.setDescription("Query authored");
			activity.set(PROV.ENDED_AT_TIME, TTLiteral.literal(
				eqReport.getCreationTime().toString()));
			entities.add(activity);

		}
	}

	private void convertFolders(EnquiryDocument eqd) throws DataFormatException {
		List<EQDOCFolder> eqFolders= eqd.getReportFolder();
		if (eqFolders!=null){
			for (EQDOCFolder eqFolder:eqFolders) {
				if (eqFolder.getId()==null)
					throw new DataFormatException("No folder id");
				if (eqFolder.getName()==null)
					throw new DataFormatException("No folder name");
				String iri= baseIri+"/folder#"+ CaseUtils.toCamelCase(eqFolder.getName().replace(" ",""),true);
				TTEntity folder = new TTEntity()
					.setIri(iri)
						.addType(IM.FOLDER)
							.setName(eqFolder.getName());
				entities.add(folder);
				folderMap.put(eqFolder.getId(),folder.getIri());
				if (eqFolder.getAuthor()!=null)
					if (eqFolder.getAuthor().getAuthorName()!=null)
						folder.addObject(IM.WAS_AUTHORED_BY,getPersonInRole(
							eqFolder.getAuthor().getAuthorName()));
			}
		}
	}

	private TTIriRef getPersonInRole(String name) {
		String agentIri= getagentIri(name);
		TTEntity agent= agentMap.get(agentIri);
		if (agent==null) {
			agent = new TTEntity()
				.setIri(agentIri)
				.addType(TTIriRef.iri(IM.NAMESPACE + "PersonInRole"))
				.setName(name);
			agent.addObject(IM.HAS_ROLE_IN, TTIriRef.iri(owner.getIri()));
			entities.add(agent);
			agentMap.put(agentIri,agent);
		}
		return TTIriRef.iri(agent.getIri());
	}

	private String getagentIri(String name) {
		String agentIri= owner.getIri().replace("org.","uir.")+"/personrole#"+
			CaseUtils.toCamelCase(name
					.replace(" ",""),true)
				.replace("(","_")
				.replace(")","_");
		return agentIri;
	}


}
