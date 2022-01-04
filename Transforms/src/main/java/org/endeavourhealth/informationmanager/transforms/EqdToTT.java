package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.query.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.PROV;
import org.endeavourhealth.informationmanager.transforms.eqd.*;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToTT {
	private TTIriRef owner;
	private Properties dataMap;
	private Properties criteriaLabels;
	private Properties duplicateOrs;
	private QueryDocument queryDocument;

	private TTDocument document;

	private final Set<String> users= new HashSet<>();



	public void convertDoc(TTDocument document,QueryDocument queryDocument,
												 TTIriRef mainFolder,EnquiryDocument eqd,
															 TTIriRef graph,
															 TTIriRef owner,
															 Properties dataMap, Properties duplicateOrs,
												 Properties criteriaLabels,
												 Map<String,String> reportNames) throws DataFormatException, JsonProcessingException {
		this.owner = owner;
		this.dataMap = dataMap;
		this.document= document;
		this.queryDocument= queryDocument;
		this.criteriaLabels= criteriaLabels;
		this.duplicateOrs= duplicateOrs;
		convertFolders(mainFolder,eqd);
		convertReports(eqd,reportNames);
	}



	private void convertReports(EnquiryDocument eqd,Map<String,String> reportNames) throws DataFormatException, JsonProcessingException {
		for (EQDOCReport eqReport : Objects.requireNonNull(eqd.getReport())) {


			if (eqReport.getId() == null)
				throw new DataFormatException("No report id");
			if (eqReport.getName() == null)
				throw new DataFormatException("No report name");
			String iri ="urn:uuid:"+eqReport.getId();
			TTEntity report = new TTEntity()
				.setIri(iri)
				.addType(IM.QUERY)
				.setName(eqReport.getName())
				.setDescription(eqReport.getDescription());
			document.addEntity(report);

			if (eqReport.getFolder() != null)
				report.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri("urn:uuid:"+ eqReport.getFolder()));
			if (eqReport.getAuthor() != null)
				report.set(IM.WAS_AUTHORED_BY, getPersonInRole(eqReport.getAuthor().getUserInRole()));
			setProvenance(eqReport, report);

			if (eqReport.getPopulation() != null) {
				EqdToQuery eqdToQuery = new EqdToQuery();
				Query qry= eqdToQuery.convertReport(eqReport,document,dataMap,duplicateOrs,criteriaLabels,reportNames);
				queryDocument.addQuery(qry);
				ObjectMapper objectMapper= new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String json=objectMapper.writeValueAsString(qry);
				report.set(IM.QUERY_DEFINITION,TTLiteral.literal(json));

			}

		}
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
			document.addEntity(activity);

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
				if (eqFolder.getAuthor()!=null)
					if (eqFolder.getAuthor().getAuthorName()!=null)
						folder.addObject(IM.WAS_AUTHORED_BY,getPersonInRole(
							eqFolder.getAuthor().getAuthorName()));
			}
		}
	}

	private TTIriRef getPersonInRole(String name) {
		UUID uuid= null;
		try{
			uuid = UUID.fromString(name);

		} catch (IllegalArgumentException ignored) {
		}
		String agentIri;
		if (uuid!=null) {
			agentIri = "urn:uuid:" + uuid;
			name="Unknown user";
		}
		else
			agentIri= getagentIri(name);
		if (!users.contains(agentIri)){
			users.add(agentIri);
			TTEntity agent = new TTEntity()
					.setIri(agentIri)
					.addType(TTIriRef.iri(IM.NAMESPACE + "PersonInRole"))
					.setName(name);
			agent.addObject(IM.HAS_ROLE_IN, TTIriRef.iri(owner.getIri()));
			document.addEntity(agent);
		}
		return TTIriRef.iri(agentIri);
	}

	private String getagentIri(String name) {
		return owner.getIri().replace("org.","uir.")+"/personrole#"+
			CaseUtils.toCamelCase(name
					.replace(" ",""),true)
				.replace("(","_")
				.replace(")","_");
	}


}
