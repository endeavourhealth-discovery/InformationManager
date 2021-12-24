package org.endeavourhealth.informationmanager.transforms;

import org.apache.commons.text.CaseUtils;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.query.Query;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.eqd.EQDOCFolder;
import org.endeavourhealth.informationmanager.transforms.eqd.EQDOCReport;
import org.endeavourhealth.informationmanager.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.informationmanager.transforms.eqd.VocPopulationType;

import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToTT {
	private String baseIri;
	private TTIriRef owner;
	private List<TTEntity> entities= new ArrayList<>();
	private final Map<String,String> folderMap= new HashMap<>();
	private final Map<String,String> reportMap= new HashMap<>();
	private final Map<String,TTEntity> agentMap= new HashMap<>();

	public List<TTEntity> convertDoc(EnquiryDocument eqd,
																	 String baseIri,
																	 TTIriRef owner) throws DataFormatException {
		this.baseIri= baseIri;
		this.owner= owner;
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
			Query qry= new Query();
			if (eqReport.getPopulationType()== VocPopulationType.PATIENT)
				qry.setMainSubject(IM.NAMESPACE+"Patient");


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
						folder.addObject(IM.HAS_AUTHOR,getPersonInRole(
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
