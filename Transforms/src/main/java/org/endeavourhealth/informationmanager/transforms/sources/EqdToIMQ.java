package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.cdm.ProvActivity;
import org.endeavourhealth.imapi.model.cdm.ProvAgent;
import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.EQDOCFolder;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.EQDOCReport;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.EnquiryDocument;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.zip.DataFormatException;

public class EqdToIMQ {
	private final EqdResources resources= new EqdResources();
	private static final Set<String> roles = new HashSet<>();
	private TTIriRef owner;



	public void convertDoc(TTDocument document, TTIriRef mainFolder, TTIriRef fieldGroupFolder, TTIriRef valueSetFolder, EnquiryDocument eqd, TTIriRef owner, Properties dataMap,
												 Properties criteriaLabels) throws DataFormatException, IOException {
		this.owner = owner;
		resources.setDataMap(dataMap);
		resources.setDocument(document);
		resources.setLabels(criteriaLabels);
		resources.setValueSetFolder(valueSetFolder);
		addReportNames(eqd);
		convertFolders(mainFolder, eqd);
		convertReports(eqd);
	}

	private void addReportNames(EnquiryDocument eqd) {
		for (EQDOCReport eqReport : Objects.requireNonNull(eqd.getReport())) {
			if (eqReport.getId() != null)
				resources.reportNames.put(eqReport.getId(), eqReport.getName());
		}

	}

	private void convertReports(EnquiryDocument eqd) throws DataFormatException, IOException {
		for (EQDOCReport eqReport : Objects.requireNonNull(eqd.getReport())) {
			if (eqReport.getId() == null)
				throw new DataFormatException("No report id");
			if (eqReport.getName() == null)
				throw new DataFormatException("No report name");
			System.out.println(eqReport.getName());
			TTEntity qry = convertReport(eqReport);
			resources.getDocument().addEntity(qry);
			setProvenance(qry.getIri(), "CEG");
		}
	}

	private void convertFolders(TTIriRef mainFolder, EnquiryDocument eqd) throws DataFormatException {
		List<EQDOCFolder> eqFolders = eqd.getReportFolder();
		if (eqFolders != null) {
			for (EQDOCFolder eqFolder : eqFolders) {
				if (eqFolder.getId() == null)
					throw new DataFormatException("No folder id");
				if (eqFolder.getName() == null)
					throw new DataFormatException("No folder name");
				String iri = "urn:uuid:" + eqFolder.getId();
				TTEntity folder = new TTEntity()
					.setIri(iri)
					.addType(IM.FOLDER)
					.setName(eqFolder.getName())
					.set(IM.IS_CONTAINED_IN, mainFolder);
				resources.getDocument().addEntity(folder);
				if (eqFolder.getAuthor() != null && eqFolder.getAuthor().getAuthorName() != null)
					setProvenance(iri, eqFolder.getAuthor().getAuthorName());
			}
		}
	}

	private void setProvenance(String iri, String authorName) {
		ProvActivity activity = new ProvActivity()
			.setIri("urn:uuid:" + UUID.randomUUID())
			.setActivityType(IM.PROV_CREATION)
			.setEffectiveDate(LocalDateTime.now().toString());
		resources.getDocument().addEntity(activity);
		if (authorName != null) {
			String uir = getPerson(authorName);
			ProvAgent agent = new ProvAgent()
				.setPersonInRole(TTIriRef.iri(uir))
				.setParticipationType(IM.AUTHOR_ROLE);
			agent.setName(authorName);
			agent.setIri(uir.replace("uir.", "agent."));
			activity.addAgent(TTIriRef.iri(agent.getIri()))
				.setTargetEntity(TTIriRef.iri(iri));
			if (!roles.contains(agent.getIri())) {
				resources.getDocument().addEntity(agent);
				roles.add(agent.getIri());
			}

		}
	}

	private String getPerson(String name) {
		StringBuilder uri = new StringBuilder();
		name.chars().forEach(c -> {
			if (Character.isLetterOrDigit(c))
				uri.append(Character.toString(c));
		});
		String root = owner.getIri();
		root = root.substring(0, root.lastIndexOf("#"));
		return root.replace("org.", "uir.") + "/personrole#" +
			uri;
	}

	public TTEntity convertReport(EQDOCReport eqReport) throws DataFormatException, IOException {

		resources.setActiveReport(eqReport.getId());
		TTEntity entity = new TTEntity();
		entity.setIri("urn:uuid:" + eqReport.getId());
		entity.setName(eqReport.getName());
		entity.setDescription(eqReport.getDescription().replace("\n", "<p>"));
		if (eqReport.getFolder() != null)
			entity.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri("urn:uuid:" + eqReport.getFolder()));
		if (eqReport.getCreationTime() != null)
			setProvenance(entity.getIri(), null);
		entity.addType(IM.QUERY);

		Query query= new Query();
		query.setIri(entity.getIri());
		query.setName(entity.getName());
		query.setDescription(entity.getDescription());

		if (eqReport.getPopulation() != null) {
			new EqdPopToIMQ().convertPopulation(eqReport, query,resources);
		}
		else 	if (eqReport.getListReport() != null) {
			new EqdListToIMQ().convertReport(eqReport, query,resources);
		}
		else
			new EqdAuditToIMQ().convertReport(eqReport,query,resources);


		ObjectMapper objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
		objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
		String json = objectMapper.writeValueAsString(query);
		entity.set(IM.QUERY_DEFINITION, TTLiteral.literal(json));
		return entity;
	}



}
