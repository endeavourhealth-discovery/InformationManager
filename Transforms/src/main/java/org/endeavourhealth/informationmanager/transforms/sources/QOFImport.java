package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class QOFImport implements TTImport{

	public static final String[] pcdClusters = {
		".*\\\\QOF\\\\.*\\_PCD_Refset_Content.txt"};


	public static final String PCDFolder= IM.NAMESPACE+"PCDClusters";
	private Map<String, TTEntity> conceptMap;
	private Map<String,TTEntity> qofMap;

	private TTDocument document;
	private static final Logger LOG = LoggerFactory.getLogger(QOFImport.class);
	private boolean isSnomedImporter;

	public QOFImport(){
		isSnomedImporter= false;

	}
 public QOFImport(TTDocument document, Map<String,TTEntity> conceptMap){
	 this.document= document;
	 this.conceptMap= conceptMap;
	 this.isSnomedImporter= true;
 }

	public Map<String, TTEntity> getConceptMap() {
		return conceptMap;
	}

	public QOFImport setConceptMap(Map<String, TTEntity> conceptMap) {
		this.conceptMap = conceptMap;
		return this;
	}




	public void importData(TTImportConfig config) throws ImportException{
		try {
			if (!isSnomedImporter){
				qofMap= new HashMap<>();
				document= new TTDocument(iri(SNOMED.NAMESPACE));
			}
			processSets(config.getFolder());
			if (!isSnomedImporter){
				try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
					filer.fileDocument(document);
				}
			}
		}
		catch (Exception e){
			throw new ImportException(e.getMessage(),e);
		}
	}

	private TTEntity getEntityFromIri(String iri){
		if (isSnomedImporter)
			return conceptMap.get(iri);
		else
			return qofMap.get(iri);
	}

	private void putEntityMap(String iri, TTEntity entity){
		if (isSnomedImporter)
			conceptMap.put(iri,entity);
		else
			qofMap.put(iri,entity);
	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {

		ImportUtils.validateFiles(inFolder, pcdClusters);

	}

	private void processSets(String path) throws IOException {
		TTEntity clusters = new TTEntity()
			.setIri(PCDFolder)
			.setName("Primary Care Code clusters")
			.setDescription("PCD code cluster reference sets")
			.addType(iri(IM.FOLDER));
		putEntityMap(PCDFolder,clusters);
		clusters.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		clusters
			.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "QueryConceptSets"));
		document.addEntity(clusters);
		for (String clusterFile : pcdClusters) {
			Path file = ImportUtils.findFilesForId(path, clusterFile).get(0);
			String qofFile = file.toFile().getName();
			String version = qofFile.split("_")[0];
			getEntityFromIri(PCDFolder).setName("PCD code cluster reference sets ("+ version+")");
			createFoldersForVersion(version);
			importPcdClusters(file,version);
		}
	}

	private void createFoldersForVersion(String version) {
		Map<String, String> folders = Map.of("CC", "Core Contract (CC)", "ES", "Enhanced Service (ES)"
			, "INLIQ", "Indicators No Longer In QOF (INLIQ)"
			, "Other", "Other"
			, "PLE", "Patient Level Extract (PLE)"
			, "NCD", "Network Contract DES (NCD)"
			, "QOF", "Quality and Outcomes Framework (QOF)"
			, "VI", "Vaccination and Immunisations (VI)");
		for (Map.Entry<String, String> folder : folders.entrySet()) {
			createServiceFolder(folder.getKey(), folder.getValue(),version);
		}
	}
	private void createServiceFolder(String serviceMnemonic,String serviceName,String version){
	 String serviceFolderIri= IM.NAMESPACE+"SetServiceFolder_"+serviceMnemonic;
	 TTEntity serviceFolder= getEntityFromIri(serviceFolderIri);
	 if (serviceFolder==null) {
		 serviceFolder = new TTEntity()
			 .setIri(serviceFolderIri)
			 .addType(iri(IM.FOLDER));
		 serviceFolder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
		 serviceFolder.addObject(iri(IM.IS_CONTAINED_IN), iri(PCDFolder));
		 putEntityMap(serviceFolderIri, serviceFolder);
		 document.addEntity(serviceFolder);
	 }
	 serviceFolder.setName(serviceName+" ("+ version+" )");
	}


	private void importPcdClusters(Path file, String version) throws IOException{
		Set<String> clusters= new HashSet<>();
			LOG.info("Processing qof cluster synonyms in {}", file.getFileName().toString());
			String qofFile = file.toFile().getName();
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine(); // NOSONAR - Skip header
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					String[] fields = line.split("\t");
					String refset = fields[4];
					String clusterName= fields[1];
					String clusterCode = fields[0];
					String services= fields[5];
					if (!clusters.contains(refset)) {
						addToReferenceSet(refset, services, clusterName, clusterCode);
						clusters.add(refset);
					}
					line = reader.readLine();
				}
			}
	}

	private void addToReferenceSet(String referenceSet, String services , String clusterTerm, String clusterCode) {
	 TTEntity set=getEntityFromIri(referenceSet);
	 if (set==null){
		 set= new TTEntity()
			 .setIri(SNOMED.NAMESPACE+referenceSet)
			 .setCrud(iri(IM.ADD_QUADS));
		 document.addEntity(set);
	 }
	 set.setType(new TTArray().add(iri(IM.CONCEPT_SET)));
		String[] serviceList = services.split("\\|");
		for (String s : serviceList) {
			String serviceRuleSet = s.trim();
			String service=serviceRuleSet.split("\\s+")[0];
			String ruleSetName = String.join(" ", Arrays.asList(serviceRuleSet.split("\\s+")).subList(1, serviceRuleSet.split("\\s+").length));
			String ruleSetFolderIri = IM.NAMESPACE + "RuleSetFolder_" + service + ruleSetName.replace(" ", "");
			TTEntity ruleSetFolder = getEntityFromIri(ruleSetFolderIri);
			if (ruleSetFolder == null) {
				ruleSetFolder = new TTEntity()
					.setIri(ruleSetFolderIri)
					.setName("Sets for ruleset " +service +" "+ ruleSetName)
					.addType(iri(IM.FOLDER));
				ruleSetFolder.addObject(iri(IM.IS_CONTAINED_IN), IM.NAMESPACE + "SetServiceFolder_" + service);
				putEntityMap(ruleSetFolderIri, ruleSetFolder);
				document.addEntity(ruleSetFolder);
			}
			if (!hasTermCode(set, clusterCode)) {
				set.addObject(iri(IM.HAS_TERM_CODE), new TTNode().set(iri(RDFS.LABEL), TTLiteral.literal(clusterCode)));
			}
			set.setName(clusterTerm);
			set.addObject(iri(IM.IS_CONTAINED_IN), iri(ruleSetFolderIri));
		}
	}


	private boolean hasTermCode(TTEntity entity, String term) {
		if (entity.get(iri(IM.HAS_TERM_CODE)) == null)
			return false;
		for (TTValue tc : entity.get(iri(IM.HAS_TERM_CODE)).getElements()) {
			if (tc.asNode().get(iri(RDFS.LABEL)).asLiteral().getValue()
				.equals(term))
				return true;
		}
		return false;

	}


	@Override
	public void close() throws Exception {

	}
}

