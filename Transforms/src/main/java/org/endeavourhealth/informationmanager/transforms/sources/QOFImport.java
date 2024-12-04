package org.endeavourhealth.informationmanager.transforms.sources;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.customexceptions.EclFormatException;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.ECLToIMQ;
import org.endeavourhealth.imapi.transforms.OWLToTT;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class QOFImport implements TTImport {

	public static final String[] pcdClusters = {
		".*\\\\QOF\\\\.*\\_PCD_Refset_Content.txt"};
	public static final String[] qofClusters = {
		".*\\\\QOF\\\\QOF_.*\\.txt"};

	private TTDocument document;
	private static final Logger LOG = LoggerFactory.getLogger(QOFImport.class);


	//======================PUBLIC METHODS============================


	@Override
	public void importData(TTImportConfig config) throws ImportException {
		validateFiles(config.getFolder());
		try (TTManager dmanager = new TTManager()){
			document = dmanager.createDocument(GRAPH.QOF);
			importPcdClusters(config.getFolder());
			importQOFClusters(config.getFolder());
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}
		} catch (Exception ex) {
			throw new ImportException(ex.getMessage());
		}



	}


	private void importPcdClusters(String path) throws IOException {
		Set<String> clusterCodes= new HashSet<>();

		for (String clusterFile : pcdClusters) {
			Path file = ImportUtils.findFilesForId(path, clusterFile).get(0);
			LOG.info("Processing qof cluster synonyms in {}", file.getFileName().toString());
			String qofFile = file.toFile().getName();
			String version = qofFile.split("_")[0];
			TTEntity clusters = new TTEntity()
				.setIri(IM.NAMESPACE + "PcdClusters")
				.setName("Primary Care refset Portal Code clusters")
				.setDescription("PCD code cluster reference sets issued on " + version)
				.addType(iri(IM.FOLDER));
			clusters.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
			clusters
				.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "QueryConceptSets"));
			document.addEntity(clusters);
			TTEntity clusterFolder = new TTEntity()
				.setIri(IM.NAMESPACE + "PcdClusters" + version)
				.setName("PCD Code clusters - " + version)
				.setDescription("PCD  code cluster reference sets issued on " + version)
				.addType(iri(IM.FOLDER));
			clusterFolder
				.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "PcdClusters"));
			clusterFolder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
			document.addEntity(clusterFolder);

			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine(); // NOSONAR - Skip header
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					String[] fields = line.split("\t");
					String refset = fields[4];
					String clusterCode = fields[0];
					if (!clusterCodes.contains(clusterCode)) {
						String normalTerm = StringUtils.capitalize(fields[1]).split(" codes")[0] + " (primary care value set)";
						addToReferenceSet(clusterFolder, refset, clusterCode, normalTerm);
						clusterCodes.add(clusterCode);
					}
					line = reader.readLine();
				}
			}
		}
	}
	private void importQOFClusters(String path) throws IOException {
		Set<String> clusterCodes= new HashSet<>();
		int i = 0;
		for (String clusterFile : qofClusters) {
			Path file = ImportUtils.findFilesForId(path, clusterFile).get(0);
			LOG.info("Processing qof cluster synonyms in {}", file.getFileName().toString());
			String qofFile = file.toFile().getName();
			String version = qofFile.split("\\.")[0].split("_")[1];
			TTEntity clusters = new TTEntity()
				.setIri(IM.NAMESPACE + "QofClusters")
				.setName("QOF Code clusters")
				.setDescription("QOF code cluster reference sets issued on " + version)
				.addType(iri(IM.FOLDER));
			clusters.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
			clusters
				.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "QueryConceptSets"));
			document.addEntity(clusters);
			TTEntity clusterFolder = new TTEntity()
				.setIri(IM.NAMESPACE + "QofClusters" + version)
				.setName("QOF Code clusters - " + version)
				.setDescription("QOF code cluster reference sets issued on " + version)
				.addType(iri(IM.FOLDER));
			clusterFolder
				.addObject(iri(IM.IS_CONTAINED_IN), iri(IM.NAMESPACE + "QofClusters"));
			clusterFolder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
			document.addEntity(clusterFolder);

			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine(); // NOSONAR - Skip header
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					String[] fields = line.split("\t");
					String referenceSet = fields[6].substring(1);
					String clusterCode = fields[1];
					if (!clusterCodes.contains(clusterCode)) {
						String normalTerm = fields[2] + " QOF code cluster version " + version;
						addToReferenceSet(clusterFolder, referenceSet, clusterCode, normalTerm);
						clusterCodes.add(clusterCode);
					}
					line = reader.readLine();
				}
			}
		}
	}

	private void addToReferenceSet(TTEntity clusterFolder,String referenceSet, String clusterTerm, String normalTerm) {
		TTEntity c = new TTEntity().setIri(SNOMED.NAMESPACE + referenceSet);
		c.setCrud(iri(IM.ADD_QUADS));
		c.set(iri(IM.PREFERRED_NAME), TTLiteral.literal(normalTerm));
		if (!hasTermCode(c, clusterTerm)) {
			c.addObject(iri(IM.HAS_TERM_CODE), new TTNode().set(iri(RDFS.LABEL), TTLiteral.literal(clusterTerm)));
			c.addObject(iri(IM.IS_CONTAINED_IN), iri(clusterFolder.getIri()));
		}
		document.addEntity(c);
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


	public void validateFiles(String inFolder) {
		ImportUtils.validateFiles(inFolder, qofClusters, pcdClusters);

	}

	@Override
	public void close() throws Exception {

	}
}

