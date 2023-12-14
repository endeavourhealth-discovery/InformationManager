package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CPRDImport implements TTImport {
	private static final Logger LOG = LoggerFactory.getLogger(CPRDImport.class);

	private static final String[] obsCodes = {".*\\\\CPRD\\\\CPRDAurumMedical.txt"};
	private static final String[] drugCodes = {".*\\\\CPRD\\\\CPRDAurumProduct.txt"};

	private final TTManager manager = new TTManager();
	private TTDocument document;


	public CPRDImport() {
	}


	/**
	 * Imports CPRD  identifiers codes and creates term code map to Snomed or local legacy entities
	 * @param config import configuration data
	 * @throws Exception From document filer
	 */


	public void importData(TTImportConfig config) throws Exception {
		document = manager.createDocument(IM.GRAPH_CPRD_MED.iri);
		document.addEntity(manager.createGraph(IM.GRAPH_CPRD_MED.iri, "CPRD medIds ",
			"CPRD clinical non product identifiers (including emis code ids)."));

		importObsCodes(config.getFolder());

		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
		document = manager.createDocument(IM.GRAPH_CPRD_PROD.iri);
		document.addEntity(manager.createGraph(IM.GRAPH_CPRD_PROD.iri, "CPRD product ids",
			"internal identifiers to DMD VMPs and AMPs."));

		importDrugs(config.getFolder());
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
	}



	private void importDrugs(String folder) throws IOException {
		Path file =  ImportUtils.findFileForId(folder, drugCodes[0]);
		int count = 0;
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				count++;
				if (count % 10000 == 0)
					LOG.info("Written {} drug concepts for " + document.getGraph().getIri(), count);
				TTEntity concept = new TTEntity();
				String drugId = (fields[0]);
				concept.setIri(IM.CODE_SCHEME_CPRD_PROD.iri + "Product_" + drugId);
				concept.setName(fields[2]);
				concept.setCode(drugId);
				concept.setScheme(IM.CODE_SCHEME_CPRD_PROD);
				concept.setStatus(IM.ACTIVE);
				if (!fields[1].equals("")) {
					concept.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE.iri + fields[1]));
				}
				document.addEntity(concept);
				line= reader.readLine();
			}
			LOG.info("Written {} entities for " + document.getGraph().getIri(), count);
		}

	}




	private void importObsCodes(String folder) throws IOException {
		Path file =  ImportUtils.findFileForId(folder, obsCodes[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			int count = 0;
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				count++;
				if (count % 10000 == 0)
					LOG.info("Written {} medical concepts for " + document.getGraph().getIri(), count);
				TTEntity concept = new TTEntity();
				String medId = (fields[0]);
				concept.setIri(IM.CODE_SCHEME_CPRD_MED.iri + "Medical_" + medId);
				concept.setName(fields[4]);
				concept.setCode(medId);
				concept.setScheme(IM.CODE_SCHEME_CPRD_MED);
				concept.setStatus(IM.ACTIVE);
				concept.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE.iri + fields[5]));
				TTNode termCode = new TTNode();
				termCode.set(IM.CODE, TTLiteral.literal(fields[6]));
				concept.addObject(IM.HAS_TERM_CODE, termCode);
				document.addEntity(concept);
				line= reader.readLine();
			}
			LOG.info("Written {} entities for " + document.getGraph().getIri(), count);
		}

	}




	public void validateFiles(String inFolder){
		ImportUtils.validateFiles(inFolder,obsCodes, drugCodes);
	}

	@Override
	public void close() throws Exception {
		manager.close();
	}
}

