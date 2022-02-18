package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WinPathKingsImport implements TTImport {


	private static final String[] kingsWinPath = {".*\\\\Kings\\\\Winpath.txt"};
	private TTDocument document;
	private Map<String, List<String>> readToSnomed = new HashMap<>();
	private final Map<String, List<String>> snomedToWinpath = new HashMap<>();

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		document= manager.createDocument(IM.GRAPH_KINGS_WINPATH.getIri());
		document.addEntity(manager.createGraph(IM.GRAPH_KINGS_WINPATH.getIri(),"Kings Winpath pathology code scheme and graph",
			"The Kings pathology Winpath LIMB local code scheme and graph"));
		TTManager backManager = new TTManager();
		TTManager vsetManager = new TTManager();
		setTopLevel();
		importR2Matches();
		importWinPathKings(config.getFolder());
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
		return this;

	}
	private void setTopLevel() {
		TTEntity kings= new TTEntity()
			.setIri(IM.GRAPH_KINGS_WINPATH.getIri()+"KingsWinPathCodes")
			.addType(IM.CONCEPT)
			.setName("Kings College Hospital  Winpath codes")
			.setCode("KingsWinPathCodes")
			.setScheme(IM.CODE_SCHEME_KINGS_WINPATH)
			.setDescription("Local codes for the Winpath pathology system in kings")
			.set(IM.IS_CONTAINED_IN,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies")));
		document.addEntity(kings);
	}


	private void importR2Matches() throws SQLException, ClassNotFoundException, TTFilerException {
		System.out.println("Retrieving read vision 2 snomed map");
		readToSnomed= ImportUtils.importReadToSnomed();

	}

	private void importWinPathKings(String folder) throws IOException {
		System.out.println("Importing kings code file");

		Path file = ImportUtils.findFileForId(folder, kingsWinPath[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine(); // NOSONAR - Skip header
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				String readCode = fields[2];
				String code = fields[0] + "-" + (fields[1].toLowerCase());
				String iri = IM.CODE_SCHEME_KINGS_WINPATH.getIri() + fields[0].replace(" ", "") + "-" + (fields[1].replace(" ", ""));
				TTEntity entity = new TTEntity()
					.setIri(iri)
					.addType(IM.CONCEPT)
					.setName(fields[1])
					.setDescription("Local winpath Kings trust pathology system entity ")
					.setScheme(IM.CODE_SCHEME_KINGS_WINPATH)
					.set(IM.IS_CHILD_OF,new TTArray().add(TTIriRef.iri(IM.GRAPH_KINGS_APEX.getIri()+"KingsWinPathCodes")))
					.setCode(code);
				document.addEntity(entity);
				if (readToSnomed.get(readCode) != null) {
					for (String snomed : readToSnomed.get(readCode)) {
						entity.addObject(IM.MATCHED_TO,TTIriRef.iri(SNOMED.NAMESPACE+snomed));
					}
				}

				count++;
				if (count % 500 == 0) {
					System.out.println("Processed " + count + " records");
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count + " records");
		}

	}


	@Override
	public TTImport validateFiles(String inFolder) {
		ImportUtils.validateFiles(inFolder,kingsWinPath);
		return null;
	}

}
