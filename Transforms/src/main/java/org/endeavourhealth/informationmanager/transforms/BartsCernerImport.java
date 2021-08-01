package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class BartsCernerImport implements TTImport {

	private static final String[] codes = {".*\\\\Barts\\\\Barts-Cerner-Codes.txt"};
	private static final String[] maps = {".*\\\\Barts\\\\Snomed-Barts-Cerner.txt"};


	private final Map<String, TTEntity> codeToConcept= new HashMap<>();
	private Set<String> snomedCodes;


	private TTDocument document;
	private final Map<String,TTEntity> r2TermIdMap= new HashMap<>();
	private final Set<String> preferredId = new HashSet<>();
	private Connection conn;
	private final TTManager manager= new TTManager();
	private TTDocument mapDocument;

	private final Map<String,TTEntity> entityMap = new HashMap<>();


	private class Snomed {
		String entityId;
		String descId;
	}
	@Override
	public TTImport importData(String inFolder, boolean bulkImport, Map<String, Integer> entityMap) throws Exception {
		conn= ImportUtils.getConnection();
		System.out.println("importing vision codes");
		System.out.println("retrieving snomed codes from IM");
		snomedCodes= ImportUtils.importSnomedCodes(conn);
		document= manager.createDocument(IM.GRAPH_BARTS_CERNER.getIri());
		mapDocument= manager.createDocument(IM.MAP_SNOMED_BC.getIri());
		importCodes(inFolder);
		TTDocumentFiler filer = new TTDocumentFilerJDBC();
		filer.fileDocument(document,bulkImport,entityMap);
		filer = new TTDocumentFilerJDBC();
		filer.fileDocument(mapDocument,bulkImport,entityMap);
		return this;
	}

	private void importCodes(String inFolder) {

	}


	@Override
	public TTImport validateFiles(String inFolder) {
			ImportUtils.validateFiles(inFolder,codes, maps);
			return this;
	}

	@Override
	public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
		return this;
	}

	@Override
	public void close() throws Exception {

	}
}
