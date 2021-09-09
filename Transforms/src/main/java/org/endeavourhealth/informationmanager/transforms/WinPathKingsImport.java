package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class WinPathKingsImport implements TTImport {


	private static final String[] kingsWinPath = {".*\\\\Kings\\\\Winpath.txt"};
	private TTDocument backMapDocument;
	private TTDocument document;
	private TTDocument valueSetDocument;
	private final Map<String, List<String>> readToSnomed = new HashMap<>();
	private final Map<String, List<String>> snomedToWinpath = new HashMap();
	private static final TTIriRef utl= TTIriRef.iri(IM.NAMESPACE+"VSET_UnifiedTestList");
	private static final Set<String> utlSet= new HashSet<>();
	private static final Set<String> utlMembers= new HashSet<>();
	private Connection conn;

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager = new TTManager();
		document= manager.createDocument(IM.GRAPH_KINGS_WINPATH.getIri());
		TTManager backManager = new TTManager();
		TTManager vsetManager = new TTManager();
		conn = ImportUtils.getConnection();
		backMapDocument = backManager.createDocument(IM.MAP_SNOMED_WINPATH_KINGS.getIri());
		backMapDocument.setCrud(IM.UPDATE);
		valueSetDocument= vsetManager.createDocument(IM.NAMESPACE);
		valueSetDocument.setCrud(IM.ADD);
		importR2Matches();
		importWinPathKings(config.folder);
		createBackMaps();
		addToUtlSet();
		TTDocumentFiler filer = new TTDocumentFilerJDBC();
		filer.fileDocument(document);
		filer = new TTDocumentFilerJDBC();
		filer.fileDocument(backMapDocument);
		if (valueSetDocument.getEntities()!=null) {
			filer = new TTDocumentFilerJDBC();
			filer.fileDocument(valueSetDocument);
		}
		return this;

	}

	private void addToUtlSet() throws SQLException {
		PreparedStatement getUtlSet= conn.prepareStatement("Select o.iri\n" +
			"from tpl\n" +
			"join entity e on tpl.subject= e.dbid\n" +
			"join entity p on tpl.predicate=p.dbid\n" +
			"join entity o on tpl.object= o.dbid\n" +
			"where e.iri='http://endhealth.info/im#VSET_UnifiedTestList' and p.iri='http://endhealth.info/im#hasMembers'");
		ResultSet rs= getUtlSet.executeQuery();
		while (rs.next()){
			String member= rs.getString("iri");
			utlSet.add(member);
		}
		for (String member:utlMembers){
			if (!utlSet.contains(member)){
				if (valueSetDocument.getEntities()==null){
					valueSetDocument.addEntity( new TTEntity().setIri(IM.NAMESPACE+"VSET_UnifiedTestList"));
				}
				valueSetDocument.getEntities().get(0).addObject(IM.HAS_MEMBER,TTIriRef.iri(member));
			}
		}
	}


	private void createBackMaps() {
		for (Map.Entry<String, List<String>> entry : snomedToWinpath.entrySet()) {
			String entityId = entry.getKey();
			TTEntity snomedEntity = new TTEntity()
				.setIri(SNOMED.NAMESPACE + entityId);
			backMapDocument.addEntity(snomedEntity);
			List<String> winpathList = entry.getValue();
			for (String winpath : winpathList) {
				TTManager.addSimpleMap(snomedEntity, IM.CODE_SCHEME_KINGS_WINPATH.getIri() + winpath);
			}
		}
	}


	private void importR2Matches() throws SQLException, ClassNotFoundException {
		System.out.println("Retrieving read vision 2 snomed map");

		PreparedStatement getR2Matches= conn.prepareStatement("select vis.code as code,snomed.code as snomed \n"+
			"from entity snomed \n" +
			"join tpl maps on maps.subject= snomed.dbid\n" +
			"join entity p on maps.predicate=p.dbid\n" +
			"join entity vis on maps.subject=vis.dbid\n" +
			"where snomed.iri like '"+ SNOMED.NAMESPACE+"%'\n"+
			"and p.iri='"+IM.MATCHED_TO+"'\n" +
			"and vis.iri like 'http://endhealth.info/VISION#'");
		ResultSet rs= getR2Matches.executeQuery();
		while (rs.next()){
			String snomed= rs.getString("snomed");
			String read= rs.getString("code");
			List<String> maps = readToSnomed.computeIfAbsent(read, k -> new ArrayList<>());
			maps.add(snomed);

		}
	}

	private void importWinPathKings(String folder) throws IOException {
		System.out.println("Importing kings code file");

		Path file = ImportUtils.findFileForId(folder, kingsWinPath[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				String readCode = fields[2];
				String code = fields[0] + "-" + (fields[1].toLowerCase());
				String iri = IM.CODE_SCHEME_KINGS_WINPATH.getIri() + fields[0].replace(" ", "") + "-" + (fields[1].replace(" ", ""));
				TTEntity entity = new TTEntity()
					.setIri(iri)
					.addType(OWL.CLASS)
					.setName(fields[1])
					.setDescription("Local winpath Kings trust pathology system entity ")
					.setCode(code);
				document.addEntity(entity);
				if (readToSnomed.get(readCode) != null) {
					for (String snomed : readToSnomed.get(readCode)) {
						List<String> maps = snomedToWinpath.computeIfAbsent(snomed, k -> new ArrayList<>());
						maps.add(snomed);
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

	@Override
	public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
		return null;
	}

	@Override
	public void close() throws Exception {

	}
}
