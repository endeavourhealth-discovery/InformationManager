package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
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
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class VisionImport implements TTImport {

	private static final String[] r2Terms = {".*\\\\READ\\\\Term.csv"};
	private static final String[] r2Desc = {".*\\\\READ\\\\DESC.csv"};
	private static final String[] r2Maps = {".*\\\\SNOMED\\\\Mapping Tables\\\\Updated\\\\Clinically Assured\\\\rcsctmap2_uk_.*\\.txt"};
	private static final String[] altMaps = {".*\\\\SNOMED\\\\Mapping Tables\\\\Updated\\\\Clinically Assured"+
		"\\\\codesWithValues_AlternateMaps_READ2_.*\\.txt"};
	private static final String[] visionRead2Code = {".*\\\\TPP_Vision_Maps\\\\vision_read2_code.csv"};
	private static final String[] visionRead2toSnomed = {".*\\\\TPP_Vision_Maps\\\\vision_read2_to_snomed_map.csv"};

	private final Map<String,TTEntity> codeToConcept= new HashMap<>();
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
		document= manager.createDocument(IM.GRAPH_VISION.getIri());
		mapDocument= manager.createDocument(IM.MAP_SNOMED_VISION.getIri());
		importR2Desc(inFolder);
		importR2Terms(inFolder);
		importVisionCodes(inFolder);
		createHierarchy();
		addVisionMaps(inFolder);
		TTDocumentFiler filer = new TTDocumentFilerJDBC();
		filer.fileDocument(document,bulkImport,entityMap);
		filer = new TTDocumentFilerJDBC();
		filer.fileDocument(mapDocument,bulkImport,entityMap);
		return this;
	}


	private void importR2Terms(String folder) throws IOException {

		Path file = ImportUtils.findFileForId(folder, r2Terms[0]);
		System.out.println("Importing official R2 terms as vision");

		try( CSVReader reader = new CSVReader(new FileReader(file.toFile()))){
			reader.readNext();
			int count=0;
			String[] fields;
			while ((fields = reader.readNext()) != null) {
				count++;
				if("C".equals(fields[1])) {
					String termid= fields[0];
					String term= fields[3];
					TTEntity readConcept= r2TermIdMap.get(termid);
					if (readConcept!=null){
						if (preferredId.contains(termid))
							readConcept.setName(term);
						else {
							TTManager.addTermCode(readConcept,term,null);
						}
					}
				}
			}
			System.out.println("Process ended with " + count +" read 2 terms");
		}
	}

	private void importR2Desc(String folder) throws IOException {

		Path file = ImportUtils.findFileForId(folder, r2Desc[0]);
		System.out.println("Importing R2 entities");

		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();

			int count = 0;
			while (line != null && !line.isEmpty()) {

				String[] fields = line.split(",");
				if ("C".equals(fields[6])) {
					String code= fields[0];
					if (!code.startsWith(".")){
						if (!Character.isLowerCase(code.charAt(0))) {
						TTEntity readConcept = codeToConcept.get(code);
						if (readConcept == null) {
							readConcept = new TTEntity()
								.setIri(IM.CODE_SCHEME_VISION.getIri() + code.replace(".",""))
								.setCode(code)
								.addType(OWL.CLASS);
							document.addEntity(readConcept);
							codeToConcept.put(code, readConcept);
						}
						String termId = fields[1];
						String preferred = fields[2];
						if (preferred.equals("P"))
							preferredId.add(termId);
						r2TermIdMap.put(termId, readConcept);
						}
						count++;
						if (count % 50000 == 0) {
							System.out.println("Processed " + count + " read code termid links");
						}
					}
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count + " read code term id links");
		}
	}



	private void createHierarchy() {
		for (TTEntity entity:document.getEntities()){
			String code= entity.getCode();
			String shortCode= code;
			if (shortCode.contains("."))
				shortCode= shortCode.substring(0,shortCode.indexOf("."));
			if (shortCode.length()==1)
				entity.set(IM.IS_CHILD_OF,new TTArray().add(iri(IM.CODE_SCHEME_VISION.getIri()+"VisionCodes")));
			else {
				String parent = shortCode.substring(0,shortCode.length()-1);
				entity.set(IM.IS_CHILD_OF, new TTArray().add(iri(IM.CODE_SCHEME_VISION.getIri() + parent)));
			}
		}
	}


	private void importVisionCodes(String folder) throws IOException {
		Path file = ImportUtils.findFileForId(folder, visionRead2Code[0]);
		System.out.println("Retrieving terms from vision read+lookup2");
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = readQuotedCSVLine(reader, line);
				count++;
				if (count % 10000 == 0) {
					System.out.println("Processed " + count + " terms");
				}
				String code = fields[0];
				String term = fields[1];
				code = code.replaceAll("\"", "");
				term = term.substring(1, term.length() - 1);
				if (!code.startsWith(".")) {
					if (!Character.isLowerCase(code.charAt(0))) {
						if (codeToConcept.get(code) == null) {
							TTEntity c = new TTEntity();
							c.setIri(IM.CODE_SCHEME_VISION.getIri() + code.replace(".", ""));
							c.setName(term);
							c.setCode(code);
							document.addEntity(c);
							codeToConcept.put(code, c);
						}
					}
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count + " additional Vision read like codes created");
		}
	}

	public String[] readQuotedCSVLine(BufferedReader reader, String line) throws IOException {
		if (line.split(",").length < 5) {
			do {
				String nextLine = reader.readLine();
				line = line.concat("\n").concat(nextLine);
			} while (line.split(",").length < 5);
		}
		String[] fields = line.split(",");
		if (fields.length > 5) {
			for (int i = 2; i < fields.length - 3; i++) {
				fields[1] = fields[1].concat(",").concat(fields[i]);
			}
		}
		return fields;
	}


	private void addVisionMaps(String folder) throws IOException {
		Path file = ImportUtils.findFileForId(folder, visionRead2toSnomed[0]);
		System.out.println("Retrieving Vision snomed maps");
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split(",");
				count++;
				if (count % 10000 == 0) {
					System.out.println("Processed " + count);
				}
				String code= fields[0];
				String snomed= fields[1];
				if (isSnomed(snomed)) {
					TTEntity snomedConcept= new TTEntity().setIri("sn:"+snomed);
					snomedConcept.setCrud(IM.ADD);
					mapDocument.addEntity(snomedConcept);
					if (codeToConcept.get(code)!=null) {
						TTManager.addSimpleMap(snomedConcept,IM.CODE_SCHEME_VISION.getIri()+code.replace(".",""));
					}
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count);
		}
	}

	public Boolean isSnomed(String s){
		return snomedCodes.contains(s);
	}

	@Override
	public TTImport validateFiles(String inFolder) {
		ImportUtils.validateFiles(inFolder,r2Terms,r2Desc,visionRead2Code,visionRead2toSnomed);
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
