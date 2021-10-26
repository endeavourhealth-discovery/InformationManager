package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.ECLToTT;
import org.endeavourhealth.imapi.transforms.SCGToTT;
import org.endeavourhealth.imapi.transforms.SnomedConcept;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class BartsCernerImport implements TTImport {

	private static final String[] used = {".*\\\\Barts\\\\Barts-Cerner-Codes.txt"};
	//private static final String[] codes = {".*\\\\Barts\\\\V500_event_code.txt"};
	//private static final String[] sets = {".*\\\\Barts\\\\V500_Event_Set_Code.txt"};
	//private static final String[] hierarchy = {".*\\\\Barts\\\\V500_event_set_canon.txt"};
	private static final String[] maps = {".*\\\\Barts\\\\Snomed-Barts-Cerner.txt"};


	private final Map<String, TTEntity> codeToConcept= new HashMap<>();
	private final Map<String,String> setToCode = new HashMap<>();
	private final Map<String,String> setTermToCode= new HashMap<>();
	private TTDocument document;

	private Connection conn;
	private final TTManager manager= new TTManager();
	private TTDocument mapDocument;

	private final Map<String,TTEntity> entityMap = new HashMap<>();


	private class Snomed {
		String entityId;
		String descId;
	}
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {

		/*
		conn= ImportUtils.getConnection();
		System.out.println("retrieving snomed codes from IM");
		document= manager.createDocument(IM.GRAPH_BARTS_CERNER.getIri());
		document.setCrud(IM.REPLACE);
		mapDocument= manager.createDocument(IM.MAP_SNOMED_BC.getIri());
		document.setCrud(IM.UPDATE);
		setTopLevel();
		//importCodes();
		//importSets();
		importUsed(config.folder);
		importHierarchy();
		TTDocumentFiler filer = new TTDocumentFilerJDBC();
		filer.fileDocument(document);
		importMaps(config.folder);
		filer = new TTDocumentFilerJDBC();
		filer.fileDocument(mapDocument);

		 */
		return this;
	}

	private void importMaps(String inFolder) throws IOException, DataFormatException {
		int count = 0;
		Integer incremental= 100020;
		Map<String, TTEntity> snomedToConcept = new HashMap<>();
		for (String conceptFile : maps) {
			Path file = ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			System.out.println("Processing  Snomed maps " + file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields = line.split("\t");
					String code = fields[1];
					String snomed = fields[4];
					String ecl= fields[5];
					String term=fields[2];
					TTEntity snomedConcept = snomedToConcept.get(snomed);
					if (snomedConcept == null) {
							snomedConcept = new TTEntity()
								.setIri(SNOMED.NAMESPACE + snomed)
								.addType(IM.CONCEPT)
								.setCrud(IM.UPDATE)
								.setName(term)
								.setDescription("Concept expression");
							mapDocument.addEntity(snomedConcept);
					}
					snomedConcept.addObject(IM.MATCHED_TO, iri(IM.CODE_SCHEME_BARTS_CERNER + code));
					line = reader.readLine();
					}
				}
			}
		System.out.println("Imported " + count + " maps");
	}


	private void setTopLevel() {
		TTEntity topConcept= new TTEntity()
			.setCrud(IM.REPLACE)
			.setIri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"BartsCernerCodes")
			.addType(IM.CONCEPT)
			.setName("Barts Cerner codes")
			.setDescription("The Cerner codes used in Barts NHS Trust Millennium system");
		topConcept.addObject(IM.IS_CHILD_OF,iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
		document.addEntity(topConcept);
		TTEntity unmatchedConcept= new TTEntity()
			.setCrud(IM.REPLACE)
			.setIri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"UnclassifiedBartsCernerCodes")
			.addType(IM.CONCEPT)
			.setName("Unclassified Barts Cerner codes")
			.setDescription("The Cerner codes used in Barts NHS Trust Millennium system"
				+"that have not yet been placed in the Barts event set hierarchy");
		unmatchedConcept.addObject(IM.IS_CHILD_OF,iri(IM.NAMESPACE+"BartsCernerCodes"));
		document.addEntity(unmatchedConcept);
	}


	private void importHierarchy() throws IOException {
		/*
		Map<String,String> childToParent= new HashMap<>();
		int count = 0;
		for (String conceptFile : hierarchy) {
			Path file = ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			System.out.println("Processing  cerner event set V500 canon in " + file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields= line.split("\t");
					String parent= fields[0];
					String child= fields[2];
					if (setToCode.get(child)!=null)
						child= setToCode.get(child);
					childToParent.put(child,parent);
					line = reader.readLine();
				}
			}
		}
		for (Map.Entry<String,String> entry:childToParent.entrySet()){
			if (childToParent.get(entry.getValue())==null){
				TTEntity setConcept= codeToConcept.get(entry.getValue());
				setConcept.addObject(IM.IS_CHILD_OF,iri(IM.CODE_SCHEME_BARTS_CERNER+"UnclassifiedBartsCernerCodes"));
			} else {
				TTEntity setConcept= codeToConcept.get(entry.getKey());
				setConcept.addObject(IM.IS_CHILD_OF,iri(IM.CODE_SCHEME_BARTS_CERNER+"BC_"+entry.getValue()));
			}
		}
		System.out.println("Imported " + count + " hierarchy links");

		 */
	}

	private void importUsed(String inFolder) throws IOException {
		int count = 0;
		for (String conceptFile : used) {
			Path file = ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			System.out.println("Processing  cerner event codes in " + file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields= line.split("\t");
					String code= fields[7];
					String term= fields[4].replace("\"","");
					TTEntity usedConcept= codeToConcept.get(code);
					if (usedConcept==null) {
						usedConcept = new TTEntity()
							.setIri(IM.CODE_SCHEME_BARTS_CERNER.getIri() + "BC_" + code)
							.addType(IM.CONCEPT)
							.setCode(code)
							.setScheme(IM.CODE_SCHEME_BARTS_CERNER);
						document.addEntity(usedConcept);
						codeToConcept.put(code,usedConcept);
					}
					usedConcept.setName(term);
					line = reader.readLine();
				}
			}
		}
		System.out.println("Imported " + count + " codes from look up");

	}

	private void importSets(String inFolder) throws IOException {
		/*
		int count = 0;
		for (String conceptFile : sets) {
			Path file = ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			System.out.println("Processing  cerner event set codes in " + file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields= line.split("\t");
					String code= fields[7];
					String term= fields[3];
					String xterm= term.toLowerCase();
					if (setTermToCode.get(xterm)!=null){
						setToCode.put(code,setTermToCode.get(xterm));
					} else {
						TTEntity eventSet = new TTEntity()
							.setIri(IM.CODE_SCHEME_BARTS_CERNER.getIri() + "BC_" + code)
							.addType(IM.CONCEPT)
							.setName(term)
							.setCode(code)
							.setScheme(IM.CODE_SCHEME_BARTS_CERNER);
						document.addEntity(eventSet);
						codeToConcept.put(code, eventSet);
					}
					line = reader.readLine();
				}
			}
		}
		System.out.println("Imported " + count + " sets");

		 */
	}

	private void importCodes(String inFolder) throws IOException {
		/*
		int count = 0;
		for (String conceptFile : codes) {
			Path file = ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			System.out.println("Processing  cerner event codes in " + file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields= line.split("\t");
					String code= fields[0];
					String term= fields[3].replace("\"","");
					String xterm= term.toLowerCase();
					String setTerm = fields[15].toLowerCase().replace("\"","");
					if (setTerm.equals(xterm))
						setTermToCode.put(setTerm,code);
					TTEntity codeConcept= new TTEntity()
						.setIri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"BC_"+code)
						.addType(IM.CONCEPT)
						.setName(term)
						.setCode(code)
						.setScheme(IM.CODE_SCHEME_BARTS_CERNER);
					document.addEntity(codeConcept);
					codeToConcept.put(code,codeConcept);
					line = reader.readLine();
				}
			}
		}
		System.out.println("Imported " + count + " codes from look up");

		 */
	}



	@Override
	public TTImport validateFiles(String inFolder) {

		ImportUtils.validateFiles(inFolder, maps,used);
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
