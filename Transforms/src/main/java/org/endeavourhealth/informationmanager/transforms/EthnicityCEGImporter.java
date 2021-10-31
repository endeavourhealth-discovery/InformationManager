package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class EthnicityCEGImporter implements TTImport {

	private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
	private final TTManager manager = new TTManager();
	private TTDocument document;
	private final Map<String,String> ethnicMap= new HashMap<>();
	private final Map<String,String> ethnicCensusMap= new HashMap<>();
	private final Map<String,String> raceMap = new HashMap<>();
	private final Map<String,TTEntity> nhsCatmap= new HashMap<>();
	private final Map<String,TTEntity> cegCatMap= new HashMap<>();
	private final Map<String,String> spellMaps= new HashMap<>();
	private final Set<String> dropWords= new HashSet<>();
	private TTEntity nhsSet;
	private TTEntity cegSet;

	private Connection conn;
	
	private PreparedStatement get2001Census;
	
	public EthnicityCEGImporter() throws SQLException, ClassNotFoundException {
		conn= ImportUtils.getConnection();
		get2001Census= conn.prepareStatement("select tc.term,child.iri,child.code,child.name from tct\n" +
			"join entity parent on tct.ancestor= parent.dbid \n" +
			"join entity child on tct.descendant= child.dbid\n" +
			"left join term_code tc on tc.entity= child.dbid\n" +
			"where parent.iri='http://snomed.info/sct#92381000000106'\n"+
			"and child.status='"+IM.ACTIVE.getIri()+"'");


	}

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		document = manager.createDocument(IM.GRAPH_CEG16.getIri());
		document.setCrud(IM.UPDATE);
		retrieveEthnicity(config.folder, config.secure);
		spellCorrections();
		importEthnicGroups(config.folder);

		TTDocumentFiler filer = TTFilerFactory.getDocumentFiler();
		filer.fileDocument(document);

		return this;
	}

	private void spellCorrections() {
		spellMaps.put("british","british or mixed british");
		spellMaps.put("other black african or caribbean background",
			"other black background - ethnic category 2001 census (finding)");
		spellMaps.put("not stated","ethnic category not stated");
		dropWords.add("any");
		dropWords.add("or multiple ethnic");
		dropWords.add("ethnic group");
	}

	private void retrieveEthnicity(String folder, boolean secure) throws SQLException, IOException, ClassNotFoundException {
		ResultSet rs= get2001Census.executeQuery();
		if (!rs.next()){
			System.out.println("Building tct as this is required");
			ClosureGenerator.generateClosure(folder, secure);
		}

		rs= get2001Census.executeQuery();
		while (rs.next()){
			String term=rs.getString("term").toLowerCase();
			String snomed=rs.getString("code");
			term=term.replace(","," ");
			term.replace("  "," ");
			if (spellMaps.get(term)!=null)
				term=spellMaps.get(term);
			ethnicCensusMap.put(term,snomed);
			if (term.contains("(")) {
				term = term.substring(0, term.indexOf("("));
				term = term.substring(0, term.lastIndexOf(" "));
			}
			if (term.contains(": ")) {
				term = term.split(": ")[1];
				ethnicCensusMap.put(term,snomed);
			}
			if (term.contains(" - ")) {
				term = term.split(" - ")[0];
				ethnicCensusMap.put(term, snomed);
			}
		}

	}

	private String matchEthnicity(String oterm){
		String term=oterm.toLowerCase().replace("\"","");
		term=term.replace(","," ");
		term=term.replace("  "," ");
		if (term.equals("unclassified"))
			return "unclassified";
		if (spellMaps.get(term)!=null)
			term=spellMaps.get(term);

		if (ethnicMap.get(term)!=null)
			return ethnicMap.get(term);
		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);

		String sterm=dropSomeWords(term);
		if (ethnicCensusMap.get(sterm)!=null)
			return ethnicCensusMap.get(sterm);
		term=term+" - ethnic category 2001 census (finding)";
		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);
		term=dropSomeWords(term);
		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);
		return null;
	}

	private String dropSomeWords(String term) {
		for (String word:dropWords) {
			if ((" " + term + " ").contains((" " + word + " ")))
				term = term.replace(word + " ", "");
		}
		return term;
	}


	private void importEthnicGroups(String folder) throws IOException {

		Path file = ImportUtils.findFileForId(folder, lookups[0]);
		System.out.println("Importing Categories");
		setConceptSetGroups();

		try( BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))){
			reader.readLine();
			int count=0;
			String[] fields;
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				count++;
				if(count%50000 == 0){
					System.out.println("Processed " + count +" terms");
				}
				fields= line.split("\t");
				String snomed=fields[1];
				String cat16=fields[11];
				String catTerm=fields[12].replace("\"","");
				String nhs16=fields[19];
				String nhsTerm= fields[20].replace("\"","");
				String pdsTerm=fields[22];
				String snoNhs= matchEthnicity(nhsTerm);
				if (snoNhs==null)
					snoNhs= matchEthnicity(pdsTerm);
				TTEntity cegSubset= cegCatMap.get(cat16);
				if (cegSubset==null){
					cegSubset= new TTEntity()
						.setIri(IM.NAMESPACE+"CSET_EthnicCategoryCEG16_"+cat16)
						.addType(IM.CONCEPT_SET)
						.setName("Concept set - "+ catTerm)
						.setCode(cat16)
						.setDescription("QMUL CEG 16+ Ethnic category "+cat16)
						.set(IM.DEFINITION,new TTNode().set(SHACL.OR, new TTArray()));
					cegSubset.addObject(IM.MEMBER_OF_GROUP,TTIriRef.iri(cegSet.getIri()));
					document.addEntity(cegSubset);
					cegCatMap.put(cat16,cegSubset);
				}
				cegSubset.get(IM.DEFINITION).asNode().get(SHACL.OR).asArray().add(TTIriRef.iri(SNOMED.NAMESPACE+snomed));
				if (cegSubset.get(IM.HAS_TERM_CODE)==null)
					TTManager.addTermCode(cegSubset,catTerm,null);
				if (!snoNhs.equals("unclassified")){
					TTEntity nhsSubset= nhsCatmap.get(snoNhs);
					if (nhsSubset==null) {
						nhsSubset = new TTEntity()
						.setIri(IM.NAMESPACE + "CSET_SN_"+snoNhs)
						.addType(IM.CONCEPT_SET)
							.setName("Concept set - "+ nhsTerm+" (2001 census ethnic category "+nhs16+")")
						.setDescription("NHS Data Dictionary 2001 ethnic category " + nhs16)
							.set(IM.DEFINITION,new TTNode().set(SHACL.OR,new TTArray()));
						nhsSubset.addObject(IM.MEMBER_OF_GROUP,TTIriRef.iri(nhsSet.getIri()));
						document.addEntity(nhsSubset);
						nhsCatmap.put(snoNhs, nhsSubset);
					}
					if (nhsSubset.get(IM.HAS_TERM_CODE)==null)
						TTManager.addTermCode(nhsSubset,nhsTerm,null);
					nhsSubset.get(IM.DEFINITION).asNode().get(SHACL.OR).asArray().add(TTIriRef.iri(SNOMED.NAMESPACE+snomed));
				}

				line=reader.readLine();

			}
			System.out.println("Process ended with " + count +" terms");
		}
	}

	private void setConceptSetGroups() {
		cegSet= new TTEntity()
			.setIri(IM.NAMESPACE+"CSET_EthnicCategoryCEG16")
			.addType(IM.SET_GROUP)
			.setName("CEG 16+1 Ethnic category (set group)")
			.setDescription("QMUL-CEG categorisations of ethnic groups");
		cegSet.set(IM.IS_CONTAINED_IN, new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"EthnicitySets")));
		document.addEntity(cegSet);
		nhsSet= new TTEntity()
			.setIri(IM.NAMESPACE+"CSET_EthnicCategory2001")
			.addType(IM.SET_GROUP)
			.setName("Concept set - 2001 census Ethnic category (set group")
			.setDescription("NHS Data Dictionary 2001 census based categorisations of ethnic groups");
		nhsSet.set(IM.IS_CONTAINED_IN, new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"EthnicitySets")));
		document.addEntity(nhsSet);
	}


	@Override
	public TTImport validateFiles(String inFolder) {
		ImportUtils.validateFiles(inFolder,lookups);
		return this;
	}

	@Override
	public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {

		return this;
	}

	@Override
	public void close() throws Exception {
		if (conn!=null)
			if (!conn.isClosed())
				conn.close();


	}
}



