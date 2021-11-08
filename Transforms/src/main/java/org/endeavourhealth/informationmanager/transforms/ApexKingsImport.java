package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
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

public class ApexKingsImport implements TTImport {


	private static final String[] kingsPath = {".*\\\\Kings\\\\KingsPathMap.txt"};
	private TTDocument backMapDocument;
	private TTDocument document;
	private TTDocument valueSetDocument;
	private final Map<String, List<String>> readToSnomed = new HashMap<>();
	private final Map<String, List<String>> snomedToRead = new HashMap<>();
	private final Map<String, List<String>> snomedToApex = new HashMap<>();
	private final Map<String, String> apexToRead = new HashMap<>();
	private static final TTIriRef utl= TTIriRef.iri(IM.NAMESPACE+"VSET_UnifiedTestList");
	private static final Set<String> utlSet= new HashSet<>();
	private static final Set<String> utlMembers= new HashSet<>();
	private Connection conn;

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
        //
        conn = ImportUtils.getConnection();
        TTManager manager = new TTManager();
        document = manager.createDocument(IM.GRAPH_KINGS_APEX.getIri());
        TTManager backManager = new TTManager();
        TTManager vsetManager = new TTManager();
        backMapDocument = backManager.createDocument(IM.MAP_SNOMED_APEX_KINGS.getIri());
        backMapDocument.setCrud(IM.UPDATE);
        valueSetDocument = vsetManager.createDocument(IM.NAMESPACE);
        valueSetDocument.setCrud(IM.ADD);
        importR2Matches();
        setTopLevel();
        importApexKings(config.folder);
        createBackMaps();
        addToUtlSet();
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(backMapDocument);
        }
        if (valueSetDocument.getEntities() != null) {
            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(valueSetDocument);
            }
        }
        return this;
    }

	private void setTopLevel() {
		TTEntity kings= new TTEntity()
			.setIri(IM.NAMESPACE+"KingsApexCodes")
			.addType(IM.CONCEPT)
			.setName("Kings College Hospital Apex path codes")
			.setDescription("Local codes for the Apex pathology system in kings")
			.set(IM.IS_CONTAINED_IN,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies")));
			document.addEntity(kings);
	}

	private void addToUtlSet() throws SQLException {
		try (PreparedStatement getUtlSet= conn.prepareStatement("Select o.iri\n" +
			"from tpl\n" +
			"join entity e on tpl.subject= e.dbid\n" +
			"join entity p on tpl.predicate=p.dbid\n" +
			"join entity o on tpl.object= o.dbid\n" +
			"where e.iri='http://endhealth.info/im#VSET_UnifiedTestList' and p.iri='http://endhealth.info/im#hasMembers'")) {
            ResultSet rs = getUtlSet.executeQuery();
            while (rs.next()) {
                String member = rs.getString("iri");
                utlSet.add(member);
            }
            for (String member : utlMembers) {
                if (!utlSet.contains(member)) {
                    if (valueSetDocument.getEntities() == null) {
                        valueSetDocument.addEntity(new TTEntity().setIri(IM.NAMESPACE + "VSET_UnifiedTestList"));
                        valueSetDocument.getEntities().get(0).
                            set(IM.DEFINITION, new TTNode()
                                .set(SHACL.OR, new TTArray()));
                    }
                    valueSetDocument.getEntities().get(0).get(IM.DEFINITION).asNode()
                        .get(SHACL.OR).asArray().add(TTIriRef.iri(member));
                }
            }
        }
	}



	private void createBackMaps() {
		for (Map.Entry<String,List<String>> entry : snomedToApex.entrySet()) {
			String snomed = entry.getKey();
			TTEntity snomedEntity = new TTEntity()
				.setIri(SNOMED.NAMESPACE + snomed);
			backMapDocument.addEntity(snomedEntity);
			List<String> apexList = entry.getValue();
			for (String apex : apexList) {
				TTManager.addSimpleMap(snomedEntity,IM.CODE_SCHEME_KINGS_APEX.getIri()+apex);
			}
		}
	}


	private void importR2Matches() throws SQLException {
		System.out.println("Retrieving read vision 2 snomed map");

		try (PreparedStatement getR2Matches= conn.prepareStatement("select vis.code as code,snomed.code as snomed \n"+
				"from entity snomed \n" +
			"join tpl maps on maps.subject= snomed.dbid\n" +
			"join entity p on maps.predicate=p.dbid\n" +
			"join entity vis on maps.subject=vis.dbid\n" +
			"where snomed.iri like '"+ SNOMED.NAMESPACE+"%'\n"+
			"and p.iri='"+IM.MATCHED_TO+"'\n" +
			"and vis.iri like 'http://endhealth.info/VISION#'")) {
            ResultSet rs = getR2Matches.executeQuery();
            while (rs.next()) {
                String snomed = rs.getString("snomed");
                String read = rs.getString("code");
                List<String> maps = readToSnomed.computeIfAbsent(read, k -> new ArrayList<>());
                maps.add(snomed);
                maps = snomedToRead.computeIfAbsent(snomed, k -> new ArrayList<>());
                maps.add(read);

            }
        }
	}

	private void importApexKings(String folder) throws IOException {
		System.out.println("Importing kings code file");

		Path file = ImportUtils.findFileForId(folder, kingsPath[0]);
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				String readCode= fields[0];
				String code= fields[1]+"-"+(fields[2].toLowerCase());
				String iri = IM.CODE_SCHEME_KINGS_APEX.getIri()+ fields[1]+ "-"+(fields[2].replace(" ",""));
				TTEntity entity= new TTEntity()
					.setIri(iri)
					.addType(IM.CONCEPT)
					.setName(fields[2])
					.setDescription("Local apex Kings trust pathology system entity ")
					.setCode(code)
					.set(IM.IS_CHILD_OF,new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"KingsApexCodes")));
				document.addEntity(entity);
				apexToRead.put(code,readCode);
				if (readToSnomed.get(readCode)!=null){
					for (String snomed:readToSnomed.get(readCode)){
						List<String> maps = snomedToApex.computeIfAbsent(snomed, k -> new ArrayList<>());
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
		ImportUtils.validateFiles(inFolder,kingsPath);
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
