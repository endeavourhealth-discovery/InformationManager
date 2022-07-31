package org.endeavourhealth.informationmanager.transforms.sources;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;


public class PRSBImport implements TTImport {

	private static final String[] prsbEntities = {".*\\\\PRSB\\\\RetrieveInstance.json"};
	private TTDocument document;
	private Map<String, TTArray> axiomMap;

	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		validateFiles(config.getFolder());
		TTManager dmanager= new TTManager();
		document= dmanager.createDocument(IM.GRAPH_PRSB.getIri());
		document.addEntity(dmanager.createGraph(IM.GRAPH_PRSB.getIri(),"PRSB code scheme and graph"
		,"The professional records standards board code scheme and graph"));
		importEntityFiles(config.getFolder());
		//TTDocumentFiler filer = new TTDocumentFilerJDBC(document.getGraph());
		dmanager.saveDocument(new File(config.getFolder() + "prsb.json"));
		//filer.fileDocument(document);
		return this;
	}

	private void initializeMaps() {
        axiomMap = new HashMap<>();
        axiomMap.put("prsb03-dataelement-10868", getAxioms(IM.NAMESPACE + "Patient"));

    }

	private TTArray getAxioms(String prsb) {
		return null;
	}

	private void importEntityFiles(String path) throws IOException {
		int i = 0;
		for (String prsbFile : prsbEntities) {
			Path file = ImportUtils.findFilesForId(path, prsbFile).get(0);
			System.out.println("Processing entities in " + file.getFileName().toString());
			JSONParser jsonParser = new JSONParser();
			try (FileReader reader = new FileReader(file.toFile()))
			{
				//Read JSON file
				Object obj = jsonParser.parse(reader);

				JSONArray prsbModel = (JSONArray) ((JSONObject) obj).get("dataset");

				//Iterate over prsbModel
				prsbModel.forEach( mod -> {
					try {
						parsePRSBModel( (JSONObject) mod );
					} catch (DataFormatException e) {
						e.printStackTrace();
					}
				});

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		System.out.println("Imported " + i + " entities");
	}

	private void parsePRSBModel(JSONObject dataModel) throws DataFormatException {
		TTEntity dm= newEntity(dataModel,SHACL.NODESHAPE);
		dm.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"DiscoveryOntology"));
		JSONArray recordTypes= (JSONArray) dataModel.entrySet();
		dataModel.entrySet().forEach(c ->{
			try {
				parseRecordType((JSONObject) c);
			} catch (DataFormatException e) {
				e.printStackTrace();
			}
		});
	}

	private void parseRecordType(JSONObject c) throws DataFormatException {
		String prsbId= (String) c.get("iddisplay");
		TTEntity rt= newEntity(c,SHACL.NODESHAPE);
		TTArray axioms= axiomMap.get(prsbId);

	}

	private TTIriRef mapStatus(String status) throws DataFormatException {
		if (status.equals("draft"))
			return IM.DRAFT;
			else
				throw new DataFormatException("unknown status type - "+ status);

	}

	private TTEntity newEntity(JSONObject c, TTIriRef... types) throws DataFormatException {
		TTEntity entity= new TTEntity();
		entity.set(IM.HAS_STATUS,mapStatus(c.get("statusCode").toString()));
		Arrays.stream(types).forEach( type-> entity.addType(type));
		String prsbId= c.get("iddisplay").toString();
		entity.setCode(prsbId);
		String name= getObjectArrayliteral(c,"name","#text");
		String iri= (PRSB.NAMESPACE + prsbId);

		entity.setName(name);
		if (c.get("shortName")!=null) {
			String shortName = (String) c.get("shortName");
			entity.set(TTIriRef.iri(IM.NAMESPACE + "shortName"), TTLiteral.literal((shortName)));
		}
		entity.setIri(iri);
		String description= getObjectArrayliteral(c,"desc","#text");
		if (description!=null)
			entity.setDescription(description);
		String background= getObjectArrayliteral(c,"context","#text");
		if (background!=null)
			entity.set(TTIriRef.iri(IM.NAMESPACE+"backgroundContext"),TTLiteral.literal(background));
		if (entity.isType(SHACL.NODESHAPE))
			return entity;
		return entity;

	}

	private String getObjectArrayliteral(JSONObject ob,String name,String subname){
		JSONArray arr= (JSONArray) ob.get(name);
		JSONObject subob = (JSONObject) arr.get(0);
		if (subob.get(subname)!=null)
			return (String) subob.get(subname);
		else
			return null;
	}

	@Override
	public TTImport validateFiles(String inFolder) {
		ImportUtils.validateFiles(inFolder,prsbEntities);
		return this;
	}




}
