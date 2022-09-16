package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.iml.Query;
import org.endeavourhealth.imapi.model.iml.QueryDocument;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.endeavourhealth.informationmanager.transforms.sources.eqd.EnquiryDocument;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

public class CEGImporter implements TTImport {
	private TTDocument document;
	private TTEntity owner;
	private final Set<TTEntity> allEntities = new HashSet<>();


	private static final String[] queries = {".*\\\\CEGQuery"};
	private static final String[] annotations = {".*\\\\QueryAnnotations.properties"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
	private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
	public static final Map<TTIriRef,TTEntity> valueSets= new HashMap<>();
	@Override
	public void importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		document= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		TTEntity graph= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri())
				.setName("CEG (QMUL) graph")
					.setDescription("CEG library of concept sets queries and profiles")
						.addType(IM.GRAPH);
			graph.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
		document.addEntity(graph);
		createOrg();
		 CEGEthnicityImport ethnicImport= new CEGEthnicityImport();
		ethnicImport.importData(config);
		createFolders();

		//Import queries
		loadAndConvert(config.getFolder());
		WrapAsJson();
		Map<String,TTEntity> vsetFolderMap= new HashMap<>();
		Set<TTEntity> vsetFolders= new HashSet<>();
		for (TTEntity entity:document.getEntities()){
			//does not go in
			if (entity.isType(IM.CONCEPT_SET)) {
				for (TTValue usedIn:entity.get(IM.USED_IN).getElements()){
					TTEntity report =manager.getEntity(usedIn.asIriRef().getIri());
					TTEntity vsetFolder= vsetFolderMap.get(usedIn.asIriRef().getIri());
					if (vsetFolder==null) {
						String reportName = report.getName();
						String vsetFolderIri = IM.GRAPH_CEG_QUERY.getIri() + "FOLDER_" + report.getIri();
						vsetFolder = new TTEntity()
							.setIri(vsetFolderIri)
							.addType(IM.FOLDER)
							.setName("Value sets used in " + reportName);
						vsetFolder.addObject(IM.IS_CONTAINED_IN, TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri() + "CSET_CEGConceptSets"));
						vsetFolders.add(vsetFolder);
					}
					entity.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(vsetFolder.getIri()));
				}

			}
		}
		for (TTEntity folder:vsetFolders){
			document.addEntity(folder);
		}
		if (TTFilerFactory.isTransactional()){
			new TTTransactionFiler(null).fileTransaction(document);
			return;
		}

		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(document);
		}
	}




	private void WrapAsJson() throws JsonProcessingException {
		for (TTEntity entity:document.getEntities()){
			if (entity.isType(IM.QUERY))
				if (entity.get(IM.QUERY_DEFINITION)!=null)
					TTManager.wrapRDFAsJson(entity);

		}
	}


	private void createFolders() {
		TTEntity folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries")
			.setName("QMUL CEG query library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_Queries"));
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"CSET_CEGConceptSets")
			.setName("QMUL CEG concept set library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGFieldGroups")
			.setName("QMUL CEG Field group library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_CEGQueries"));
		document.addEntity(folder);

	}

	private void createOrg() {
		owner= new TTEntity()
			.setIri("http://org.endhealth.info/im#QMUL_CEG")
			.addType(TTIriRef.iri(IM.NAMESPACE+"Organisation"))
			.setName("Clinical Effectiveness Group of Queen Mary University of London - CEG")
			.setDescription("The Clinical effectiveness group being a special division of Queen Mary University of London," +
				"deliverying improvements in clinical outcomes for the population of UK");
		document.addEntity(owner);
	}

	public void loadAndConvert(String folder) throws JAXBException, IOException, DataFormatException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
		Properties dataMap= new Properties();
		try (FileReader reader = new FileReader(( ImportUtils.findFileForId(folder, dataMapFile[0]).toFile()))) {
            dataMap.load(reader);
        }

        Properties labels= new Properties();
		try (FileReader reader = new FileReader(( ImportUtils.findFileForId(folder, annotations[0]).toFile()))) {
            labels.load(reader);
        }

		Path directory=  ImportUtils.findFileForId(folder,queries[0]);
		TTIriRef mainFolder= TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries");
		TTIriRef fieldGroupFolder= TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGFieldGroups");
		TTIriRef valueSetFolder= TTIriRef.iri(IM.NAMESPACE+"CSET_CEGConceptSets");
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext= FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
                    System.out.println("..." + fileEntry.getName());
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToIMQ converter= new EqdToIMQ();
					converter.convertDoc(document,mainFolder,fieldGroupFolder,valueSetFolder,eqd,
						TTIriRef.iri(owner.getIri()),dataMap,
						labels);

				  output(fileEntry);
				}
			}
		}

	}

	private void output(File fileEntry) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
		ObjectMapper om= new ObjectMapper();
		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			TTManager manager = new TTManager();
			QueryDocument hql = new QueryDocument();
			TTDocument qDocument = manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
			for (TTEntity entity : document.getEntities()) {
				qDocument.addEntity(entity);
				if (!allEntities.contains(entity)) {
					if (entity.isType(IM.QUERY)){
						String json= entity.get(IM.QUERY_DEFINITION).asLiteral().getValue();
						hql.addQuery(om.readValue(json, Query.class));
						allEntities.add(entity);
					}
				}
			}

			manager.setDocument(qDocument);
			manager.saveDocument(new File(directory + "\\"+ fileEntry.getName().replace(".xml", "") + "-new--LD.json"));
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json= objectMapper.writeValueAsString(hql);
			//try (FileWriter wr= new FileWriter(directory+"\\"+ fileEntry.getName().replace(".xml","") + ".json")){
			//	wr.write(json);
			//}
			try (FileWriter wr= new FileWriter(directory + fileEntry.getName().replace(".xml","") + "-NEW.json")){
				wr.write(json);
			}

		}

	}


	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		 ImportUtils.validateFiles(inFolder,queries,annotations,duplicates,lookups);
	}


    @Override
    public void close() throws Exception {
        allEntities.clear();
        valueSets.clear();
    }
}
