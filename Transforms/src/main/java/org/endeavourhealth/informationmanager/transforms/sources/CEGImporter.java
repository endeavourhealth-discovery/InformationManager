package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.iml.ConceptSet;
import org.endeavourhealth.imapi.model.iml.Entity;
import org.endeavourhealth.imapi.model.iml.ModelDocument;
import org.endeavourhealth.imapi.model.imq.QueryEntity;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;

import javax.xml.bind.JAXBContext;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;

public class CEGImporter implements TTImport {
	private TTEntity owner;
	private final Set<TTEntity> allEntities = new HashSet<>();


	private static final String[] queries = {".*\\\\CEGQuery"};
	private static final String[] annotations = {".*\\\\QueryAnnotations.properties"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
	private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
	public Set<ConceptSet> conceptSets= new HashSet<>();
	public Set<String> querySet= new HashSet<>();
	public TTIriRef valueSetFolder;

	@Override
	public void importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		TTDocument document= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		TTEntity graph= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri())
				.setName("CEG (QMUL) graph")
					.setDescription("CEG library of concept sets queries and profiles")
						.addType(IM.GRAPH);
			graph.addObject(RDFS.SUBCLASSOF,IM.GRAPH);
		document.addEntity(graph);
		createOrg(document);
		 CEGEthnicityImport ethnicImport= new CEGEthnicityImport();
		ethnicImport.importData(config);
		createFolders(document);

		if (TTFilerFactory.isTransactional()){
			new TTTransactionFiler(null).fileTransaction(document);
		}
		else {
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}
		}

		//Import queries
		loadAndConvert(config.getFolder());

		if (!conceptSets.isEmpty()) {
			document = new TTDocument(IM.GRAPH_CEG_QUERY);
			for (ConceptSet set : conceptSets) {
				if (!querySet.contains(set.getIri())) {
					TTEntity ttSet = new TTEntity()
						.setIri(set.getIri())
						.setName(set.getName());
					if (set.getUsedIn() != null) {
						for (TTIriRef used : set.getUsedIn())
							ttSet.addObject(IM.USED_IN, used);
					}
					ttSet.addObject(IM.IS_CONTAINED_IN, valueSetFolder);
					document.addEntity(ttSet);
				}
			}
			if (TTFilerFactory.isTransactional()) {
				new TTTransactionFiler(null).fileTransaction(document);
			}
			else {
				try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
					filer.fileDocument(document);
				}
			}
		}
	}





	private void createFolders(TTDocument document) {
		valueSetFolder= TTIriRef.iri(IM.GRAPH_CEG_QUERY.getIri()+"CSET_CEGConceptSets");
		TTEntity folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGQueries")
			.setName("QMUL CEG query library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_Queries"));
			folder.addObject(IM.CONTENT_TYPE,IM.QUERY);
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"CSET_CEGConceptSets")
			.setName("QMUL CEG concept set library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
		folder.addObject(IM.CONTENT_TYPE,IM.CONCEPT_SET);
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(IM.GRAPH_CEG_QUERY.getIri()+"Q_CEGFieldGroups")
			.setName("QMUL CEG Field group library")
			.addType(IM.FOLDER)
			.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_CEGQueries"));
		folder.addObject(IM.CONTENT_TYPE,IM.QUERY);;
		document.addEntity(folder);

	}

	private void createOrg(TTDocument document) {
		owner= new TTEntity()
			.setIri("http://org.endhealth.info/im#QMUL_CEG")
			.addType(TTIriRef.iri(IM.NAMESPACE+"Organisation"))
			.setName("Clinical Effectiveness Group of Queen Mary University of London - CEG")
			.setDescription("The Clinical effectiveness group being a special division of Queen Mary University of London," +
				"deliverying improvements in clinical outcomes for the population of UK");
		document.addEntity(owner);
	}

	public void loadAndConvert(String folder) throws Exception {
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
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext= FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
                    System.out.println("..." + fileEntry.getName());
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToIMQ converter= new EqdToIMQ();
					ModelDocument qDocument= converter.convertEQD(eqd,dataMap,
						labels);
					TTDocument document= new TTDocument().setGraph(IM.GRAPH_CEG_QUERY);
					if (qDocument.getFolder()!=null){
						for (Entity qFolder:qDocument.getFolder()){
							TTEntity ttFolder= new TTEntity()
								.setIri(qFolder.getIri())
								.setName(qFolder.getName())
								.setName(qFolder.getName());
							qFolder.getEntityType().forEach(ttFolder::addType);
								ttFolder
								.setDescription(qFolder.getDescription());
							for( TTIriRef type: qFolder.getEntityType()) {
								ttFolder.addType(type);
							}
							document.addEntity(ttFolder);
							if (qFolder.getIsContainedIn() != null) {
								for (TTIriRef inFolder : qFolder.getIsContainedIn())
									ttFolder.addObject(IM.IS_CONTAINED_IN, inFolder);
							}
							else
								ttFolder.addObject(IM.IS_CONTAINED_IN,mainFolder);
						}
					}
					if (qDocument.getQuery()!=null){
						for (QueryEntity qq:qDocument.getQuery()){
							querySet.add(qq.getIri());
							TTEntity ttQuery= new TTEntity()
								.setIri(qq.getIri())
								.setName(qq.getName())
								.setDescription(qq.getDescription());
							qq.getEntityType().stream().forEach(ttQuery::addType);
							if (qq.getEntityType().contains(IM.COHORT_QUERY))
								ttQuery.set(IM.RETURN_TYPE,TTIriRef.iri(IM.NAMESPACE+"Patient"));
							document.addEntity(ttQuery);
							if (qq.getIsContainedIn()!=null){
								for (TTIriRef inFolder:qq.getIsContainedIn()){
									ttQuery.addObject(IM.IS_CONTAINED_IN,inFolder);
								}
							}
							ttQuery.set(IM.DEFINITION,TTLiteral.literal(qq.getDefinition()));
						}
					}
				  output(fileEntry,qDocument,document);
					if (TTFilerFactory.isTransactional()){
						new TTTransactionFiler(null).fileTransaction(document);
					}
					else {
						try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
							filer.fileDocument(document);
						}
					}
					this.conceptSets.addAll(converter.getValueSets().values());
					}
				}
			}
	}

	private void output(File fileEntry,ModelDocument qDocument, TTDocument document) throws IOException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {

		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
			objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
			String json= objectMapper.writerWithDefaultPrettyPrinter().withAttribute(TTContext.OUTPUT_CONTEXT, true).writeValueAsString(qDocument);
		  json= json.replaceAll(IM.NAMESPACE,":");
			try (FileWriter wr= new FileWriter(directory + fileEntry.getName().replace(".xml","") + ".json")){
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
        conceptSets.clear();
    }
}
