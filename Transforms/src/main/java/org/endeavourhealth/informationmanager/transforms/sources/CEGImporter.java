package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.Unmarshaller;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
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
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.xml.bind.JAXBContext;

import javax.xml.transform.stream.StreamSource;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CEGImporter implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(CEGImporter.class);

	private static final String[] queries = {".*\\\\CEGQuery"};
	private static final String[] annotations = {".*\\\\QueryAnnotations.properties"};
	private static final String[] dataMapFile = {".*\\\\EMIS\\\\EqdDataMap.properties"};
	private static final String[] duplicates = {".*\\\\CEGQuery\\\\DuplicateOrs.properties"};
	private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};

    public Set<ConceptSet> conceptSets= new HashSet<>();
	public Set<String> querySet= new HashSet<>();
	public TTIriRef valueSetFolder;

    private TTEntity owner;
    private final Set<TTEntity> allEntities = new HashSet<>();

    @Override
	public void importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		TTDocument document= manager.createDocument(GRAPH.CEG_QUERY);
		TTEntity graph= new TTEntity()
			.setIri(GRAPH.CEG_QUERY)
				.setName("CEG (QMUL) graph")
					.setDescription("CEG library of value sets, queries and profiles")
						.addType(iri(IM.GRAPH));
			graph.addObject(iri(RDFS.SUBCLASS_OF),iri(IM.GRAPH));
		document.addEntity(graph);
		createOrg(document);
		 CEGEthnicityImport ethnicImport= new CEGEthnicityImport();
		ethnicImport.importData(config);
		createFolders(document);
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);

		}

		//Import queries
		loadAndConvert(config.getFolder());

		if (!conceptSets.isEmpty()) {
			document = new TTDocument(iri(GRAPH.CEG_QUERY));
			for (ConceptSet set : conceptSets) {
				if (!querySet.contains(set.getIri())) {
					TTEntity ttSet = new TTEntity()
						.setIri(set.getIri())
						.setName(set.getName())
                        .addType(iri(IM.VALUESET));
					if (set.getUsedIn() != null) {
						for (TTIriRef used : set.getUsedIn())
							ttSet.addObject(iri(IM.USED_IN), used);
					}
					ttSet.addObject(iri(IM.IS_CONTAINED_IN), valueSetFolder);
					document.addEntity(ttSet);
				}
			}
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
					filer.fileDocument(document);
				}
			}
	}





	private void createFolders(TTDocument document) {
		valueSetFolder= TTIriRef.iri(GRAPH.CEG_QUERY+"CSET_CEGConceptSets");
		TTEntity folder= new TTEntity()
			.setIri(GRAPH.CEG_QUERY+"Q_CEGQueries")
			.setName("QMUL CEG query library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN),iri(IM.NAMESPACE+"Q_Queries"));
			folder.addObject(iri(IM.CONTENT_TYPE),iri(IM.QUERY));
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(GRAPH.CEG_QUERY+"CSET_CEGConceptSets")
			.setName("QMUL CEG value set library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN),TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
		folder.addObject(iri(IM.CONTENT_TYPE),iri(IM.CONCEPT_SET));
		document.addEntity(folder);
		folder= new TTEntity()
			.setIri(GRAPH.CEG_QUERY+"Q_CEGFieldGroups")
			.setName("QMUL CEG Field group library")
			.addType(iri(IM.FOLDER))
			.set(iri(IM.IS_CONTAINED_IN),TTIriRef.iri(IM.NAMESPACE+"Q_CEGQueries"));
		folder.addObject(iri(IM.CONTENT_TYPE),iri(IM.QUERY));;
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
		TTIriRef mainFolder= TTIriRef.iri(GRAPH.CEG_QUERY+"Q_CEGQueries");
		for (File fileEntry : Objects.requireNonNull(directory.toFile().listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext= FilenameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
                    LOG.info("...{}", fileEntry.getName());
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(fileEntry);
					EqdToIMQ converter= new EqdToIMQ();
					ModelDocument qDocument= converter.convertEQD(eqd,dataMap,
						labels);
					TTDocument document= new TTDocument().setGraph(iri(GRAPH.CEG_QUERY));
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
								for (TTEntity inFolder : qFolder.getIsContainedIn())
									ttFolder.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(inFolder.getIri()));
							}
							else
								ttFolder.addObject(iri(IM.IS_CONTAINED_IN),mainFolder);
						}
					}
					if (qDocument.getConceptSet()!=null){
						for (ConceptSet set:qDocument.getConceptSet()){
							TTEntity setEntity= new TTEntity()
								.setIri(set.getIri())
								.addType(TTIriRef.iri(IM.CONCEPT_SET))
								.setName("Unknown value set")
								.set(TTIriRef.iri(IM.DEFINITION),TTLiteral.literal(set.getDefinition()));
							for (TTIriRef used:set.getUsedIn()){
								setEntity.addObject(TTIriRef.iri(IM.USED_IN),used);
							}
							setEntity.addObject(TTIriRef.iri(IM.IS_CONTAINED_IN),TTIriRef.iri("http://endhealth.info/ceg/qry#CSET_CEGConceptSets)"));
							document.addEntity(setEntity);
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
							if (qq.getEntityType().contains(iri(IM.COHORT_QUERY)))
								ttQuery.set(iri(IM.RETURN_TYPE),TTIriRef.iri(IM.NAMESPACE+"Patient"));
							document.addEntity(ttQuery);
							if (qq.getIsContainedIn()!=null){
								for (TTEntity inFolder:qq.getIsContainedIn()){
									ttQuery.addObject(iri(IM.IS_CONTAINED_IN),TTIriRef.iri(inFolder.getIri()));
								}
							}
							ttQuery.set(iri(IM.DEFINITION),TTLiteral.literal(qq.getDefinition()));
						}
					}
				  output(fileEntry,qDocument,document);
						try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
							filer.fileDocument(document);
						}
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
