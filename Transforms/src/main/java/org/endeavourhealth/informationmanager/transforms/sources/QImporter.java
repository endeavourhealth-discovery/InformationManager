package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.rdf4j.TTTransactionFiler;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.util.*;

public class QImporter implements TTImport {
	private final Client client= ClientBuilder.newClient();
	private final TTDocument document = new TTDocument(TTIriRef.iri(QR.NAMESPACE));
	private final TTIriRef projectsFolder= TTIriRef.iri(QR.NAMESPACE+"QProjects");
	private final Map<String,TTEntity> idProjectMap = new HashMap<>();
	private final Map<String,TTEntity> idCodeGroupMap = new HashMap<>();
	private final ObjectMapper om = new ObjectMapper();
	private final Map<String,String> codeGroupVersion = new HashMap<>();
	private final Map<String,String> projectVersion= new HashMap<>();

	private static final Logger LOG = LoggerFactory.getLogger(QImporter.class);
	@Override
	public void importData(TTImportConfig ttImportConfig) throws Exception {
		TTManager manager = new TTManager();
		document.addEntity(manager.createGraph(QR.NAMESPACE,
				"Q Research scheme and graph"
				,"Q Research scheme and graph"));
		addQFolders();
//	queryQRisk3();
		importQProjects();
		importCodeGroups();
		for (Map.Entry<String,TTEntity> entry:idProjectMap.entrySet()){
			document.addEntity(entry.getValue());
		}
		for (Map.Entry<String,TTEntity> entry:idCodeGroupMap.entrySet()){
			document.addEntity(entry.getValue());
		}
		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			manager.setDocument(document);
			manager.saveDocument(new File(directory + "\\QCodes.json"));
		}
		QueryRequest qr= new QueryRequest()
			.addArgument(new Argument()
				.setParameter("this")
				.setValueIri(TTIriRef.iri(QR.NAMESPACE)))
			.setUpdate(new Update().setIri(IM.NAMESPACE+"DeleteSets"));

		LOG.info("Deleting q code groups..");
		new SearchService().updateIM(qr);
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}

	}

	private void queryQRisk3() throws JsonProcessingException {
		TTEntity qRisk3= new TTEntity()
			.setIri(IM.NAMESPACE+"Q_QRisk3")
			.setName("QRisk3 record query")
			.setDescription("query of health data to set qrisk 3 parameters");
		Query query= new Query();
		qRisk3.set(IM.DEFINITION,TTLiteral.literal(query));
		query
			.setIri(IM.NAMESPACE+"Q_Qrisk3")
			.setName("QRisk3 health record query")
			.setTypeOf(IM.NAMESPACE+"Patient");
		query.addReturn(new Return());
		qMatch(query,null,"age","age",false,null,false);
		qMatch(query,null,"statedGender","sex",false,null,false);
		qMatch(query,"observation","concept=_411","cvd",true,null,true);
		qMatch(query,"observation","concept=_24","af",true,null,true);
	}



	private void ageSex(Query query){
		query.getReturn().get(0)
			.property(p->p
				.as("age")
				.setIri("age")
				.setUnit("years"))
			.property(p->p
					.as("sex")
					.case_(c->c
						.when(w->w
							.property(p1->p1
								.setIri("statedGender")
								.is(in->in.setIri("http://endhealth.info/im#905031000252103")))
							.then(t->t.setValue("Male")))
						.when(w->w
							.property(p1->p1
								.setIri("statedGender")
								.is(in->in.setIri("http://endhealth.info/im#905041000252107")))
							.then(t->t.setValue("Female")))
						.else_x(e->e
							.setValue("Male"))));
	}

	private void qMatch(Query query, String path, String propertyValues,String as,Boolean optional,
											Map<String,String> mapValues,
											boolean isTruFalse) {
		Match match= new Match();
		query.addMatch(match);
		if (optional==true)
			match.setOptional(true);
		if (path!=null) {
			for (int i = 0; i < path.split("/").length - 1; i++) {
				Property property = new Property();
				match.addProperty(property);
				property.setIri(path.split("/")[i]);
				match = match.setMatch(new Match().getMatch());
			}
		}
		for (int i=0; i<propertyValues.split(",").length-1;i++){
			String pv= propertyValues.split(",")[i];
			String field= pv.split("=")[0];
			Property property= new Property();
			property.setIri(field);
			match.addProperty(property);
			String values= pv.split("=")[1];
			for (int q=0; q< values.split(";").length-1; q++){
				String value= values.split(";")[q];
				if (value.startsWith("http")){
					property.addInSet(new Node().setIri(value));
				}
				property.setValueVariable(as);
			}
		}
		query.getReturn().get(0)
			.property(p->p
				.setAs(as)
				.setValueRef(as));


	}

	private void importCodeGroups() throws JsonProcessingException {
		for (Map.Entry<String,TTEntity> project:idProjectMap.entrySet()) {
			String projectId = project.getKey();
			LOG.info("Fetching  code groups for project "+projectId+"...");
			int page=0;
			boolean results=true;
			while (results) {
				page++;
				JsonNode json= getResults("codegroups_for_project/" + projectId,page);
				ArrayNode groups= (ArrayNode) json.get("Results");
				if (!groups.isEmpty()) {
					for (Iterator<JsonNode> it = groups.elements(); it.hasNext(); ) {
						JsonNode codeGroup = it.next();
						String id = codeGroup.get("Id").asText();
						String groupId=id;
						String version = codeGroup.get("CurrentVersion").asText();
						TTEntity qGroup = idCodeGroupMap.get(groupId);
						if (qGroup == null) {
							qGroup = new TTEntity()
								.setIri(QR.NAMESPACE + "QCodeGroup_" + groupId)
								.setName("Q code group "+codeGroup.get("Name").asText())
								.addType(IM.CONCEPT_SET);
						}
						qGroup.addObject(IM.IS_SUBSET_OF, TTIriRef.iri(project.getValue().getIri()));
						qGroup.set(IM.VERSION, TTLiteral.literal(version));
						if (idCodeGroupMap.get(groupId)==null) {
							idCodeGroupMap.put(groupId, qGroup);
							codeGroupVersion.put(groupId, version);
						}
						else if (Integer.parseInt(version)>Integer.parseInt(codeGroupVersion.get(groupId))){
							idCodeGroupMap.put(groupId, qGroup);
							codeGroupVersion.put(groupId, version);
						}
						importCodes(projectId, qGroup, id);

					}
				}
				else
					results= false;
			}
		}
	}

	private void importCodes(String projectId, TTEntity qGroup,String id) throws JsonProcessingException {
		String version = qGroup.get(IM.VERSION).asLiteral().getValue();
		int page=0;
		boolean results=true;
		LOG.info("Fetching  members for  "+projectId+" "+ qGroup.getName()+"...");
		while (results) {
			page++;
			JsonNode json = getResults("codes_for_codegroup/" + id + "/" + projectId + "/" + version,page);
			ArrayNode codes= (ArrayNode) json.get("Results");
			if (!codes.isEmpty()) {
				for (Iterator<JsonNode> it = codes.elements(); it.hasNext(); ) {
					JsonNode code = it.next();
					String concept = SNOMED.NAMESPACE + code.get("Code").asText();
					String term = code.get("Text").asText();
					qGroup.addObject(IM.HAS_MEMBER, TTIriRef.iri(concept));
				}
			}
			else
				results=false;
		}
	}

	private void importQProjects() throws JsonProcessingException {
		LOG.info("Fetching Q projects ...");
		JsonNode json = getResults("projects_list",1);
		ArrayNode projects= (ArrayNode) json.get("Results");
		for (Iterator<JsonNode> it = projects.elements(); it.hasNext(); ) {
			JsonNode project = it.next();
			/*
			TTEntity qp= new TTEntity()
				.setIri(QR.NAMESPACE+"QProject_"+ project.get("Id").asText())
				.addType(IM.FOLDER)
				.setName(project.get("Name").asText());
			qp.set(IM.IS_CONTAINED_IN,projectsFolder);
			document.addEntity(qp);

			 */
			String id= project.get("Id").asText();
			TTEntity qset= new TTEntity()
				.setIri(QR.NAMESPACE+"QPredict_"+ project.get("Id").asText())
				.addType(IM.CONCEPT_SET)
				.setName(project.get("Name").asText());
			qset.set(IM.IS_CONTAINED_IN,projectsFolder);
			qset.set(SHACL.ORDER,TTLiteral.literal(1));
			String version = project.get("Version").asText();
			qset.set(IM.VERSION,TTLiteral.literal(version));
			if (idProjectMap.get(id)==null) {
				idProjectMap.put(id, qset);
				projectVersion.put(id,version);
			}
			else if (Integer.parseInt(version)> Integer.parseInt(projectVersion.get(id))) {
				idProjectMap.put(id,qset);
				projectVersion.put(id,version);
			}
		}
	}

	private JsonNode getResults(String path,int page) throws JsonProcessingException {
		String url = System.getenv("Q_URL");
		String auth = System.getenv("Q_AUTH");


		WebTarget target = client.target(url)
			.path(path)
			.queryParam("PageNumber",page)
			.queryParam("PageSize",10000);
		Response response = target
			.request(MediaType.APPLICATION_JSON_TYPE)
			.header("Ocp-Apim-Subscription-Key", auth)
			.get();

		String responseRaw = response.readEntity(String.class);
		if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
			LOG.error("Could not get Q results for "+ url+"/"+path);
			LOG.error(responseRaw);
			System.exit(-1);
			return null;
		} else {
			 return om.readTree(responseRaw);
		}
	}

	private void addQFolders() {
		TTEntity folder= new TTEntity()
			.setIri(projectsFolder.getIri())
			.addType(IM.FOLDER)
			.setName("Q Project based code groups")
			.setDescription("Folder containing the Q research  concept groups");
		folder.addObject(IM.CONTENT_TYPE,IM.CONCEPT_SET);
		document.addEntity(folder);
		folder.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
		TTEntity qFolder= new TTEntity()
			.setIri(IM.NAMESPACE+"Q_PredictionQueries")
			.addType(IM.FOLDER)
			.setName("Predication queries")
			.setDescription("Folder containing queries for prediction algorithms");
		qFolder.addObject(IM.CONTENT_TYPE,IM.QUERY);
		document.addEntity(qFolder);
		qFolder.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"Q_Queries"));
	}

	@Override
	public void validateFiles(String s) throws TTFilerException {
			boolean missingEnvs = false;
			Iterator var2 = Arrays.asList("Q_AUTH", "Q_URL", "GRAPH_SERVER", "GRAPH_REPO").iterator();
			while(true) {
				String env;
				String envData;
				do {
					if (!var2.hasNext()) {
						if (missingEnvs) {
							System.exit(-1);
						}

						return;
					}

					env = (String)var2.next();
					envData = System.getenv(env);
				} while(envData != null && !envData.isEmpty());

				LOG.error("Environment variable {} not set", env);
				missingEnvs = true;
			}


	}

	@Override
	public void close() throws Exception {

	}
}
