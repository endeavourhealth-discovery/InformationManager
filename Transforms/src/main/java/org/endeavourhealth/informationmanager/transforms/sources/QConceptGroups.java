package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.endeavourhealth.imapi.filer.*;
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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class QConceptGroups implements TTImport {
	private final Client client= ClientBuilder.newClient();
	private final TTDocument document = new TTDocument(TTIriRef.iri(QR.NAMESPACE));
	private final TTIriRef projectsFolder= TTIriRef.iri(QR.NAMESPACE+"QProjects");
	private final Map<String,String> idProjectMap = new HashMap<>();
	private final Map<String,TTEntity> idCodeGroupMap = new HashMap<>();
	private final ObjectMapper om = new ObjectMapper();
	private final Map<String,String> codeGroups = new HashMap<>();

	private static final Logger LOG = LoggerFactory.getLogger(QConceptGroups.class);
	@Override
	public void importData(TTImportConfig ttImportConfig) throws Exception {
		addQFolder();
		importQProjects();
		importCodeGroups();
		if ( ImportApp.testDirectory!=null) {
			String directory = ImportApp.testDirectory.replace("%", " ");
			TTManager manager = new TTManager();
			manager.setDocument(document);
			manager.saveDocument(new File(directory + "\\QCodes.json"));
		}
		if (!TTFilerFactory.isBulk()) {
			TTTransactionFiler filer= new TTTransactionFiler(null);
			filer.fileTransaction(document);
		} else {
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}
		}
	}

	private void importCodeGroups() throws JsonProcessingException {
		for (Map.Entry<String,String> project:idProjectMap.entrySet()) {
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
						String version = codeGroup.get("CurrentVersion").asText();
						TTEntity qGroup = idCodeGroupMap.get(id + "_" + version);
						if (qGroup == null) {
							qGroup = new TTEntity()
								.setIri(QR.NAMESPACE + "QCodeGroup_" + id)
								.setName("Q code group "+codeGroup.get("Name").asText())
								.addType(IM.CONCEPT_SET);
						}
						qGroup.addObject(IM.IS_SUBSET_OF, TTIriRef.iri(project.getValue()));
						qGroup.set(IM.VERSION, TTLiteral.literal(version));
						document.addEntity(qGroup);
						idCodeGroupMap.put(id + "_" + version, qGroup);
						if (codeGroups.get(id)==null) {
							importCodes(projectId, qGroup, id);
							codeGroups.put(id, version);
						}
						else {
							if (!codeGroups.get(id).equals(version))
								System.err.println("Code group " + id + " has 2 versions");
						}
					}
				}
				else
					results= false;
			}
		}
	}

	private void importCodes(String projectId, TTEntity qGroup,String groupId) throws JsonProcessingException {
		String version = qGroup.get(IM.VERSION).asLiteral().getValue();
		int page=0;
		boolean results=true;
		LOG.info("Fetching  members for  "+projectId+" code group "+ qGroup.getName()+"...");
		while (results) {
			page++;
			JsonNode json = getResults("codes_for_codegroup/" + groupId + "/" + projectId + "/" + version,page);
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
			TTEntity qp= new TTEntity()
				.setIri(IM.NAMESPACE+"QProject_"+ project.get("Id").asText())
				.addType(IM.FOLDER)
				.setName(project.get("Name").asText());
			qp.set(IM.IS_CONTAINED_IN,projectsFolder);
			document.addEntity(qp);
			TTEntity qset= new TTEntity()
				.setIri(QR.NAMESPACE+"QPredict_"+ project.get("Id").asText())
				.addType(IM.CONCEPT_SET)
				.setName(project.get("Name").asText());
			qset.set(IM.IS_CONTAINED_IN,IM.NAMESPACE+"QProject_"+ project.get("Id").asText());
			qset.set(SHACL.ORDER,TTLiteral.literal(1));
			document.addEntity(qset);

			idProjectMap.put(project.get("Id").asText(),qset.getIri());

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

	private void addQFolder() {
		TTEntity folder= new TTEntity()
			.setIri(projectsFolder.getIri())
			.addType(IM.FOLDER)
			.setName("Q Project based code groups")
			.setDescription("Folder containing the Q research  concept groups");
		document.addEntity(folder);
		folder.set(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"QueryConceptSets"));
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