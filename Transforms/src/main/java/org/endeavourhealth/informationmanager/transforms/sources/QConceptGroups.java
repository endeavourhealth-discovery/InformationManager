package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.endeavourhealth.imapi.dataaccess.OpenSearchSender;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class QConceptGroups implements TTImport {
	private final Client client= ClientBuilder.newClient();
	private final TTDocument document = new TTDocument(TTIriRef.iri(QR.NAMESPACE));
	private final TTIriRef projectsFolder= TTIriRef.iri(QR.NAMESPACE+"QProjects");
	private final Map<String,String> idProjectMap = new HashMap<>();
	private final Map<String,String> idCodeGroupMap = new HashMap<>();

	private static final Logger LOG = LoggerFactory.getLogger(QConceptGroups.class);
	@Override
	public void importData(TTImportConfig ttImportConfig) throws Exception {
		addQFolder();
		importQProjects();
		importCodeGroups();
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
			ArrayNode projects = getResults("codegroups_for_project/" + projectId);
			for (Iterator<JsonNode> it = projects.elements(); it.hasNext(); ) {
				JsonNode codeGroup = it.next();
				String id= codeGroup.get("Id").asText();
				TTEntity qGroup = new TTEntity()
					.setIri(QR.NAMESPACE+"CSET_QCodeGroup_"+id)
					.setName(codeGroup.get("Name").asText())
					.addType(IM.CONCEPT_SET);
				qGroup.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(project.getValue()));
				qGroup.set(IM.VERSION, TTLiteral.literal(codeGroup.get("CurrentVersion")));
				document.addEntity(qGroup);
				idCodeGroupMap.put(id,qGroup.getIri());
			}
		}
	}

	private void importQProjects() throws JsonProcessingException {
		ArrayNode projects = getResults("projects_list");
		for (Iterator<JsonNode> it = projects.elements(); it.hasNext(); ) {
			JsonNode project = it.next();
			TTEntity qp= new TTEntity()
				.setIri(QR.NAMESPACE+"Project_"+ project.get("Id").asText())
				.addType(IM.FOLDER)
				.setName(project.get("Name").asText());
			qp.set(IM.IS_CONTAINED_IN,projectsFolder);
			document.addEntity(qp);
			idProjectMap.put(project.get("Id").asText(),qp.getIri());

		}
	}

	private ArrayNode getResults(String path) throws JsonProcessingException {
		String url = System.getenv("Q_URL");
		String bearer = System.getenv("Q_AUTH");
		ObjectMapper om = new ObjectMapper();
		LOG.info("Fetching Q projects ...");
		WebTarget target = client.target(url)
			.path(path);
		Response response = target
			.request(MediaType.APPLICATION_JSON_TYPE)
			.header("Authorization", "Bearer " + bearer)
			.get();

		String responseRaw = response.readEntity(String.class);
		if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
			LOG.error("Could not get Q results for "+ url+"/"+path);
			LOG.error(responseRaw);
			System.exit(-1);
			return null;
		} else {
			JsonNode json = om.readTree(responseRaw);
			return (ArrayNode) json.get("Results");
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
