package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.requests.QueryRequest;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.logic.reasoner.SetBinder;
import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;
import static org.endeavourhealth.imapi.vocabulary.VocabUtils.asHashSet;

public class QImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(QImporter.class);
  private final Client client = ClientBuilder.newClient();
  private final TTDocument document = new TTDocument();
  private final TTIriRef projectsFolder = TTIriRef.iri(Namespace.QR + "QProjects");
  private final Map<String, TTEntity> idProjectMap = new HashMap<>();
  private final Map<String, TTEntity> idCodeGroupMap = new HashMap<>();
  private final ObjectMapper om = new ObjectMapper();
  private final Map<String, String> projectVersion = new HashMap<>();

  @Override
  public void importData(TTImportConfig ttImportConfig) throws ImportException {
    try {
      try (TTManager manager = new TTManager()) {
        document.addEntity(manager.createNamespaceEntity(Namespace.QR,
          "Q Research scheme and graph"
          , "Q Research scheme and graph"));
        addQFolders();
        importQProjects();
        importCodeGroups();
        for (Map.Entry<String, TTEntity> entry : idProjectMap.entrySet()) {
          document.addEntity(entry.getValue());
        }
        for (Map.Entry<String, TTEntity> entry : idCodeGroupMap.entrySet()) {
          document.addEntity(entry.getValue());
        }
        QueryRequest qr = new QueryRequest()
          .addArgument(new Argument()
            .setParameter("this")
            .setValueIri(iri(Namespace.QR)))
          .setUpdate(new Update().setIri(Namespace.IM + "DeleteSets"));

        LOG.info("Deleting q code groups..");
        new SearchService().updateIM(qr, Graph.IM);
        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
          filer.fileDocument(document);
        }
        resetDrugs();
      }
    } catch (Exception ex) {
      throw new ImportException(ex.getMessage(), ex);
    }
  }

  private void resetDrugs() throws QueryException, TTFilerException, JsonProcessingException {
    TTDocument drugDocument = new TTDocument();
    LOG.info("if drugs then creating as entailed members");
    for (TTEntity entity : document.getEntities()) {
      if (entity.isType(iri(IM.CONCEPT_SET))) {
        if (entity.get(iri(IM.HAS_MEMBER)) != null) {
          if (isMedicationSet(entity)) {
            for (TTValue medication : entity.get(iri(IM.HAS_MEMBER)).getElements()) {
              entity.addObject(iri(IM.ENTAILED_MEMBER), new TTNode()
                .set(iri(IM.INSTANCE_OF), medication)
                .set(iri(IM.ENTAILMENT), iri(IM.DESCENDANTS_OR_SELF_OF)));

            }
            entity.getPredicateMap().remove(iri(IM.HAS_MEMBER));
            drugDocument.addEntity(entity);
          }
        }
      }
    }
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
      filer.fileDocument(drugDocument);
    }
  }

  private boolean isMedicationSet(TTEntity entity) {
    if (entity.get(IM.HAS_MEMBER) != null) {
      SetBinder binder = new SetBinder();
      binder.bindSet(entity.getIri(), Graph.IM);
      TTEntity boundSet = new EntityService().getBundle(entity.getIri(), asHashSet(IM.BINDING)).getEntity();

      if (!boundSet.has(IM.BINDING)) {
        LOG.error("Set has no bindings [{}]", entity.getIri());
        return false;
      }

      for (TTValue binding : boundSet.get(IM.BINDING).getElements()) {
        if (binding.asNode().get(SHACL.NODE).asIriRef().getIri().contains("Medication"))
          return true;
      }
    }
    return false;
  }


  private void importCodeGroups() throws JsonProcessingException {
    for (Map.Entry<String, TTEntity> project : idProjectMap.entrySet()) {
      String projectId = project.getKey();
      LOG.info("Fetching  code groups for project " + projectId + "...");
      int page = 0;
      TTEntity projectEntity = project.getValue();
      String projectVersion = projectEntity.get(iri(IM.VERSION)).asLiteral().getValue();
      boolean results = true;
      while (results) {
        page++;
        JsonNode json = getResults("codegroups_for_project/" + projectId, page);
        ArrayNode groups = (ArrayNode) json.get("Results");
        if (!groups.isEmpty()) {
          for (Iterator<JsonNode> it = groups.elements(); it.hasNext(); ) {
            JsonNode codeGroup = it.next();
            String id = codeGroup.get("Id").asText();
            String groupId = id;
            if (!codeGroup.get("Name").asText().contains("(ICD10)")) {
              String version = codeGroup.get("CurrentVersion").asText();
              TTEntity qGroup = idCodeGroupMap.get(groupId);
              if (qGroup == null) {
                qGroup = new TTEntity()
                  .setIri(Namespace.QR + "QCodeGroup_" + groupId)
                  .setName("Q code group " + codeGroup.get("Name").asText())
                  .setScheme(Namespace.QR.asIri())
                  .addType(iri(IM.CONCEPT_SET));
                if (idCodeGroupMap.get(groupId) == null) {
                  idCodeGroupMap.put(groupId, qGroup);
                }
                if (qGroup.getIri().equals("http://apiqcodes.org/qcodes#QPredict_347")) {
                  qGroup.addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "EthnicitySets"));
                }
                qGroup.set(iri(IM.VERSION), TTLiteral.literal(version));
                importCodes(projectId, qGroup, id);
              }
              qGroup.addObject(iri(IM.IS_SUBSET_OF), TTIriRef.iri(project.getValue().getIri()));
            }
          }
        } else
          results = false;
      }
    }
  }

  private void importCodes(String projectId, TTEntity qGroup, String id) throws JsonProcessingException {
    String version = qGroup.get(iri(IM.VERSION)).asLiteral().getValue();
    int page = 0;
    boolean results = true;
    LOG.info("Fetching  members for  " + projectId + " " + qGroup.getName() + "...");
    while (results) {
      page++;
      JsonNode json = getResults("codes_for_codegroup/" + id + "/" + projectId + "/" + version, page);
      ArrayNode codes = (ArrayNode) json.get("Results");
      if (!codes.isEmpty()) {
        for (Iterator<JsonNode> it = codes.elements(); it.hasNext(); ) {
          JsonNode code = it.next();
          String concept = Namespace.SNOMED + code.get("Code").asText();
          String term = code.get("Text").asText();
          qGroup.addObject(iri(IM.HAS_MEMBER), TTIriRef.iri(concept));
        }
      } else
        results = false;
    }
  }

  private void importQProjects() throws JsonProcessingException {
    LOG.info("Fetching Q projects ...");
    JsonNode json = getResults("projects_list", 1);
    ArrayNode projects = (ArrayNode) json.get("Results");
    for (Iterator<JsonNode> it = projects.elements(); it.hasNext(); ) {
      JsonNode project = it.next();
      String id = project.get("Id").asText();
      TTEntity qset = new TTEntity()
        .setIri(Namespace.QR + "QPredict_" + project.get("Id").asText())
        .addType(iri(IM.CONCEPT_SET))
        .setScheme(Namespace.QR.asIri())
        .setName(project.get("Name").asText());
      qset.set(iri(IM.IS_CONTAINED_IN), projectsFolder);
      qset.set(iri(SHACL.ORDER), TTLiteral.literal(1));
      String version = project.get("Version").asText();
      qset.set(iri(IM.VERSION), TTLiteral.literal(version));
      if (qset.getIri().equals("http://apiqcodes.org/qcodes#QPredict_347")) {
        qset.addObject(iri(IM.IS_CONTAINED_IN), iri(Namespace.IM + "EthnicitySets"));
      }
      if (idProjectMap.get(id) == null) {
        idProjectMap.put(id, qset);
        projectVersion.put(id, version);
      } else if (Integer.parseInt(version) > Integer.parseInt(projectVersion.get(id))) {
        idProjectMap.put(id, qset);
        projectVersion.put(id, version);
      }
    }
  }

  private String getecl() {
    return "<< 763158003 | Medicinal product (product) | :  (<< 127489000 | Has active ingredient (attribute) |  = << 116601002 | Prednisolone (substance) |  OR << 127489000 | Has active ingredient (attribute) |  = << 396458002 | Hydrocortisone (substance) |  OR << 127489000 | Has active ingredient (attribute) |  = << 116571008 | Betamethasone (substance) |  OR << 127489000 | Has active ingredient (attribute) |  = << 396012006 | Deflazacort (substance) |  OR << 127489000 | Has active ingredient (attribute) |  = << 372584003 | Dexamethasone (substance) |  OR << 127489000 | Has active ingredient (attribute) |  = << 116593003 | Methylprednisolone (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 116601002 | Prednisolone (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 396458002 | Hydrocortisone (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 116571008 | Betamethasone (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 396012006 | Deflazacort (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 372584003 | Dexamethasone (substance) |  OR << 10363001000001101 | Has NHS dm+d (dictionary of medicines and devices) basis of strength substance (attribute) |  = << 116593003 | Methylprednisolone (substance) | ) ,  (<< 411116001 | Has manufactured dose form (attribute) |  = << 385268001 | Oral dose form (dose form) |  OR << 13088501000001100 | Has NHS dm+d (dictionary of medicines and devices) VMP (Virtual Medicinal Product) ontology form and route (attribute) |  = << 21000001106 | tablet.oral ontology form and route (qualifier value) |  OR << 13088401000001104 | Has NHS dm+d (dictionary of medicines and devices) VMP (Virtual Medicinal Product) route of administration (attribute) |  = << 26643006 | Oral route (qualifier value) |  OR << 10362901000001105 | Has dispensed dose form (attribute) |  = << 385268001 | Oral dose form (dose form) | )";
  }

  private JsonNode getResults(String path, int page) throws JsonProcessingException {
    String url = System.getenv("Q_URL");
    String auth = System.getenv("Q_AUTH");


    WebTarget target = client.target(url)
      .path(path)
      .queryParam("PageNumber", page)
      .queryParam("PageSize", 10000);
    Response response = target
      .request(MediaType.APPLICATION_JSON_TYPE)
      .header("Ocp-Apim-Subscription-Key", auth)
      .get();

    String responseRaw = response.readEntity(String.class);
    if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
      LOG.error("Could not get Q results for " + url + "/" + path);
      LOG.error(responseRaw);
      System.exit(-1);
      return null;
    } else {
      return om.readTree(responseRaw);
    }
  }

  private void addQFolders() {
    TTEntity folder = new TTEntity()
      .setIri(projectsFolder.getIri())
      .addType(iri(IM.FOLDER))
      .setScheme(Namespace.IM.asIri())
      .setName("Q Project based code groups")
      .setDescription("Folder containing the Q research  concept groups");
    folder.addObject(iri(IM.CONTENT_TYPE), iri(IM.CONCEPT_SET));
    document.addEntity(folder);
    folder.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "QueryConceptSets"));
   /* TTEntity qFolder = new TTEntity()
      .setIri(Namespace.IM + "Q_PredictionQueries")
      .addType(iri(IM.FOLDER))
      .setScheme(Namespace.IM.asIri())
      .setName("Predication queries")
      .setDescription("Folder containing queries for prediction algorithms");
    qFolder.addObject(iri(IM.CONTENT_TYPE), iri(IM.QUERY));
    document.addEntity(qFolder);
    qFolder.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(Namespace.IM + "Q_Queries"));

    */
  }

  @Override
  public void validateFiles(String s) throws TTFilerException {
    boolean missingEnvs = false;
    Iterator var2 = Arrays.asList("Q_AUTH", "Q_URL", "GRAPH_SERVER", "GRAPH_REPO").iterator();
    while (true) {
      String env;
      String envData;
      do {
        if (!var2.hasNext()) {
          if (missingEnvs) {
            System.exit(-1);
          }

          return;
        }

        env = (String) var2.next();
        envData = System.getenv(env);
      } while (envData != null && !envData.isEmpty());

      LOG.error("Environment variable {} not set", env);
      missingEnvs = true;
    }


  }

  @Override
  public void close() throws Exception {

  }
}
