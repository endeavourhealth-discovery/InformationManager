package org.endeavourhealth.informationmanager.scratch.opensearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class OpenSearchSender {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSender.class);
    private static final int BULKSIZE = 10000;
    private final Client client = ClientBuilder.newClient();
    private WebTarget target;
    private final ObjectMapper om = new ObjectMapper();
    // Env vars
    private final String osUrl = System.getenv("OPENSEARCH_URL");
    private final String osAuth = System.getenv("OPENSEARCH_AUTH");
    private final String server = System.getenv("GRAPH_SERVER");
    private final String repoId = System.getenv("GRAPH_REPO");

    public void execute() throws IOException, InterruptedException {
        checkEnvs();

        int maxId = getMaxDocument();

        continueUpload(maxId);
    }

    private void continueUpload(int maxId) throws JsonProcessingException, InterruptedException {
        target = client.target(osUrl).path("_bulk");

        List<OpenSearchDocument> docs = new ArrayList<>(BULKSIZE);

        LOG.info("Connecting to database...");

        String sql = new StringJoiner(System.lineSeparator())
            .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
            .add("PREFIX im: <http://endhealth.info/im#>")
            .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
            .add("select ?iri ?name ?code ?scheme ?schemeName ?type ?typeName ?status ?statusName")
            .add("where {")
            .add("  ?iri im:status ?status;")
            .add("       rdf:type ?type .")
            .add("  GRAPH ?scheme { ?iri rdfs:label ?name }")
            .add("  OPTIONAL { ?iri im:code ?code }")
            .add("  OPTIONAL { ?status rdfs:label ?statusName }")
            .add("  OPTIONAL { ?scheme rdfs:label ?schemeName }")
            .add("  OPTIONAL { ?type rdfs:label ?typeName }")
            .add("}")
            .add("ORDER BY ?iri")
            // limit 100
            .toString();

        LOG.info("Connecting");

        HTTPRepository repo = new HTTPRepository(server, repoId);
        repo.initialize();
        try (RepositoryConnection conn = repo.getConnection()) {
            LOG.info("Fetching...");

            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {

                LOG.info("Processing...");
                processResults(maxId, docs, qr);

                if (!docs.isEmpty())
                    postMeili(docs);
            }
        }
    }

    private void processResults(int maxId, List<OpenSearchDocument> docs, TupleQueryResult qr) throws JsonProcessingException, InterruptedException {
        int i = 0;
        OpenSearchDocument blob = null;
        while (qr.hasNext()) {
            BindingSet rs = qr.next();

            boolean newConcept = (blob == null) || !blob.getIri().equals(rs.getValue("iri").stringValue());

            if (newConcept)
                i++;

            if (i > maxId) {
                blob = processDocument(docs, blob, i, rs, newConcept);
            } else if ((i % BULKSIZE) == 0) {
                LOG.info("...skipped {} concepts", i);
            }
        }
    }

    private OpenSearchDocument processDocument(List<OpenSearchDocument> docs, OpenSearchDocument blob, int i, BindingSet rs, boolean newConcept) throws JsonProcessingException, InterruptedException {
        if (newConcept) {
            if (docs.size() == BULKSIZE) {
                postMeili(docs);
                docs.clear();
                LOG.info("...processed {} concepts", i);
            }

            blob = new OpenSearchDocument()
                .setId(i)
                .setIri(rs.getValue("iri").stringValue())
                .setName(rs.getValue("name").stringValue())
                .setCode(rs.hasBinding("code") ? rs.getValue("code").stringValue() : null)
                .setScheme(iri(rs.getValue("scheme").stringValue(), rs.hasBinding("schemeName") ? rs.getValue("schemeName").stringValue() : null))
                .setStatus(iri(rs.getValue("status").stringValue(), rs.hasBinding("statusName") ? rs.getValue("statusName").stringValue() : null))
                .addType(iri(rs.getValue("type").stringValue(), rs.hasBinding("typeName") ? rs.getValue("typeName").stringValue() : null));
            docs.add(blob);
        } else {
            blob.addType(iri(rs.getValue("type").stringValue(), rs.getValue("typeName").stringValue()));
        }
        return blob;
    }

    private void checkEnvs() {
        boolean missingEnvs = false;

        for(String env : Arrays.asList("OPENSEARCH_AUTH", "OPENSEARCH_URL", "GRAPH_SERVER", "GRAPH_REPO")) {
            String envData = System.getenv(env);
            if (envData == null || envData.isEmpty()) {
                LOG.error("Environment variable {} not set", env);
                missingEnvs = true;
            }
        }

        if (missingEnvs)
            System.exit(-1);
    }

    private void postMeili(List<OpenSearchDocument> docs) throws JsonProcessingException, InterruptedException {
        StringJoiner batch = new StringJoiner("\n");

        for (OpenSearchDocument doc : docs) {
            batch.add("{ \"index\" : { \"_index\": \"concept\", \"_id\" : \"" + doc.getId() + "\" } }");
            batch.add(om.writeValueAsString(doc));
        }
        batch.add("");

        boolean retry;
        int retrySleep = 5;

        do {
            LOG.info("Sending...");
            retry = false;
            Response response = target
                .request()
                .header("Authorization", "Basic " + osAuth)
                .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));

            if (response.getStatus() == 429) {
                retry = true;
                LOG.error("Queue full, retrying in {}s", retrySleep);
                TimeUnit.SECONDS.sleep(retrySleep);

                if (retrySleep < 60)
                    retrySleep = retrySleep * 2;

            } else if (response.getStatus() != 200 && response.getStatus() != 201) {
                String responseData = response.readEntity(String.class);
                LOG.error(responseData);
                throw new IllegalStateException("Error posting to OpenSearch");
            } else {
                retrySleep = 5;
            }

        } while (retry);
        LOG.info("Done.");
    }

    private int getMaxDocument() throws IOException {
        target = client.target(osUrl).path("_search");

        Response response = target
            .request()
            .header("Authorization", "Basic " + osAuth)
            .post(Entity.entity("{\n" +
                "    \"aggs\" : {\n" +
                "      \"max_id\" : {\n" +
                "        \"max\" : { \n" +
                "          \"field\" : \"id\"\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"size\":0\n" +
                "  }", MediaType.APPLICATION_JSON));

        if (response.getStatus() != 200) {
            String responseData = response.readEntity(String.class);
            LOG.error(responseData);
            throw new IllegalStateException("Error getting max document id from OpenSearch");
        } else {
            String responseData = response.readEntity(String.class);
            JsonNode root = om.readTree(responseData);
            int maxId = root.get("aggregations").get("max_id").get("value").asInt();
            if (maxId > 0)
                LOG.info("Continuing from {}", maxId);
            return maxId;
        }
    }
}
