package org.endeavourhealth.informationmanager.scratch.meili;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class OpenSearchSender {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSender.class);
    private static final int BULKSIZE = 10000;
    private final Client client = ClientBuilder.newClient();
    private WebTarget target;
    private final ObjectMapper om = new ObjectMapper();

    public void execute() throws JsonProcessingException, InterruptedException {
        checkEnvs("OPENSEARCH_URL", "OPENSEARCH_AUTH", "GRAPH_SERVER", "GRAPH_REPO");

        String OSUrl = System.getenv("OPENSEARCH_URL");

        target = client.target(OSUrl).path("_bulk");

        List<MeiliBlob> docs = new ArrayList<>(BULKSIZE);
        MeiliBlob blob = null;
        int i = 0;

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

        String server = System.getenv("GRAPH_SERVER");
        String repoid = System.getenv("GRAPH_REPO");

        HTTPRepository repo = new HTTPRepository(server, repoid);
        repo.initialize();
        try (RepositoryConnection conn = repo.getConnection()) {
            LOG.info("Fetching...");

            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {

                LOG.info("Processing...");
                while (qr.hasNext()) {
                    BindingSet rs = qr.next();
                    if (blob == null) {
                        blob = new MeiliBlob()
                            .setId(i)
                            .setIri(rs.getValue("iri").stringValue())
                            .setName(rs.getValue("name").stringValue())
                            .setCode(rs.hasBinding("code") ? rs.getValue("code").stringValue() : null)
                            .setScheme(iri(rs.getValue("scheme").stringValue(), rs.hasBinding("schemeName") ? rs.getValue("schemeName").stringValue() : null))
                            .setStatus(iri(rs.getValue("status").stringValue(), rs.hasBinding("statusName") ? rs.getValue("statusName").stringValue() : null));
                        docs.add(blob);
                    }

                    if (rs.getValue("iri").stringValue().equals(blob.getIri())) {
                        blob.addType(iri(rs.getValue("type").stringValue(), rs.getValue("typeName").stringValue()));
                    } else {
                        if (((++i) % BULKSIZE) == 0) {
                            postMeili(docs);
                            docs.clear();
                            LOG.info("...processed {} concepts", i);
                        }

                        blob = new MeiliBlob()
                            .setId(i)
                            .setIri(rs.getValue("iri").stringValue())
                            .setName(rs.getValue("name").stringValue())
                            .setCode(rs.hasBinding("code") ? rs.getValue("code").stringValue() : null)
                            .setScheme(iri(rs.getValue("scheme").stringValue(), rs.hasBinding("schemeName") ? rs.getValue("schemeName").stringValue() : null))
                            .setStatus(iri(rs.getValue("status").stringValue(), rs.hasBinding("statusName") ? rs.getValue("statusName").stringValue() : null))
                            .addType(iri(rs.getValue("type").stringValue(), rs.hasBinding("typeName") ? rs.getValue("typeName").stringValue() : null));
                        docs.add(blob);
                    }
                }

                if (!docs.isEmpty())
                    postMeili(docs);
            }
        }
    }

    private void checkEnvs(String... envs) {
        boolean missingEnvs = false;
        for(String env : envs) {
            String envData = System.getenv(env);
            if (envData == null || envData.isEmpty()) {
                LOG.error("Environment variable {} not set", env);
                missingEnvs = true;
            }
        }

        if (missingEnvs)
            System.exit(-1);
    }

    private void postMeili(List<MeiliBlob> docs) throws JsonProcessingException, InterruptedException {
        StringJoiner batch = new StringJoiner("\n");

        for (MeiliBlob doc : docs) {
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
                .header("Authorization", "Basic " + System.getenv("OPENSEARCH_AUTH"))
                .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));

            if (response.getStatus() == 429) {
                retry = true;
                LOG.error("Queue busy, retrying in {}s", retrySleep);
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
}
