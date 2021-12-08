package org.endeavourhealth.informationmanager.scratch.meili;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class Main {
    private static final int BULKSIZE= 10000;
    private static Client client = ClientBuilder.newClient();
    private static WebTarget target = client.target("https://search.endeavourhealth.net").path("_bulk");
    private static ObjectMapper om = new ObjectMapper();

    public static void main(String[] argv) throws SQLException, ClassNotFoundException, JsonProcessingException, InterruptedException {
        String sql = new StringJoiner(System.lineSeparator())
            .add("SELECT e.dbid, e.iri, e.name, e.code, e.scheme AS scheme, REPLACE(n.name, \" namespace\", \"\") as schemeName, et.type, typ.name AS typeName, e.status, stt.name AS statusName")
            .add("FROM entity e")
            .add("JOIN entity_type et ON et.entity = e.dbid")
            .add("LEFT JOIN namespace n ON n.iri = e.scheme")
            .add("LEFT JOIN entity typ ON typ.iri = et.type")
            .add("LEFT JOIN entity stt ON stt.iri = e.status")
            .add("ORDER BY e.dbid;")
            .toString();

        /* TODO: Migrate to graph/sparql

            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX im: <http://endhealth.info/im#>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            select ?iri ?name ?code ?scheme ?schemeName ?type ?typeName ?status ?statusName
            where {
                GRAPH ?scheme { ?iri rdfs:label ?name;
                    im:code ?code;
                 im:status ?status;
                 rdf:type ?type }
                OPTIONAL { ?status rdfs:label ?statusName }
                OPTIONAL { ?scheme rdfs:label ?schemeName }
                OPTIONAL { ?type rdfs:label ?typeName }
            }
            order by ?iri
            limit 100
         */

        System.out.println("Connecting to database...");

        try (
            Connection conn = getConnection();
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {

            System.out.println("Processing...");
            List<MeiliBlob> docs = new ArrayList<>(BULKSIZE);
            MeiliBlob blob = null;
            int i = 0;
            while(rs.next()) {
                if (blob == null) {
                    blob = new MeiliBlob()
                        .setId(rs.getInt("dbid"))
                        .setIri(rs.getString("iri"))
                        .setName(rs.getString("name"))
                        .setCode(rs.getString("code"))
                        .setScheme(iri(rs.getString("scheme"), rs.getString("schemeName")))
                        .setStatus(iri(rs.getString("status"), rs.getString("statusName")));
                    docs.add(blob);
                }

                if (rs.getInt("dbid") == blob.getId()) {
                    blob.addType(iri(rs.getString("type"), rs.getString("typeName")));
                } else {
                    if (((++i) % BULKSIZE) == 0) {
                        postMeili(docs);
                        docs.clear();
                        System.out.println("...processed " + i + " rows");
                    }

                    blob = new MeiliBlob()
                        .setId(rs.getInt("dbid"))
                        .setIri(rs.getString("iri"))
                        .setName(rs.getString("name"))
                        .setCode(rs.getString("code"))
                        .setScheme(iri(rs.getString("scheme"), rs.getString("schemeName")))
                        .setStatus(iri(rs.getString("status"), rs.getString("statusName")))
                        .addType(iri(rs.getString("type"), rs.getString("typeName")));
                    docs.add(blob);
                }
            }
        }
    }

    private static void postMeili(List<MeiliBlob> docs) throws JsonProcessingException, InterruptedException {
        StringJoiner batch = new StringJoiner("\n");

        for (MeiliBlob doc : docs) {
            batch.add("{ \"index\" : { \"_index\": \"dev-test2\", \"_id\" : \"" + doc.getId() + "\" } }");
            batch.add(om.writeValueAsString(doc));
        }
        batch.add("");

        boolean retry;

        do {
            System.out.println("Sending...");
            retry = false;
            Response response = target
                .request()
                .header("Authorization", "Basic " + System.getenv("OPENSEARCH_AUTH"))
                .post(Entity.entity(batch.toString(), MediaType.APPLICATION_JSON));

            if (response.getStatus() == 429) {
                retry = true;
                System.err.println("Queue busy, retrying");
                TimeUnit.SECONDS.sleep(1);
            } else if (response.getStatus() != 200 && response.getStatus() != 201) {
                System.err.println(response.readEntity(String.class));
                throw new IllegalStateException("Error posting to OpenSearch");
            }

        } while (retry);
        System.out.println("Done.");
    }

    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        Map<String, String> envVars = System.getenv();

        String url = envVars.get("CONFIG_JDBC_URL");
        String user = envVars.get("CONFIG_JDBC_USERNAME");
        String pass = envVars.get("CONFIG_JDBC_PASSWORD");
        String driver = envVars.get("CONFIG_JDBC_CLASS");

        if (url == null || url.isEmpty()
            || user == null || user.isEmpty()
            || pass == null || pass.isEmpty())
            throw new IllegalStateException("You need to set the CONFIG_JDBC_ environment variables!");

        if (driver != null && !driver.isEmpty())
            Class.forName(driver);

        Properties props = new Properties();

        props.setProperty("user", user);
        props.setProperty("password", pass);

        return DriverManager.getConnection(url, props);
    }
}
