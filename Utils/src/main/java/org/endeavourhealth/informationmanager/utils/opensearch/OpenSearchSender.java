package org.endeavourhealth.informationmanager.utils.opensearch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.endeavourhealth.imapi.model.search.EntityDocument;
import org.endeavourhealth.imapi.model.search.SearchTermCode;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class OpenSearchSender {
    private static final Logger LOG = LoggerFactory.getLogger(OpenSearchSender.class);
    private static final int BULKSIZE = 500000;
    private final Client client = ClientBuilder.newClient();
    private WebTarget target;
    private final ObjectMapper om = new ObjectMapper();
    // Env vars
    private final String osUrl = System.getenv("OPENSEARCH_URL");
    private final String osAuth = System.getenv("OPENSEARCH_AUTH");
    private final String server = System.getenv("GRAPH_SERVER");
    private final String repoId = System.getenv("GRAPH_REPO");
    private final String index = System.getenv("OPENSEARCH_INDEX");
    private final HTTPRepository repo = new HTTPRepository(server, repoId);
    private Map<String,EntityDocument> docs;
    private String cache;



    public void execute(boolean update) throws IOException, InterruptedException {
        om.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        om.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        checkEnvs();
        checkIndexExists();

        int maxId=0;
        if (!update)
            maxId = getMaxDocument();

        continueUpload(maxId);
    }


    private void continueUpload(int maxId) throws IOException, InterruptedException {
        target = client.target(osUrl).path("_bulk");
        docs= new HashMap<>();
        if (maxId>0&&(cache!=null)){
            getIriCache(docs);
        }
        else {

            String sql = new StringJoiner(System.lineSeparator())
              .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
              .add("PREFIX im: <http://endhealth.info/im#>")
              .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
              .add("select ?iri")
              .add("where {")
              .add("  ?iri rdfs:label ?name.")
              .add("  filter(isIri(?iri))")
              .add("}")
              .add("order by ?iri").toString();
            try (RepositoryConnection conn = repo.getConnection()) {
                LOG.info("Fetching entity iris  ...");
                TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
                EntityDocument blob = null;
                String lastIri = null;
                try (TupleQueryResult qr = tupleQuery.evaluate()) {
                    while (qr.hasNext()) {
                        BindingSet rs = qr.next();
                        String iri = rs.getValue("iri").stringValue();
                        if (!iri.equals(lastIri)) {
                            blob = new EntityDocument();
                            blob.setIri(iri);
                            docs.put(iri, blob);
                            lastIri = iri;
                        }
                    }
                }
            }
        }
        if (cache!=null){
            saveIriCache(docs);
        }
        //assign docids in order of keys
        int docid=0;
        for (Map.Entry<String,EntityDocument> entry:docs.entrySet()) {
            docid++;
            entry.getValue().setId(docid);
        }
        int mapNumber = 0;
        int member = 0;
        int batchSize = 5000;
        Iterator<Map.Entry<String, EntityDocument>> mapIterator = docs.entrySet().iterator();
        while (mapIterator.hasNext()) {
            Map.Entry<String, EntityDocument> entry = mapIterator.next();
            mapNumber++;
            if (mapNumber > maxId) {
                List<String> iriBatch = new ArrayList<>();
                iriBatch.add("<"+ entry.getKey()+">");
                while (member < batchSize) {
                    member++;
                    mapNumber++;
                    if (mapIterator.hasNext()) {
                        entry = mapIterator.next();
                        iriBatch.add("<" + entry.getKey() + ">");
                    }
                }
                if (!iriBatch.isEmpty()) {
                    String inList = String.join(",", iriBatch);
                    Set<EntityDocument> batch= getEntityBatch(inList,mapNumber);
                    index(batch);
                    member=0;
                }
            }
        }
    }

    private void saveIriCache(Map<String, EntityDocument> docs) throws IOException {
        try (FileWriter wr= new FileWriter(cache)){
            for (String iri:docs.keySet()){
                wr.write(iri+"\n");
            }
        }
    }

    private void getIriCache(Map<String, EntityDocument> docs) {
        try (BufferedReader reader = new BufferedReader(new FileReader(cache))) {
            String line = reader.readLine();
            while (line != null && !line.isEmpty()) {
                EntityDocument blob = new EntityDocument();
                blob.setIri(line);
                docs.put(line, blob);
                line= reader.readLine();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Set<EntityDocument> getEntityBatch(String inList, int mapNumber) {
        Set<EntityDocument> batch = new HashSet<>();
        getCore(batch,inList,mapNumber);
        getTermCodes(batch,inList,mapNumber);
        getIsas(batch,inList,mapNumber);
        return batch;
    }

    private String getBatchCoreSql(String inList){
        return new StringJoiner(System.lineSeparator())
          .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
          .add("PREFIX im: <http://endhealth.info/im#>")
          .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
          .add("select ?iri ?name ?status ?statusName ?code ?scheme ?schemeName ?type ?typeName ?weighting")
          .add("?extraType ?extraTypeName")
          .add("where {")
          .add("  graph ?scheme {")
          .add("    ?iri rdf:type ?type.")
          .add("      filter (?iri in ("+ inList+") )")
          .add("    ?iri rdfs:label ?name.}")
         .add("    Optional {?iri im:isA ?extraType.")
          .add("                         ?extraType rdfs:label ?extraTypeName.")
          .add("            filter (?extraType in (im:dataModelProperty, im:DataModelEntity))}")
          .add("    Optional {?type rdfs:label ?typeName}")
          .add("    Optional {?iri im:status ?status.")
          .add("    Optional {?status rdfs:label ?statusName} }")
          .add("    Optional {?scheme rdfs:label ?schemeName }")
          .add("    Optional {?iri im:code ?code.}")
          .add("    Optional {?iri im:weighting ?weighting.}")
          .add("}").toString();
    }
    private String getBatchTermsSql(String inList){
        return new StringJoiner(System.lineSeparator())
          .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
          .add("PREFIX im: <http://endhealth.info/im#>")
          .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
          .add("select ?iri ?termCode ?synonym ?termCodeStatus")
          .add("where {")
          .add("?iri im:hasTermCode ?tc.")
          .add("      filter (?iri in ("+ inList+") )")
          .add("       Optional {?tc im:code ?termCode}")
          .add("       Optional  {?tc rdfs:label ?synonym}")
          .add("       Optional  {?tc im:status ?termCodeStatus}")
          .add("}").toString();
    }

    private String getBatchIsaSql(String inList){
        return new StringJoiner(System.lineSeparator())
          .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
          .add("PREFIX im: <http://endhealth.info/im#>")
          .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
          .add("select ?iri ?superType")
          .add("where {")
          .add(" ?iri im:isA ?superType.")
          .add("      filter (?iri in ("+ inList+") )")
          .add(" ?superType im:status im:Active.")
          .add("}").toString();
    }



    private void getIsas(Set<EntityDocument> batch, String inList, int mapNumber) {
        String sql= getBatchIsaSql(inList);
        try (RepositoryConnection conn = repo.getConnection()) {
            LOG.info(" Fetching  supertypes (isas) up to  entity number " + mapNumber + " ...");
            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {
                while (qr.hasNext()) {
                    BindingSet rs = qr.next();
                    String iri = rs.getValue("iri").stringValue();
                    EntityDocument blob = docs.get(iri);
                    blob.getIsA().add(TTIriRef.iri(rs.getValue("superType").stringValue()));
                    }

                }
        }

    }

    private void getTermCodes(Set<EntityDocument> batch, String inList, int mapNumber) {
        String sql= getBatchTermsSql(inList);
        try (RepositoryConnection conn = repo.getConnection()) {
            LOG.info(" Fetching  iri term codes up to  entity number " + mapNumber + " ...");
            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {
                while (qr.hasNext()) {
                    BindingSet rs = qr.next();
                    String iri = rs.getValue("iri").stringValue();
                    EntityDocument blob = docs.get(iri);
                    String termCode = null;
                    String synonym = null;
                    TTIriRef status = null;
                    if (rs.getValue("synonym") != null)
                        synonym = rs.getValue("synonym").stringValue().toLowerCase();
                    if (rs.getValue("termCode") != null)
                        termCode = rs.getValue("termCode").stringValue();
                    if (rs.getValue("termCodeStatus") != null)
                        status = TTIriRef.iri(rs.getValue("termCodeStatus").stringValue());
                    if (synonym != null) {
                        SearchTermCode tc = getTermCode(blob, synonym);
                        if (tc == null) {
                            blob.addTermCode(synonym, termCode, status);
                            addKey(blob,synonym);
                        }
                        else if (termCode != null) {
                            tc.setCode(termCode);
                        }
                    }
                    else if (termCode != null) {
                        SearchTermCode tc = getTermCodeFromCode(blob, termCode);
                        if (tc == null)
                            blob.addTermCode(null, termCode, status);
                    }

                }
            }
        }
    }

    private void getCore(Set<EntityDocument> batch,String inList,int mapNumber) {

    String sql= getBatchCoreSql(inList);

        try (RepositoryConnection conn = repo.getConnection()) {
            LOG.info(" Fetching  iris up to  entity number " + mapNumber + " ...");
            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {
                while (qr.hasNext()) {
                    BindingSet rs = qr.next();
                    String iri = rs.getValue("iri").stringValue();
                    EntityDocument blob = docs.get(iri);
                    batch.add(blob);
                    String name = rs.getValue("name").stringValue();
                    blob.setName(name);
                   // if (name.contains(" "))
                       // if (name.split(" ")[0].length() < 3)
                         //   blob.addKey(name.substring(0, name.indexOf(" ")).toLowerCase());
                    String code = null;
                    if (rs.getValue("code") != null) {
                        code = rs.getValue("code").stringValue();
                        blob.setCode(code);
                    }

                    TTIriRef scheme = TTIriRef.iri(rs.getValue("scheme").stringValue());
                    ;
                    if (rs.getValue("schemeName") != null)
                        scheme.setName(rs.getValue("schemeName").stringValue());
                    blob.setScheme(scheme);

                    if (rs.getValue("status") != null) {
                        TTIriRef status = TTIriRef.iri(rs.getValue("status").stringValue());
                        if (rs.getValue("statusName") != null)
                            status.setName(rs.getValue("statusName").stringValue());
                        blob.setStatus(status);
                    }
                    TTIriRef type = TTIriRef.iri(rs.getValue("type").stringValue());
                    if (rs.getValue("typeName") != null)
                        type.setName(rs.getValue("typeName").stringValue());
                    blob.addType(type);
                    TTIriRef extraType = null;
                    if (rs.getValue("extraType") != null) {
                        extraType = TTIriRef.iri(rs.getValue("extraType").stringValue());
                        extraType.setName(rs.getValue("extraTypeName").stringValue());
                        blob.addType(extraType);
                        if (extraType.equals(TTIriRef.iri(IM.NAMESPACE + "DataModelEntity"))) {
                            int weighting = 2000000;
                            blob.setWeighting(weighting);
                        }
                    }
                    if (rs.getValue("weighting") != null) {
                        blob.setWeighting(Integer.parseInt(rs.getValue("weighting").stringValue()));
                    }

                    blob.addTermCode(name, null, null);
                    addKey(blob,name);
                }

            }catch (Exception e){
                System.err.println("Bad Query \n"+sql);
            }
        }
    }

    private void  addKey(EntityDocument blob, String key) {
        key= key.split(" ")[0];
        if (key.length()>1) {
            if (key.length() > 20)
                key = key.substring(0, 20);
            key = key.toLowerCase();
            List<String> deletes = new ArrayList<>();
            boolean skip = false;
            if (blob.getKey() != null) {
                for (String already : blob.getKey()) {
                    if (key.startsWith(already))
                        deletes.add(already);
                    if (already.startsWith(key))
                        skip = true;
                }
                if (!deletes.isEmpty())
                    deletes.forEach(d -> blob.getKey().remove(d));
            }
            if (!skip)
                blob.addKey(key);
        }
    }

    private boolean hasTerm(EntityDocument blob,String term){
        for (SearchTermCode tc:blob.getTermCode()){
            if (tc.getTerm()!=null)
             if (tc.getTerm().equals(term))
                return true;
        }
        return false;
    }

    private SearchTermCode getTermCode(EntityDocument blob,String term){
        for (SearchTermCode tc:blob.getTermCode()){
            if (tc.getTerm()!=null)
                if (tc.getTerm().equals(term))
                    return tc;
        }
        return null;
    }

    private SearchTermCode getTermCodeFromCode(EntityDocument blob,String code){
        for (SearchTermCode tc:blob.getTermCode()){
            if (tc.getCode()!=null)
                if (tc.getCode().equals(code))
                    return tc;
        }
        return null;
    }


    private void checkEnvs() {
        boolean missingEnvs = false;

        for(String env : Arrays.asList("OPENSEARCH_AUTH", "OPENSEARCH_URL","OPENSEARCH_INDEX","GRAPH_SERVER", "GRAPH_REPO")) {
            String envData = System.getenv(env);
            if (envData == null || envData.isEmpty()) {
                LOG.error("Environment variable {} not set", env);
                missingEnvs = true;
            }
        }

        if (missingEnvs)
            System.exit(-1);
    }

    private void index(Set<EntityDocument> docs) throws JsonProcessingException, InterruptedException {
        StringJoiner batch = new StringJoiner("\n");
        for (EntityDocument doc : docs) {
            batch.add("{ \"index\" : { \"_index\": \"" + index + "\", \"_id\" : \"" + doc.getId() + "\" } }");
            batch.add(om.writeValueAsString(doc));
        }
        batch.add("\n");
       upload(batch);
    }

    private void upload(StringJoiner batch) throws InterruptedException {
         boolean retry;
        int retrySleep = 5;
        boolean done= false;
        int tries=0;
        while (!done &&tries<5) {
            try {
                tries++;
                do {
                    LOG.info("Sending batch to Open search ...");
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
                done = true;
            } catch (Exception e){
                System.err.println(e.getMessage());
            }
        }
        if (tries==5)
            throw new InterruptedException("Connecitivity problem to open search");
    }

    private int getMaxDocument() throws IOException {
        target = client.target(osUrl).path(index + "/_search");

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
            if (responseData.contains("index_not_found_exception")) {
                LOG.info("Index not found, starting from zero");
                return 0;
            } else {
                LOG.error(responseData);
                throw new IllegalStateException("Error getting max document id from OpenSearch");
            }
        } else {
            String responseData = response.readEntity(String.class);
            JsonNode root = om.readTree(responseData);
            int maxId = root.get("aggregations").get("max_id").get("value").asInt();
            if (maxId > 0)
                LOG.info("Continuing from {}", maxId);
            return maxId;
        }
    }

    private void checkIndexExists() throws IOException {

        target = client.target(osUrl).path(index);

        Response response = target
          .request()
          .header("Authorization", "Basic " + osAuth)
          .head();

        if (response.getStatus() != 200) {
            LOG.info(index + " does not exist - creating index and default mappings");
            target = client.target(osUrl).path(index);
            String mappings = "{\n" +
              "  \"mappings\": {\n" +
              "    \"properties\": {\n" +
              "      \"scheme.@id\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"entityType.@id\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"status.@id\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"isA.@id\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"code\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"iri\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      },\n" +
              "      \"key\": {\n" +
              "        \"type\": \"keyword\"\n" +
              "      }\n" +
              "    }\n" +
              "  }\n" +
              "}";
            response = target
              .request()
              .header("Authorization", "Basic " + osAuth)
              .put(Entity.entity(mappings , MediaType.APPLICATION_JSON));
            System.out.println(response.getStatus());
        }
    }

    public String getCache() {
        return cache;
    }

    public OpenSearchSender setCache(String cache) {
        this.cache = cache;
        return this;
    }
}
