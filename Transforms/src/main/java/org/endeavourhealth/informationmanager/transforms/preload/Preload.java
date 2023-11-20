package org.endeavourhealth.informationmanager.transforms.preload;

import org.apache.http.HttpStatus;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.rdf4j.LuceneIndexer;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.logic.reasoner.RangeInheritor;
import org.endeavourhealth.imapi.logic.reasoner.SetExpander;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.QR;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.endeavourhealth.informationmanager.transforms.sources.DeltaImporter;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;

public class Preload {
    private static final Logger LOG = LoggerFactory.getLogger(Preload.class);

    public static void main(String[] args) throws Exception {
        boolean skipBulk= false;
        LOG.info("Running Preload...");
        if (args.length < 4) {
            LOG.error("Insufficient parameters supplied:");
            LOG.error("source={sourcefolder} preload={foldercontaing preload} " +
                "temp= {folder for temporary data} and privacy= {0 public, 1 private publisher 2 private for authoring} [cmd={graphdbExecutable}]");
            System.exit(-1);
        }
        TTFilerFactory.setBulk(true);
        String graphdbCommand = null;

        LOG.info("Configuring...");
        TTImportConfig cfg = new TTImportConfig();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith("source=")) {
                cfg.setFolder(arg.split("=")[1]);
                TTBulkFiler.setConfigTTl(arg.split("=")[1]);
            } else if (arg.startsWith("preload"))
                TTBulkFiler.setPreload(arg.split("=")[1]);
            else if (arg.startsWith("temp="))
                TTBulkFiler.setDataPath(arg.split("=")[1]);
            else if (arg.startsWith("privacy"))
                TTBulkFiler.setPrivacyLevel(Integer.parseInt(arg.split("=")[1]));
            else if (arg.startsWith("cmd"))
                graphdbCommand = arg.split("=")[1];
            else if (args[i].contains("test="))
                ImportApp.testDirectory = args[i].substring(args[i].lastIndexOf("=") + 1);
            else if (args[i].toLowerCase().contains("skipbulk"))
                cfg.setSkipBulk(true);
            else
                LOG.error("Unknown parameter " + args[i]);
        }

        LOG.info("Starting import...");
        importData(cfg, graphdbCommand);
    }

    private static void importData(TTImportConfig cfg, String graphdb) throws Exception {
        LOG.info("Validating config...");
        validateGraphConfig(cfg.getFolder());

        LOG.info("Validating data files...");
        TTImportByType importer = new Importer()
            .validateByType(IM.GRAPH_DISCOVERY, cfg.getFolder())
            .validateByType(SNOMED.GRAPH_SNOMED, cfg.getFolder())
            .validateByType(IM.GRAPH_QUERY, cfg.getFolder())
            .validateByType(IM.GRAPH_ENCOUNTERS, cfg.getFolder())
            .validateByType(IM.GRAPH_EMIS, cfg.getFolder())
            .validateByType(IM.GRAPH_TPP, cfg.getFolder())
            .validateByType(IM.GRAPH_OPCS4, cfg.getFolder())
            .validateByType(IM.GRAPH_ICD10, cfg.getFolder())
            .validateByType(IM.GRAPH_VISION, cfg.getFolder())
            .validateByType(IM.GRAPH_KINGS_APEX, cfg.getFolder())
            .validateByType(IM.GRAPH_KINGS_WINPATH, cfg.getFolder())
            .validateByType(IM.GRAPH_BARTS_CERNER, cfg.getFolder())
            .validateByType(IM.GRAPH_ODS, cfg.getFolder())
            .validateByType(IM.GRAPH_NHS_TFC, cfg.getFolder())
            .validateByType(IM.GRAPH_CEG_QUERY, cfg.getFolder())
            .validateByType(IM.GRAPH_IM1, cfg.getFolder())
          //.validateByType(IM.GRAPH_CPRD_MED, cfg.getFolder())
            .validateByType(TTIriRef.iri(QR.NAMESPACE),cfg.getFolder());
        if (!cfg.isSkipBulk()) {

            LOG.info("Importing files...");
            importer.importByType(IM.GRAPH_DISCOVERY, cfg);
            importer.importByType(SNOMED.GRAPH_SNOMED, cfg);
            importer.importByType(IM.GRAPH_QUERY, cfg);
            importer.importByType(IM.GRAPH_ENCOUNTERS, cfg);
            importer.importByType(IM.GRAPH_EMIS, cfg);
            importer.importByType(IM.GRAPH_TPP, cfg);
            importer.importByType(IM.GRAPH_OPCS4, cfg);
            importer.importByType(IM.GRAPH_ICD10, cfg);
            importer.importByType(IM.GRAPH_VISION, cfg);
            importer.importByType(IM.GRAPH_BARTS_CERNER, cfg);
            importer.importByType(IM.GRAPH_ODS, cfg);
            importer.importByType(IM.GRAPH_NHS_TFC, cfg);
            importer.importByType(IM.GRAPH_IM1, cfg);
            //importer.importByType(IM.GRAPH_CPRD_MED, cfg);


            LOG.info("Generating closure...");
            TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
            closureGenerator.generateClosure(TTBulkFiler.getDataPath(), cfg.isSecure());

            LOG.info("Preparing bulk filer...");
            TTBulkFiler.createRepository();
            startGraph(graphdb);
        }



        LOG.info("Filing into live graph");
        TTFilerFactory.setBulk(false);
        TTFilerFactory.setTransactional(true);
        importer.importByType(IM.GRAPH_KINGS_APEX, cfg);
        importer.importByType(IM.GRAPH_KINGS_WINPATH, cfg);
        importer.importByType(IM.GRAPH_CEG_QUERY, cfg);
        importer.importByType(TTIriRef.iri(QR.NAMESPACE),cfg);
        try ( TTImport deltaImporter = new DeltaImporter()) {
            deltaImporter.importData(cfg);
        }
        new RangeInheritor().inheritRanges(null);

        LOG.info("expanding value sets");
        new SetExpander().expandAllSets();
        LOG.info("Finished - " + (new Date()));


        LOG.info("Building text index");
        new LuceneIndexer().buildIndexes();
        System.exit(0);
    }

    public static void validateGraphConfig(String inFolder) {
        ImportUtils.validateFiles(inFolder, new String[]{".*config.ttl"});
    }

    private static void startGraph(String graphdb) throws IOException, InterruptedException {

        if (graphdb == null || graphdb.isEmpty()) {
            Scanner scanner = new Scanner(System.in);
            boolean ok = false;
            while (!ok) {
                LOG.info("");
                LOG.error("Please start graph db in the usual manner and enter 'OK' when done : ");
                String line = scanner.nextLine();
                if (line.equalsIgnoreCase("ok"))
                    ok = true;
            }
        } else {
            LOG.info("Starting graph db....");
            Runtime.getRuntime().exec(new String[]{graphdb});
            LOG.info("Waiting for startup....");

            int retries = 20;
            boolean alive = pingGraphServer();
            while (!alive && retries > 0) {
                Thread.sleep(1000);
                LOG.info("Pinging server {} retries remaining....", retries);
                alive = pingGraphServer();
                retries --;
            }

            if (!alive) {
                LOG.error("Server failed to start in time, exiting...");
                System.exit(-1);
            }
        }
    }

    private static boolean pingGraphServer() {
        Client client = ClientBuilder.newClient();
        client.property("jersey.config.client.connectTimeout", 20000);
        client.property("jersey.config.client.readTimeout", 20000);

        WebTarget resource = client.target("http://localhost:7200/protocol");

        Invocation.Builder request = resource.request();
        request.accept(MediaType.APPLICATION_JSON);

        try {
            Response response = request.get();
            return response.getStatus() == HttpStatus.SC_OK;
        } catch (Exception e) {
            return false;
        }
    }
}
