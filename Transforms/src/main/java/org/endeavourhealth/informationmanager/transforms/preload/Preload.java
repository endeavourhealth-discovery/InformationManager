package org.endeavourhealth.informationmanager.transforms.preload;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.logic.reasoner.DomainResolver;
import org.endeavourhealth.imapi.logic.reasoner.RangeInheritor;
import org.endeavourhealth.imapi.logic.reasoner.SetBinder;
import org.endeavourhealth.imapi.logic.reasoner.SetMemberGenerator;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportByType;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;
import org.endeavourhealth.informationmanager.transforms.sources.DeltaImporter;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;
import org.endeavourhealth.informationmanager.transforms.sources.Importer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

public class Preload {
  private static final Logger LOG = LoggerFactory.getLogger(Preload.class);

  public static void main(String[] args) throws Exception {
    LOG.info("Running Preload...");
    if (args.length < 4) {
      LOG.error("Insufficient parameters supplied:");
      LOG.error("source={sourcefolder} preload={foldercontaing preload} " + "temp= {folder for temporary data} and privacy= {0 public, 1 private publisher 2 private for authoring} [cmd={graphdbExecutable}]");
      System.exit(-1);
    }
    LOG.info("Configuring...");
    TTImportConfig cfg = getGraphsToLoad(args);
    TTBulkFiler.setConfigTTl(cfg.getFolder());
    TTFilerFactory.setBulk(true);
    String graphdbCommand = null;

    for (String arg : args) {
      if (arg.startsWith("preload")) TTBulkFiler.setPreload(arg.split("=")[1]);
      else if (arg.startsWith("temp=")) TTBulkFiler.setDataPath(arg.split("=")[1]);
      else if (arg.startsWith("privacy")) TTBulkFiler.setPrivacyLevel(Integer.parseInt(arg.split("=")[1]));
      else if (arg.startsWith("cmd")) graphdbCommand = arg.split("=")[1];
      else if (arg.contains("test=")) ImportApp.setTestDirectory(arg.substring(arg.lastIndexOf("=") + 1));
      else if (arg.toLowerCase().contains("skipbulk")) cfg.setSkipBulk(true);
    }

    LOG.info("Starting import...");
    importData(cfg, graphdbCommand);
  }

  private static TTImportConfig getGraphsToLoad(String[] args) throws ImportException, IOException {
    String folder = null;
    String graphs = null;
    for (String argument : args) {
      if (argument.startsWith("source=")) {
        folder = (argument.split("=")[1]);
      }
      if (argument.toLowerCase().split("=")[0].equals("graphs")) {
        graphs = argument.split("=")[1].trim();
      }
    }
    if (folder == null) throw new ImportException(" no sources folder set");
    if (graphs == null) graphs = "endeavour.json";
    TTImportConfig config = new ObjectMapper().readValue(new File(folder + "/PreloadGraphs/" + graphs), TTImportConfig.class);
    config.setFolder(folder);
    return config;

  }


  public static List<String> canBulk = List.of(GRAPH.DISCOVERY, SNOMED.NAMESPACE, GRAPH.ENCOUNTERS, GRAPH.QUERY, GRAPH.IM1, GRAPH.FHIR, GRAPH.EMIS, GRAPH.TPP, GRAPH.OPCS4, GRAPH.ICD10, GRAPH.VISION, GRAPH.ODS, GRAPH.BARTS_CERNER, GRAPH.NHS_TFC, GRAPH.BNF);


  private static void importData(TTImportConfig cfg, String graphdb) throws Exception {
    LOG.info("Validating config...");
    validateGraphConfig(cfg.getFolder());


    LOG.info("Validating data files...");
    TTImportByType importer = new Importer();
    for (String graph : cfg.getGraph()) {
      importer.validateByType(graph, cfg.getFolder());
    }
    LOG.info("Importing files...");
    if (!cfg.isSkipBulk()) {
      for (String graph : cfg.getGraph()) {
        if (canBulk.contains(graph)) {
          importer.importByType(graph, cfg);
        }
      }
      LOG.info("Generating closure...");
      TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
      closureGenerator.generateClosure(TTBulkFiler.getDataPath(), cfg.isSecure());
      LOG.info("Preparing bulk filer...");
      TTBulkFiler.createRepository();
      startGraph(graphdb);
    }
    new RangeInheritor().inheritRanges(null);
    LOG.info("expanding value sets");
    new SetMemberGenerator().generateAllSetMembers();
    new SetBinder().bindSets();
    LOG.info("Filing into live graph");
    TTFilerFactory.setBulk(false);
    TTFilerFactory.setTransactional(true);
    for (String graph : cfg.getGraph()) {
      if (!canBulk.contains(graph)) {
        importer.importByType(graph, cfg);
      }
    }
    try (TTImport deltaImporter = new DeltaImporter()) {
      deltaImporter.importData(cfg);
    }
    LOG.info("adding missing properties into concept domains");
    new DomainResolver().updateDomains();

    LOG.info("Finished - " + (new Date()));

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
        if (line.equalsIgnoreCase("ok")) ok = true;
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
        retries--;
      }

      if (!alive) {
        LOG.error("Server failed to start in time, exiting...");
        System.exit(-1);
      }
    }
  }

  private static boolean pingGraphServer() {
    try (Client client = ClientBuilder.newClient()) {
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
}
