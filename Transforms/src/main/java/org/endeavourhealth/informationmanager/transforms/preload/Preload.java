package org.endeavourhealth.informationmanager.transforms.preload;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.apache.http.HttpStatus;
import org.endeavourhealth.imapi.dataaccess.databases.IMDB;
import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.logic.reasoner.DomainResolver;
import org.endeavourhealth.imapi.logic.reasoner.RangeInheritor;
import org.endeavourhealth.imapi.logic.reasoner.SetBinder;
import org.endeavourhealth.imapi.logic.reasoner.SetMemberGenerator;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.ImportType;
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
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Stream;

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
    TTImportConfig cfg = getImports(args);
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
    LOG.info("Deleting temp folder.a()..");
    clearTempFolder(TTBulkFiler.getDataPath());

    LOG.info("Starting import...");
    importData(cfg, graphdbCommand);
  }

  private static void clearTempFolder(String tempUrl) throws IOException {
    Path path = Paths.get(tempUrl);
    try (Stream<Path> paths = Files.walk(path)) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (NoSuchFileException e) {
      LOG.info("No files found in temp folder");
    }
    String folderHome = tempUrl.substring(0,tempUrl.lastIndexOf("\\"));
    if (folderHome.endsWith(":")) folderHome = folderHome+"\\";
    String folderName = tempUrl.substring(tempUrl.lastIndexOf("\\") + 1);
    File newDirectory = new File(folderHome,folderName);
    if (!newDirectory.exists()) {
      newDirectory.mkdir();
    }
  }

  private static TTImportConfig getImports(String[] args) throws ImportException, IOException {
    String folder = null;
    String imports = null;
    for (String argument : args) {
      if (argument.startsWith("source=")) {
        folder = (argument.split("=")[1]);
      }
      if (argument.toLowerCase().split("=")[0].equals("graphs")) {
        imports = argument.split("=")[1].trim();
      }
    }
    if (folder == null) throw new ImportException(" no sources folder set");
    if (imports == null) imports = "endeavour.json";
    TTImportConfig config = new ObjectMapper().readValue(new File(folder + "/PreloadImports/" + imports), TTImportConfig.class);
    config.setFolder(folder);
    return config;

  }

  public static List<ImportType> canBulk = List.of(ImportType.CORE, ImportType.SNOMED, ImportType.ENCOUNTERS, ImportType.QUERY, ImportType.IM1, ImportType.FHIR, ImportType.EMIS, ImportType.TPP, ImportType.OPCS4, ImportType.ICD10, ImportType.VISION, ImportType.ODS, ImportType.BARTS_CERNER, ImportType.NHS_TFC);

  private static void importData(TTImportConfig cfg, String graphdb) throws Exception {
    LOG.info("Validating config...");
    validateGraphConfig(cfg.getFolder());

    LOG.info("Validating data files...");
    TTImportByType importer = new Importer();
    for (ImportType i : cfg.getImports()) {
      importer.validateByType(i, cfg.getFolder());
    }

    LOG.info("Importing files...");
    if (!cfg.isSkipBulk()) {
      for (ImportType i : cfg.getImports()) {
        if (canBulk.contains(i)) {
          importer.importByType(i, cfg);
        }
      }
      LOG.info("Generating closure...");
      TCGenerator closureGenerator = TTFilerFactory.getClosureGenerator();
      closureGenerator.generateClosure(TTBulkFiler.getDataPath(), cfg.isSecure());
      LOG.info("Preparing bulk filer...");
      TTBulkFiler.createRepository();
      startGraph(graphdb);
    }

    try (IMDB conn = IMDB.getConnection()) {
      new RangeInheritor().inheritRanges(conn, Graph.IM);
    }

    LOG.info("expanding value sets");
    new SetMemberGenerator().generateAllSetMembers(Graph.IM);
    new SetBinder().bindSets(Graph.IM);
    LOG.info("Filing into live graph");
    TTFilerFactory.setBulk(false);
    TTFilerFactory.setTransactional(true);

    for (ImportType i : cfg.getImports()) {
      if (!canBulk.contains(i)) {
        importer.importByType(i, cfg);
      }
    }

    try (TTImport deltaImporter = new DeltaImporter()) {
      deltaImporter.importData(cfg);
    }
    LOG.info("adding missing properties into concept domains");
    new DomainResolver().updateDomains(Graph.IM);

    LOG.info("Finished - {}" ,new Date());

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
