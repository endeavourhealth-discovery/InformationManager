package org.endeavourhealth.informationmanager.trud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.Response;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TrudUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(TrudUpdater.class);

  private static final List<TrudFeed> feeds = Arrays.asList(
    new TrudFeed("CLINICAL", "101"),
    new TrudFeed("DRUG", "105"),
    new TrudFeed("MAPS", "9"),
    new TrudFeed("ICD10", "258"),
    new TrudFeed("OPCS4", "119"),
    new TrudFeed("HISTORY", "276"),
    new TrudFeed("ODSTOOLS", "343", TrudUpdater::processODSTools),
    new TrudFeed("ODS", "341", TrudUpdater::processODS),
    new TrudFeed("PRIMARY", "659"),
    new TrudFeed("BNF", "25", TrudUpdater::processBNF),
    new TrudFeed("DMD", "24"),
    new TrudFeed("DMDTOOLS", "239", TrudUpdater::transformDMDandBNF)
  );

  private static final int BUFFER_SIZE = 8192;
  private static String APIKey;
  private static String WorkingDir;
  private static ObjectMapper mapper;
  private static ObjectNode localVersions;

  public static void main(String[] argv) {
    if (argv.length != 2) {
      System.err.println("You must provide an API key and working directory as parameters");
      System.err.println("TrudUpdater <api key> <working dir>");
      System.exit(-1);
    }

    APIKey = argv[0];
    WorkingDir = argv[1];

    if (!WorkingDir.endsWith("\\") && !WorkingDir.endsWith("/"))
      WorkingDir += "/";

    mapper = new ObjectMapper();

    LOG.info("Collecting version information...");
    try {
      getRemoteVersions();
      getLocalVersions();
      processFeeds();
      LOG.info("Finished");
    } catch (Exception e) {
      LOG.error("Error:");
      e.printStackTrace();
    }
  }

  private static void getRemoteVersions() throws IOException {
    try (Client client = ClientBuilder.newClient()) {
      ObjectMapper om = new ObjectMapper();
      for (TrudFeed feed : feeds) {
        LOG.info("Fetching remote version [{}]...", feed.getName());
        WebTarget target = client.target("https://isd.digital.nhs.uk").path("/trud3/api/v1/keys/" + APIKey + "/items/" + feed.getId() + "/releases?latest");
        Response response = target.request().get();
        String responseRaw = response.readEntity(String.class);
        if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
          LOG.error("Could not get remote version for {}", feed.getName());
          LOG.error(responseRaw);
          System.exit(-1);
        } else {
          JsonNode json = om.readTree(responseRaw);
          ArrayNode releases = (ArrayNode) json.get("releases");
          JsonNode latest = releases.get(0);
          feed.setRemoteVersion(latest.get("releaseDate").asText());
          feed.setDownload(latest.get("archiveFileUrl").asText());
        }
      }
    }
  }

  private static void getLocalVersions() throws IOException {
    try {
      localVersions = (ObjectNode) mapper.readTree(new File(WorkingDir + "versions.json"));
    } catch (FileNotFoundException e) {
      localVersions = JsonNodeFactory.instance.objectNode();
    }
  }

  private static void processFeeds() throws IOException, URISyntaxException {
    for (TrudFeed feed : feeds) {
      if (versionMismatch(feed) || !downloadExists(feed))
        downloadFeed(feed);

      if (!feedUnzipped(feed))
        unzipFeed(feed);

      feed.runPostProcess();
    }
  }

  private static boolean versionMismatch(TrudFeed feed) {
    if (localVersions.has(feed.getName()) && feed.getRemoteVersion().equals(localVersions.get(feed.getName()).asText())) {
      LOG.info("[" + feed.getName() + "] - CURRENT");
      return false;
    } else {
      LOG.warn("[" + feed.getName() + "] - UPDATED");
      feed.setUpdated(true);
      return true;
    }
  }

  private static boolean downloadExists(TrudFeed feed) {
    String zipFile = WorkingDir + feed.getName() + "_" + feed.getRemoteVersion() + ".zip";
    if (Files.notExists(Paths.get(zipFile))) {
      LOG.warn("Download missing [{}]", feed.getName());
      return false;
    } else {
      return true;
    }
  }

  private static void downloadFeed(TrudFeed feed) throws IOException, URISyntaxException {
    LOG.info("Downloading {}", feed.getName());

    if (feed.getUpdated() && localVersions.get(feed.getName()) != null) {
      String oldLocalZip = WorkingDir + "/" + feed.getName() + "_" + localVersions.get(feed.getName()).asText() + ".zip";
      LOG.info("Deleting previous archive [{}]...", oldLocalZip);
      if (!new File(oldLocalZip).delete())
        LOG.warn("Unable to delete archive!");

      String oldLocalFolder = WorkingDir + "/" + feed.getName() + "/" + localVersions.get(feed.getName()).asText();
      LOG.info("Deleting previous folder [{}]...", oldLocalFolder);
      if (!deleteDirectory(new File(oldLocalFolder)))
        LOG.warn("Unable to delete folder!");
    }

    // Does the zip exist?
    String zipFile = WorkingDir + feed.getName() + "_" + feed.getRemoteVersion() + ".zip";
    if (Files.notExists(Paths.get(zipFile))) {
      downloadFile(feed.getDownload(), zipFile);
    }
    updateLocalVersions(feed);
  }

  private static void downloadFile(String sourceUrl, String destination) throws IOException, URISyntaxException {
    URL url = new URI(sourceUrl).toURL();
    URLConnection con = url.openConnection();
    long contentLength = con.getContentLengthLong();
    LOG.info("Downloading {} - {} bytes", url, contentLength);
    try (InputStream inputStream = con.getInputStream();
         OutputStream outputStream = new FileOutputStream(destination + ".tmp")) {

      // Limiting byte written to file per loop
      byte[] buffer = new byte[BUFFER_SIZE];

      // Increments file size
      int length;
      long downloaded = 0;

      // Looping until server finishes
      while ((length = inputStream.read(buffer)) != -1) {
        // Writing data
        outputStream.write(buffer, 0, length);
        downloaded += length;
        System.out.print("Download Status: " + (downloaded * 100) / contentLength + "%\r");
      }
      System.out.println();
    }

    Path source = Paths.get(destination + ".tmp");
    Path dest = Paths.get(destination);
    Files.move(source, dest, StandardCopyOption.REPLACE_EXISTING);
  }

  private static void updateLocalVersions(TrudFeed feed) throws IOException {
    localVersions.put(feed.getName(), feed.getRemoteVersion());
    mapper.writerWithDefaultPrettyPrinter().writeValue(new File(WorkingDir + "versions.json"), localVersions);
  }

  private static boolean feedUnzipped(TrudFeed feed) {
    String extractDir = WorkingDir + "/" + feed.getName() + "/" + feed.getRemoteVersion();
    return Files.exists(Paths.get(extractDir));
  }

  private static void unzipFeed(TrudFeed feed) throws IOException {
    String extractDir = WorkingDir + "/" + feed.getName() + "/" + feed.getRemoteVersion();
    String zipFile = WorkingDir + feed.getName() + "_" + feed.getRemoteVersion() + ".zip";
    unzipArchive(zipFile, extractDir);
  }

  private static void unzipArchive(String zipFile, String destination) throws IOException {
    LOG.info("Unzipping {}", zipFile);
    File destDir = new File(destination);
    byte[] buffer = new byte[BUFFER_SIZE];
    try (ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile))) {
      ZipEntry zipEntry = zis.getNextEntry();
      while (zipEntry != null) {
        File newFile = newFile(destDir, zipEntry);
        if (zipEntry.isDirectory()) {
          if (!newFile.isDirectory() && !newFile.mkdirs()) {
            throw new IOException("Failed to create directory " + newFile);
          }
        } else {
          // fix for Windows-created archives
          File parent = newFile.getParentFile();
          if (!parent.isDirectory() && !parent.mkdirs()) {
            throw new IOException("Failed to create directory " + parent);
          }

          writeFileContent(buffer, zis, zipEntry, newFile);
        }
        zipEntry = zis.getNextEntry();
      }
    }
  }

  private static void writeFileContent(byte[] buffer, ZipInputStream zis, ZipEntry zipEntry, File newFile) throws IOException {
    // write file content
    try (FileOutputStream fos = new FileOutputStream(newFile)) {
      int len;
      long read = 0;
      System.out.print("Extracting " + zipEntry.getName() + " - 0%\r");
      while ((len = zis.read(buffer)) > 0) {
        read += len;
        fos.write(buffer, 0, len);
        if (read % 1024 == 0)
          System.out.print("Extracting " + zipEntry.getName() + " - " + (read * 100 / zipEntry.getSize()) + "%\r");
      }
    }
    System.out.println("Extracted " + zipEntry.getName() + " - 100%\r");
  }

  private static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
    File destFile = new File(destinationDir, zipEntry.getName());

    String destDirPath = destinationDir.getCanonicalPath();
    String destFilePath = destFile.getCanonicalPath();

    if (!destFilePath.startsWith(destDirPath + File.separator)) {
      throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
    }

    return destFile;
  }

  private static boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  private static void processODSTools(TrudFeed feed) {
    String path = WorkingDir + feed.getName() + "/" + feed.getRemoteVersion();

    testAndUnzip(path, ".*/HSCOrgRefData_xslt.zip", ".*/transform.zip");

    if (testAndUnzip(path, ".*/HSCOrgRefData_xmltocsv.xslt", ".*/HSCOrgRefData_xslt.zip")) {
      // FIX XSLT TO BE CROSS PLATFORM!
      try {
        Path xsltFile = Path.of(path + "/HSCOrgRefData_xmltocsv.xslt");
        LOG.warn("Fixing XSLT file to be cross platform compatible");
        String xslt = Files.readString(xsltFile);
        xslt = xslt.replace("concat('file:/\\\\',$server-name,'/',$share-name,'/')", "concat('file:///',$server-name,'/',$share-name,'/')");
        Files.writeString(xsltFile, xslt);
      } catch (IOException e) {
        LOG.error("Error fixing XSLT");
      }
    }
  }

  private static void processODS(TrudFeed feed) {
    String path = WorkingDir + feed.getName() + "/" + feed.getRemoteVersion();

    if (Files.exists(Paths.get(path + "/Organisation_Details.csv"))) {
      LOG.info("Already transformed");
      return;
    }

    testAndUnzip(path, ".*/HSCOrgRefData_Full_.*.xml", ".*/fullfile.zip");

    try {
      LOG.info("Transforming XML to CSV...");
      Path fullData = findFileForId(path, ".*\\HSCOrgRefData_Full_.*.xml");
      String tools = WorkingDir + "ODSTOOLS/" + localVersions.get("ODSTOOLS").asText();

      execute(path,
        "java",
        "-jar",
        tools + "/saxon/Java/saxon9he.jar",
        "-t",
        "-s:" + fullData.getFileName(),
        "-xsl:" + tools + "/HSCOrgRefData_xmltocsv.xslt",
        "server-name=" + path);
    } catch (Exception e) {
      LOG.error("Error transforming ODS data", e);
    }
  }

  private static void transformDMDandBNF(TrudFeed feed) {
    String loaderPath = WorkingDir + feed.getName() + "/" + feed.getRemoteVersion() + "/dmd_extract_tool/";
    String bnfPath = WorkingDir + "BNF/" + localVersions.get("BNF").asText();
    String dmdPath = WorkingDir + "DMD/" + localVersions.get("DMD").asText();

    if (Files.exists(Paths.get(dmdPath + "/f_vmpp_ContentType.csv"))) {
      LOG.info("Already transformed");
      return;
    }

    testAndUnzip(loaderPath, ".*/dmdDataLoader", ".*/dmdDataLoader.zip");

    transformFile(loaderPath, bnfPath, ".*/f_bnf1_0.*.xml",
      "f_bnf_Amp",
      "f_bnf_Vmp");

    transformFile(loaderPath, dmdPath, ".*/f_amp2_3.*.xml",
      "f_amp_AmpType",
      "f_amp_ApiType",
      "f_amp_LicRouteType",
      "f_amp_AppProdInfoType");

    transformFile(loaderPath, dmdPath, ".*/f_ampp2_3.*.xml",
      "f_ampp_AmppType",
      "f_ampp_PackInfoType",
      "f_ampp_ContentType",
      "f_ampp_PrescInfoType",
      "f_ampp_PriceInfoType",
      "f_ampp_ReimbInfoType");

    transformFile(loaderPath, dmdPath, ".*/f_vtm2_3.*.xml",
      "f_vtm");

    transformFile(loaderPath, dmdPath, ".*/f_ingredient2_3.*.xml",
      "f_ingredient");

    transformFile(loaderPath, dmdPath, ".*/f_lookup2_3.*.xml",
      "f_lookup_CombPackIndInfoType",
      "f_lookup_CombProdIndInfoType",
      "f_lookup_BasisOfNameInfoType",
      "f_lookup_NamechangeReasonInfoType",
      "f_lookup_VirProdPresStatInfoType",
      "f_lookup_ControlDrugCatInfoType",
      "f_lookup_LicAuthInfoType",
      "f_lookup_UoMHistoryInfoType",
      "f_lookup_FormHistoryInfoType",
      "f_lookup_OntFormRouteInfoType",
      "f_lookup_RouteHistoryInfoType",
      "f_lookup_DtPayCatInfoType",
      "f_lookup_SupplierSupplierInfoType",
      "f_lookup_FlavourInfoType",
      "f_lookup_ColourInfoType",
      "f_lookup_BasisOfStrengthInfoType",
      "f_lookup_ReimbStatInfoType",
      "f_lookup_SpecContInfoType",
      "f_lookup_VirProdNoAvailInfoType",
      "f_lookup_DiscIndInfoType",
      "f_lookup_DfIndInfoType",
      "f_lookup_PriceBasisInfoType",
      "f_lookup_LegalCatInfoType",
      "f_lookup_AvailRestrictInfoType",
      "f_lookup_LicAuthChgRsnInfoType",
      "f_lookup_DNDInfoType");

    transformFile(loaderPath, dmdPath, ".*/f_vmp2_3.*.xml",
      "f_vmp_VmpType",
      "f_vmp_VpiType",
      "f_vmp_OntDrugFormType",
      "f_vmp_DrugFormType",
      "f_vmp_DrugRouteType",
      "f_vmp_ControlInfoType");

    transformFile(loaderPath, dmdPath, ".*/f_vmpp2_3.*.xml",
      "f_vmpp_VmppType",
      "f_vmpp_DtInfoType",
      "f_vmpp_ContentType");
  }

  private static void transformFile(String cmdPath, String workDir, String xmlFile, String... transforms) {
    try {
      LOG.info("Transforming XML to CSV...");
      String tools = WorkingDir + "ODSTOOLS/" + localVersions.get("ODSTOOLS").asText();
      Path xml = findFileForId(workDir, xmlFile);
      for (String transform : transforms) {
        execute(workDir,
          "java",
          "-jar",
          tools + "/saxon/Java/saxon9he.jar",
          "-t",
          "-s:" + xml.toString(),
          "-xsl:" + cmdPath + "/xsl/" + transform + ".xsl",
          "-o:" + transform + ".csv");
      }
    } catch (Exception e) {
      LOG.error("Error transforming ODS data", e);
    }
  }

  private static void execute(String path, String... params) {
    try {
      LOG.debug("Executing...");
      LOG.debug(String.join(" ", params));
      Process proc = Runtime.getRuntime().exec(params, null, new File(path));
      proc.waitFor();

      String debug = getStreamAsString(proc.getInputStream());
      if (!debug.isEmpty())
        LOG.debug(debug);

      String error = getStreamAsString(proc.getErrorStream());
      if (!error.isEmpty())
        LOG.error(error);
    } catch (Exception e) {
      LOG.error("Failed to execute command");
      System.exit(-1);
    }
  }

  private static void processBNF(TrudFeed feed) {
    String path = WorkingDir + feed.getName() + "/" + feed.getRemoteVersion();

    testAndUnzip(path, ".*/f_bnf1_.*.xml", ".*/week.*-r2_3-BNF.zip");
  }

  private static boolean testAndUnzip(String path, String test, String zip) {
    try {
      findFileForId(path, test);
      return false;
    } catch (IOException e) {
      try {
        String file = findFileForId(path, zip).toString();
        LOG.info("Unzip [{}]", file);
        try {
          unzipArchive(file, path);
        } catch (IOException x) {
          LOG.error("Error extracting [{}}]", file);
          System.exit(-1);
        }
      } catch (IOException x) {
        LOG.error("[{}] not found in [{}]", zip, path);
        System.exit(-1);
      }
      return true;
    }
  }

  private static String getStreamAsString(InputStream is) throws IOException {
    byte[] b = new byte[is.available()];
    is.read(b, 0, b.length);
    return new String(b).trim();
  }

  public static Path findFileForId(String path, String filePattern) throws IOException {
    try (Stream<Path> stream = Files.list(Paths.get(path))) { //, 16, (file, attr) -> file.toString().replace("/", "\\").matches(filePattern))) {
      List<Path> paths = stream.collect(Collectors.toList());
      paths = paths.stream().filter(f -> f.toString().replaceAll("\\\\", "/").matches(filePattern)).toList();


      if (paths.size() == 1)
        return paths.getFirst();

      if (paths.isEmpty())
        throw new IOException("No files found in [" + path + "] for expression [" + filePattern + "]");
      else {
        for (Path p : paths) {
          System.err.println("Found match : " + p.toString());
        }
        throw new IOException("Multiple files found in [" + path + "] for expression [" + filePattern + "]");
      }
    }
  }
}
