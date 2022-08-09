package org.endeavourhealth.informationmanager.trud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class TrudUpdater {
    private static final Logger LOG = LoggerFactory.getLogger(TrudUpdater.class);

    private static final List<TrudFeed> feeds = Arrays.asList(
        new TrudFeed("CLINICAL","101"),
        new TrudFeed("DRUG","105"),
        new TrudFeed("MAPS","9"),
        new TrudFeed("ICD10","258"),
        new TrudFeed("OPCS4","119"),
        new TrudFeed("HISTORY","276"),
        new TrudFeed("ODS","341"),
        new TrudFeed("PRIMARY","659")
    );

    private static String APIKey;
    private static String WorkingDir;
    private static ObjectMapper mapper;
    private static ObjectNode localVersions;

    public static void main(String argv[]) {
        if (argv.length != 2) {
            System.err.println("You must provide an API key and working directory as parameters");
            System.err.println("TrudUpdater <api key> <working dir>");
            System.exit(-1);
        }

        APIKey = argv[0];
        WorkingDir = argv[1];
        mapper = new ObjectMapper();

        LOG.info("Collecting version information...");
        try {
            getRemoteVersions();
            getLocalVersions();
            if (versionMismatches()) {
                downloadUpdates();
                // importSnomed();
            }
            LOG.info("Finished");
        } catch (Exception e) {
            LOG.error("Error:");
            e.printStackTrace();
        }
    }

    private static void getRemoteVersions() throws IOException {
        Client client = ClientBuilder.newClient();
        ObjectMapper om = new ObjectMapper();
        for (TrudFeed feed : feeds) {
            LOG.info("Fetching remote version [" + feed.getName() + "]...");
            WebTarget target = client.target("https://isd.digital.nhs.uk").path("/trud3/api/v1/keys/" + APIKey + "/items/" + feed.getId() + "/releases?latest");
            Response response = target.request().get();
            String responseRaw = response.readEntity(String.class);
            if (response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL) {
                LOG.error("Could not get remote version for " + feed.getName());
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

    private static void getLocalVersions() throws IOException {
        try {
            localVersions = (ObjectNode) mapper.readTree(new File(WorkingDir + "versions.json"));
        } catch (FileNotFoundException e) {
            localVersions = JsonNodeFactory.instance.objectNode();
        }
    }

    private static void updateLocalVersions(TrudFeed feed) throws IOException {
        localVersions.put(feed.getName(), feed.getRemoteVersion());
        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(WorkingDir + "versions.json"), localVersions);
    }

    private static boolean versionMismatches() {
        boolean mismatches = false;
        for (TrudFeed feed : feeds) {
            if (localVersions.has(feed.getName()) && feed.getRemoteVersion().equals(localVersions.get(feed.getName()).asText()))
                LOG.info("[" + feed.getName() + "] - CURRENT");
            else {
                LOG.warn("[" + feed.getName() + "] - UPDATED");
                feed.setUpdated(true);
                mismatches = true;
            }
        }
        return mismatches;
    }

    private static void downloadUpdates() throws IOException {
        LOG.info("Downloading updates...");

        for (TrudFeed feed : feeds) {
            LOG.info("Processing {}", feed.getName());
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

            // Does the extract dir exist?
            String extractDir = WorkingDir + "/" + feed.getName() + "/" + feed.getRemoteVersion();
            if (Files.notExists(Paths.get(extractDir))) {
                // Does the zip exist?
                String zipFile = WorkingDir + feed.getName() + "_" + feed.getRemoteVersion() + ".zip";
                if (Files.notExists(Paths.get(extractDir))) {
                    downloadFile(feed.getDownload(), zipFile);
                }
                unzipArchive(zipFile, extractDir);
            }
            updateLocalVersions(feed);
        }
    }

    private static void downloadFile(String sourceUrl, String destination) throws IOException {
        URL url = new URL(sourceUrl);
        URLConnection con = url.openConnection();
        long contentLength = con.getContentLengthLong();
        LOG.info("Downloading {} - {} bytes", url, contentLength);
        try (InputStream inputStream = con.getInputStream();
            OutputStream outputStream = new FileOutputStream(destination + ".tmp")) {

            // Limiting byte written to file per loop
            byte[] buffer = new byte[2048];

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

    private static void unzipArchive(String zipFile, String destination) throws IOException {
        LOG.info("Unzipping {}", zipFile);
        File destDir = new File(destination);
        byte[] buffer = new byte[1024];
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
}
