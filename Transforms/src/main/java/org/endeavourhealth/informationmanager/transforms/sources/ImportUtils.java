package org.endeavourhealth.informationmanager.transforms.sources;


import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.informationmanager.common.ZipUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;


public class ImportUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ImportUtils.class);

    private static final int BUFFER_SIZE = 8192;

    /**
     * Validates the presence of various files from a root folder path
     * Files are presented as an array of an array of file patterns as regex patterns
     *
     * @param path   the root folder that holds the files as subfolders
     * @param values One or more string arrays each containing a list of file patterns
     */
    public static void validateFiles(String path, String[]... values) {
        AtomicBoolean exit = new AtomicBoolean(false);
        Arrays.stream(values).sequential().forEach(fileArray ->
            Arrays.stream(fileArray).sequential().forEach(file -> {
                    try {
                        findFileForId(path, file);
                    } catch (IOException e) {
                        LOG.info("Error finding file [{}]", file);
                        System.err.println(e.getMessage());
                        exit.set(true);
                    }
                }
            )
        );

        if (exit.get())
            System.exit(-1);
    }

    /**
     * Finds a file for a folder and file name pattern
     *
     * @param path        the root folder
     * @param filePattern the file name with wild cards as a regex pattern
     * @return Path  as a Path object
     * @throws IOException if the file cannot be found or is invalid
     */
    public static Path findFileForId(String path, String filePattern) throws IOException {
        try (Stream<Path> stream = Files.find(Paths.get(path), 16, (file, attr) -> file.toString().replace("/", "\\").matches(filePattern))) {
            List<Path> paths = stream.collect(Collectors.toList());

            if (paths.size() == 1)
                return paths.get(0);

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

    /**
     * Returns a list of file paths for a file pattern with a root folder
     *
     * @param path        The root folder that contains the files
     * @param filePattern a regex pattern for a file
     * @return List of Paths
     * @throws IOException if the files cannot be found or are invalid
     */
    public static List<Path> findFilesForId(String path, String filePattern) throws IOException {
        try (Stream<Path> stream = Files.find(Paths.get(path), 16, (file, attr) -> getFilePath(file.toString()).matches(filePattern))) {
            return stream.collect(Collectors.toList());
        }
    }

    public static boolean isEmpty(TTArray array) {
        if (array == null)
            return true;
        return array.getElements().size() == 0;
    }

    public static boolean isEmpty(List list) {
        if (list == null)
            return true;
        return list.size() == 0;
    }

    public static void unzipArchive(String zipFile, String destination) throws IOException {
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
            LOG.info("Extracting " + zipEntry.getName() + " - 0%\r");
            while ((len = zis.read(buffer)) > 0) {
                read += len;
                fos.write(buffer, 0, len);
                if (read % 1024 == 0)
                    System.out.print("Extracting " + zipEntry.getName() + " - " + (read * 100 / zipEntry.getSize()) + "%\r");
            }
        }
        LOG.info("Extracted {} - 100%", zipEntry.getName());
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

    private static boolean isWindows() {
        return (System.getProperty("os.name").toLowerCase().contains("win"));
    }

    private static String getFilePath(String path) {
        return isWindows() ? path : path.replace("/", "\\");
    }

}
