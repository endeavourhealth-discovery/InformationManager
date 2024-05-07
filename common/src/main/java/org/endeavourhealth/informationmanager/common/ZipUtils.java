package org.endeavourhealth.informationmanager.common;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;

public class ZipUtils {
    private static final Logger LOG = LoggerFactory.getLogger(ZipUtils.class);
    private static final Integer ZIP_BUFFER = 1024 * 32;

/*    public static void zipFile(String sourceFile, String destZip, String workDir) throws IOException, InterruptedException {
        deleteZipParts(workDir + destZip);

        LOG.info("Zipping {} to {}...", sourceFile, destZip);
        String zipCmd = "zip -s 25m " + destZip + " " + sourceFile;
        if (execCmd(zipCmd, new File(workDir)) != 0) {
            LOG.error("Zip command failed!");
            System.exit(-1);
        }

        File fileToZip = new File(workDir + sourceFile);
        Files.delete(fileToZip.toPath());
    }*/

    public static File unzipFile(String sourceZip, String workDir) throws IOException {
        LOG.info("Unzipping {}...", sourceZip);
        File fileToUnzip = new File(workDir, sourceZip);
        try (ZipFile zipFile = new ZipFile(fileToUnzip)) {

            List<FileHeader> zipEntries = zipFile.getFileHeaders();
            if (zipEntries.isEmpty() || zipEntries.size() > 1) {
                LOG.error("Zip contains more than 1 file!");
                System.exit(-1);
            }

            FileHeader entry = zipEntries.get(0);

            zipFile.extractFile(entry, workDir);
            return new File(workDir, entry.getFileName());
        }
    }
/*
    public static byte[] compress(byte[] data) throws IOException {
        Deflater deflater = new Deflater(Deflater.BEST_COMPRESSION);
        deflater.setInput(data);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
            deflater.finish();
            byte[] buffer = new byte[1024];
            while (!deflater.finished()) {
                int count = deflater.deflate(buffer); // returns the generated code... index
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        }
    }
    public static byte[] decompress(byte[] data) throws IOException, DataFormatException {
        Inflater inflater = new Inflater();
        inflater.setInput(data);
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length)) {
            byte[] buffer = new byte[1024];
            while (!inflater.finished()) {
                int count = inflater.inflate(buffer);
                outputStream.write(buffer, 0, count);
            }
            outputStream.close();
            return outputStream.toByteArray();
        }
    }

    private static void deleteZipParts(String zipFile) throws IOException {
        File zip = new File(zipFile);
        File zipPath = new File(zip.getParent());
        final String delPattern = zip.getName().substring(0, zip.getName().length() - 4) + ".z";
        LOG.info("Removing files matching {}/{}*", zipPath, delPattern);
        for (File f: zipPath.listFiles((d,f) -> f.startsWith(delPattern))) {
            LOG.info("Deleting {} ...", f);
            Files.delete(f.toPath());
        }
    }

    private static int execCmd(String command, File workingDir) throws InterruptedException, IOException {
        Runtime rt = Runtime.getRuntime();

        Process proc = rt.exec(command, null, workingDir);
        proc.waitFor();

        String output = getStreamAsString(proc.getInputStream());
        if (!output.isEmpty())
            LOG.debug(output);

        String error = getStreamAsString(proc.getErrorStream());
        if (!error.isEmpty())
            LOG.error(error);

        return proc.exitValue();
    }

    private static String getStreamAsString(InputStream is) throws IOException {
        byte[] b=new byte[is.available()];
        is.read(b,0,b.length);
        return new String(b);
    }*/
}
