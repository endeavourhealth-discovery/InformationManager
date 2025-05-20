package org.endeavourhealth.informationmanager.transforms;

import net.lingala.zip4j.ZipFile;
import net.lingala.zip4j.model.FileHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ZipUtils {
  private static final Logger LOG = LoggerFactory.getLogger(ZipUtils.class);
  private static final Integer ZIP_BUFFER = 1024 * 32;

  public static File unzipFile(String sourceZip, String workDir) throws IOException {
    LOG.info("Unzipping {}...", sourceZip);
    File fileToUnzip = new File(workDir, sourceZip);
    try (ZipFile zipFile = new ZipFile(fileToUnzip)) {

      List<FileHeader> zipEntries = zipFile.getFileHeaders();
      if (zipEntries.size() != 1) {
        LOG.error("Zip contains more than 1 file!");
        System.exit(-1);
      }

      FileHeader entry = zipEntries.getFirst();

      zipFile.extractFile(entry, workDir);
      return new File(workDir, entry.getFileName());
    }
  }
}
