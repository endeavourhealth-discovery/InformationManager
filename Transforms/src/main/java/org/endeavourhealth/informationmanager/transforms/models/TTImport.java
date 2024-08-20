package org.endeavourhealth.informationmanager.transforms.models;

import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTImportConfig;

/**
 * An Interface that handles the import of a variety of data sources such as Classifications and supplier look ups
 */
public interface TTImport extends AutoCloseable {
  void importData(TTImportConfig config) throws ImportException;

  void validateFiles(String inFolder) throws TTFilerException;
}
