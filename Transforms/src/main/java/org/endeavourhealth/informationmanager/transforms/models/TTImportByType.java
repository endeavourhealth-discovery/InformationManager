package org.endeavourhealth.informationmanager.transforms.models;

import org.endeavourhealth.imapi.vocabulary.ImportType;

/**
 * An interface that handles a variety of data imports to the information model as specified by the type of import (Graph name)
 * and root folder containing the files and subfolders required
 */
public interface TTImportByType {

  TTImportByType importByType(ImportType importType, TTImportConfig config) throws Exception;

  TTImportByType validateByType(ImportType importType, String inFolder) throws Exception;

}

