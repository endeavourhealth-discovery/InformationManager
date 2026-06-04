package org.endeavourhealth.informationmanager.transforms.models;

import org.endeavourhealth.interfacemanager.model.IMPORTTYPE;

/**
 * An interface that handles a variety of data imports to the information model as specified by the type of import (Graph name)
 * and root folder containing the files and subfolders required
 */
public interface TTImportByType {

  TTImportByType importByType(IMPORTTYPE importType, TTImportConfig config) throws Exception;

  TTImportByType validateByType(IMPORTTYPE importType, String inFolder) throws Exception;

}

