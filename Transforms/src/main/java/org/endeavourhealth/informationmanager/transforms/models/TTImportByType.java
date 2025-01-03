package org.endeavourhealth.informationmanager.transforms.models;


import org.endeavourhealth.imapi.model.tripletree.TTIriRef;

/**
 * An interface that handles a variety of data imports to the information model as specified by the type of import (Graph name)
 * and root folder containing the files and subfolders required
 */
public interface TTImportByType {

  TTImportByType importByType(TTIriRef importType, TTImportConfig config) throws Exception;

  TTImportByType importByType(String importType, TTImportConfig config) throws Exception;

  TTImportByType validateByType(TTIriRef importType, String inFolder) throws Exception;

  TTImportByType validateByType(String importType, String inFolder) throws Exception;

}

