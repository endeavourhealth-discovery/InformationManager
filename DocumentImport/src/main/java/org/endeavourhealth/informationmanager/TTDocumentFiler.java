package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTPrefix;
import org.endeavourhealth.informationmanager.common.transform.exceptions.FileFormatException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.zip.DataFormatException;

public interface TTDocumentFiler {

	void fileDocument(TTDocument document) throws SQLException, DataFormatException, IOException, FileFormatException;

	void fileEntities(TTDocument document) throws SQLException, DataFormatException, IOException;

	void fileNamespaces(List<TTPrefix> prefixes) throws SQLException;

	void setGraph(TTIriRef graph);
	}
