package org.endeavourhealth.informationmanager;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An Interface that handles the import of a variety of data sources such as Classifications and supplier look ups
 */
public interface TTImport extends AutoCloseable{

   TTImport importData(TTImportConfig config) throws Exception;
   TTImport validateFiles(String inFolder);
   TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException;

}
