package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.sql.*;
import java.util.*;
import java.util.zip.DataFormatException;

public class TTEntityFilerJDBC {

    private final TTConceptFilerJDBC conceptFiler;
    private final TTInstanceFilerJDBC instanceFiler;

   public TTEntityFilerJDBC(Connection conn, Map<String, String> prefixMap) throws SQLException{
      conceptFiler = new TTConceptFilerJDBC(conn, prefixMap);
      instanceFiler = new TTInstanceFilerJDBC(conn, conceptFiler);
   }

    public void fileEntity(TTEntity entity, TTIriRef graph) throws DataFormatException, SQLException {
        if (IM.GRAPH_ODS.equals(graph))
            instanceFiler.fileEntity(entity, graph);
        else
            conceptFiler.fileEntity(entity, graph);
    }
}
