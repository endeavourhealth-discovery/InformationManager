package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.sql.*;
import java.util.*;
import java.util.zip.DataFormatException;

public class TTEntityFilerJDBC {

    private final TTGenericFilerJDBC conceptFiler;
    private final TTGenericFilerJDBC instanceFiler;
//     private final TTGenericFilerJDBC provFiler;

   public TTEntityFilerJDBC(Connection conn, Map<String, String> prefixMap) throws SQLException{
      conceptFiler = new TTGenericFilerJDBC(conn, prefixMap);
      instanceFiler = new TTGenericFilerJDBC(conn, prefixMap, "inst");
//      provFiler = new TTGenericFilerJDBC(conn, entityMap, prefixMap, bulk, "prov");
   }

    public void fileEntity(TTEntity entity, TTIriRef graph) throws DataFormatException, SQLException {
        if (IM.GRAPH_ODS.equals(graph))
            instanceFiler.fileEntity(entity, graph);
//        else if (IM.GRAPH_PROV.equals(graph))
//            provFiler.fileEntity(entity, graph);
        else
            conceptFiler.fileEntity(entity, graph);
    }
}
