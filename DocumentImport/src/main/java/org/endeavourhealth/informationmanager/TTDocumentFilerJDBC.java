package org.endeavourhealth.informationmanager;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.zip.DataFormatException;

public class TTDocumentFilerJDBC implements TTDocumentFiler {

   private static final Logger LOG = LoggerFactory.getLogger(TTDocumentFiler.class);

   private final Map<String, Integer> namespaceMap = new HashMap<>();
   private final Map<String, String> prefixMap = new HashMap<>();
   private TTIriRef graph;
   private final Connection conn;
   private TTEntityFilerJDBC entityFiler;

   private final PreparedStatement getNamespace;
   private final PreparedStatement getNsFromPrefix;
   private final PreparedStatement insertNamespace;
   private final PreparedStatement updateNamespace;




   public TTDocumentFilerJDBC() throws Exception {
      conn= getConnection();

      getNamespace = conn.prepareStatement("SELECT * FROM namespace WHERE iri = ?");
      getNsFromPrefix = conn.prepareStatement("SELECT * FROM namespace WHERE prefix = ?");
      insertNamespace = conn.prepareStatement("INSERT INTO namespace (iri, prefix,name) VALUES (?, ?,?)", Statement.RETURN_GENERATED_KEYS);
      updateNamespace = conn.prepareStatement("UPDATE namespace SET name=? WHERE dbid=?");



   }

   public static Connection getConnection() throws SQLException, ClassNotFoundException {
      Map<String, String> envVars = System.getenv();

      String url = envVars.get("CONFIG_JDBC_URL");
      String user = envVars.get("CONFIG_JDBC_USERNAME");
      String pass = envVars.get("CONFIG_JDBC_PASSWORD");
      String driver = envVars.get("CONFIG_JDBC_CLASS");

      if (url == null || url.isEmpty()
        || user == null || user.isEmpty()
        || pass == null || pass.isEmpty())
         throw new IllegalStateException("You need to set the CONFIG_JDBC_ environment variables!");

      if (driver != null && !driver.isEmpty())
         Class.forName(driver);

      Properties props = new Properties();

      props.setProperty("user", user);
      props.setProperty("password", pass);

      Connection conn = DriverManager.getConnection(url, props);
      return conn;

   }


   public void startTransaction() throws SQLException {
      conn.setAutoCommit(false);
   }


   public void commit() throws SQLException {
      conn.commit();
   }


   public void close() throws SQLException {
      conn.close();

   }


   public void rollBack() throws SQLException {
      conn.rollback();

   }


   @Override
   public void fileDocument(TTDocument document) throws SQLException, DataFormatException, IOException{
      try {
         this.graph= document.getGraph();
         entityFiler = new TTEntityFilerJDBC(conn, prefixMap);
         System.out.println("Saving ontology - " + (new Date().toString()));
         LOG.info("Processing namespaces");

         // Ensure all namespaces exist (auto-create)
         //Different prefixes for filing are not allowed in this version
         fileNamespaces(document.getContext().getNameSpaces());

         //Sets the graph namesepace id for use in statements so they are owned by the namespace graph
         setGraph(document.getGraph());
         fileEntities(document);


         // Record document details, updating ontology and module
         LOG.info("Processing document-ontology-module");




         LOG.info("Ontology filed");
      } catch (Exception e) {
         LOG.info("Error - " + (new Date().toString()));
         e.printStackTrace();
         throw e;
      } finally {
         close();
         System.out.println("Finished - " + (new Date().toString()));
      }
   }


   @Override
   public void fileEntities(TTDocument document) throws SQLException, DataFormatException, IOException {
      System.out.println("Filing entities.... ");
      startTransaction();
      if (document.getEntities()!=null) {
         int i = 0;
         for (TTEntity entity : document.getEntities()) {
            //inherit crud
            if (entity.getCrud()==null)
               if (document.getCrud()==null)
                  entity.setCrud(IM.REPLACE);
               else
                  entity.setCrud(document.getCrud());
            fileEntity(entity);
            i++;
            if (i % 1000 == 0) {
               System.out.println("Filed "+i +" entities from "+document.getEntities().size()+" example "+entity.getIri());
               commit();
               startTransaction();
            }
         }
      }
      commit();
   }



   @Override
   public void fileNamespaces(List<TTPrefix> prefixes) throws SQLException {
      if (prefixes == null || prefixes.size() == 0)
         return;
      startTransaction();
      //Populates the namespace map with both namespace iri and prefix as keys
      for (TTPrefix prefix : prefixes) {
         upsertNamespace(prefix);
      }
      commit();
   }


   public void upsertNamespace(TTPrefix ns) throws SQLException {

      DALHelper.setString(getNamespace, 1, ns.getIri());
      try (ResultSet rs = getNamespace.executeQuery()) {
         if (rs.next()) {
            Integer dbid=rs.getInt("dbid");
            if (!ns.getPrefix().equals(rs.getString("prefix"))) {
               throw new SQLException("prefix in database -> " + ns.getPrefix() + " does not match the iri " + ns.getIri());
            } else {
               if (ns.getName()!=null){
                  DALHelper.setString(updateNamespace,1,ns.getName());
                  DALHelper.setInt(updateNamespace,2,dbid);
                  updateNamespace.executeUpdate();
               }
               prefixMap.put(ns.getPrefix(),ns.getIri());
               namespaceMap.put(ns.getPrefix(), rs.getInt("dbid"));
               namespaceMap.put(ns.getIri(), rs.getInt("dbid"));
            }
         } else {
            DALHelper.setString(getNsFromPrefix, 1, ns.getPrefix());
            ResultSet ps = getNsFromPrefix.executeQuery();
            if (ps.next()) {
               if (!ns.getIri().equals(ps.getString("iri"))) {
                  throw new SQLException(ps.getString("prefix") + "->" + ps.getString("iri) "
                      + " does not match " + ns.getIri() + ns.getIri()));
               }
            } else {
               DALHelper.setString(insertNamespace, 1, ns.getIri());
               DALHelper.setString(insertNamespace, 2, ns.getPrefix());
               DALHelper.setString(insertNamespace, 3, ns.getName());
               insertNamespace.executeUpdate();
               Integer dbid = DALHelper.getGeneratedKey(insertNamespace);
               prefixMap.put(ns.getPrefix(), ns.getIri());
               namespaceMap.put(ns.getIri(), dbid);
               namespaceMap.put(ns.getPrefix(), dbid);
            }
         }
      } catch (SQLException e) {
         throw new SQLException(e.toString() + " problem adding namespace " +
             ns.getPrefix() + ns.getIri());
      }
   }



   public void fileEntity(TTEntity entity) throws SQLException, DataFormatException, IOException {
      entityFiler.fileEntity(entity,graph);

   }

   @Override
   public void setGraph(TTIriRef graph) {
      this.graph= graph;
   }
}
