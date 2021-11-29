package org.endeavourhealth.informationmanager.transforms;


import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTFilerFactory;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;
import org.endeavourhealth.informationmanager.rdf4j.TTDocumentFilerRdf4j;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;


public class ImportUtils {
   private static final ValueFactory valueFactory= new ValidatingValueFactory(SimpleValueFactory.getInstance());
   private static final Logger LOG = LoggerFactory.getLogger(ImportUtils.class);

   /**
    * Validates the presence of various files from a root folder path
    * Files are presented as an array of an array of file patterns as regex patterns
    * @param path  the root folder that holds the files as subfolders
    * @param values One or more string arrays each containing a list of file patterns
    */
   public static void validateFiles(String path,String[] ... values){

      Arrays.stream(values).sequential().forEach(fileArray ->
          Arrays.stream(fileArray).sequential().forEach(file-> {
         try {
            findFileForId(path, file);
         } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
         }
      }
          )
      );
   }

   private enum FilerType {
      JDBC,RDF4J
   }

   /**Finds a file for a folder and file name pattern
    *
    * @param path the root folder
    * @param filePattern the file name with wild cards as a regex pattern
    * @return Path  as a Path object
    * @throws IOException if the file cannot be found or is invalid
    */
   public static Path findFileForId(String path, String filePattern) throws IOException {
      List<Path> paths = Files.find(Paths.get(path), 16,
          (file, attr) -> file.toString().replace("/", "\\").matches(filePattern))
          .collect(Collectors.toList());

      if (paths.size() == 1)
         return paths.get(0);

      if (paths.isEmpty())
         throw new IOException("No files found in [" + path + "] for expression [" + filePattern + "]");
      else
         throw new IOException("Multiple files found in [" + path + "] for expression [" + filePattern + "]");
   }

   /**
    * Returns a list of file paths for a file pattern with a root folder
    * @param path  The root folder that contains the files
    * @param filePattern a regex pattern for a file
    * @return List of Paths
    * @throws IOException if the files cannot be found or are invalid
    */
   public static List<Path> findFilesForId(String path, String filePattern) throws IOException {
      return Files.find(Paths.get(path), 16,
          (file, attr) -> getFilePath(file.toString())
              .matches(filePattern))
          .collect(Collectors.toList());
   }

   public static RepositoryConnection getGraphConnection(){
      //repo = new SailRepository(new NativeStore(new File("Z:\\rdf4j")));
      Repository repo = new HTTPRepository("http://localhost:7200/", "im");
      return repo.getConnection();
   }

   /**
    * Creates a JDBC connection given a set of environment varables
    * @return The JSBC Connection
    * @throws SQLException in the event of an SQL failure
    * @throws ClassNotFoundException if the JDBC connection driver cannot be found
    */
   public static Connection getConnection() throws ClassNotFoundException, SQLException {
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

      return DriverManager.getConnection(url, props);
   }

   private static FilerType getFilerType() throws TTFilerException {
      TTDocumentFiler aFiler = TTFilerFactory.getDocumentFiler();
      if (TTDocumentFilerRdf4j.class.isAssignableFrom(aFiler.getClass()))
         return FilerType.RDF4J;
      else
         return FilerType.JDBC;
   }

   /**
    * Retrieves EMIS to Snomed code maps
    * @param emisToSnomed Mapping object for emis code to many snomed codes
    * @param emisToTerm Map object for emis code to term
    * @throws SQLException
    * @throws ClassNotFoundException
    * @throws TTFilerException
    */
   public static void importEmisToSnomed(Map<String,List<String>> emisToSnomed,Map<String,String> emisToTerm) throws SQLException, ClassNotFoundException, TTFilerException {
      if (getFilerType()==FilerType.JDBC)
         importEmisToSnomedJDBC(emisToSnomed,emisToTerm);
      else
         importEmisToSnomedRdf4j(emisToSnomed,emisToTerm);
   }

   /**
    * Retrieves snomed codes from im
    * @return a set of snomed codes
    * @throws TTFilerException if using rdf4j
    * @throws SQLException if using jdbc
    * @throws ClassNotFoundException if using jdbc
    */
   public  static Set<String> importSnomedCodes() throws TTFilerException, SQLException, ClassNotFoundException {
      Set<String> snomedCodes = new HashSet<>();
      if (getFilerType() == FilerType.JDBC)
         return importSnomedJDBC(snomedCodes);
      else
         return importSnomedRDF4J(snomedCodes);
   }

   /**
    * Retieves read to Snomed maps, using the Vision code scheme as a proxy for read
    * @return the code to Snomed code one to many map
    * @throws SQLException  if using RJDBC
    * @throws ClassNotFoundException if using JDBC
    * @throws TTFilerException
    */
   public static Map<String,List<String>> importReadToSnomed() throws SQLException, ClassNotFoundException, TTFilerException {
      Map<String,List<String>> readToSnomed= new HashMap<>();
      if (getFilerType()==FilerType.JDBC)
         return importReadToSnomedJdbc(readToSnomed);
      else
         return importReadToSnomedRdf4j(readToSnomed);
   }

   /**
    * Gets descendant codes for an iri and its terms;
    * @param concept the iri for the parent concept
    * @param scheme the iri for the code scheme of the parent concept
    * @return A map from code to many terms;
    * @throws TTFilerException
    */
   public static Map<String,List<String>> getDescendants(String concept,String scheme) throws TTFilerException {
      Map<String, List<String>> codeToTerm = new HashMap<>();
      RepositoryConnection conn = getGraphConnection();
      TupleQuery qry = conn.prepareTupleQuery("select ?child ?name\n" +
        "where {GRAPH <"+SNOMED.GRAPH_SNOMED.getIri()+"> { ?child <" + RDFS.SUBCLASSOF.getIri() + ">+ ?concept.\n" +
        "?child <" + RDFS.LABEL.getIri() + "> ?name.}}");
      qry.setBinding("concept", valueFactory.createIRI(concept));
      try {
         TupleQueryResult rs = qry.evaluate();
         while (rs.hasNext()) {
            BindingSet bs = rs.next();
            String code = bs.getValue("child").stringValue();
            String term = bs.getValue("name").stringValue();
            List<String> maps = codeToTerm.computeIfAbsent(code, k -> new ArrayList<>());
            maps.add(term);
         }

      } catch (RepositoryException e) {
         throw new TTFilerException("Unable to retrieve snomed codes");
      }
      return codeToTerm;
   }




   public static Set<String> importSimpleSet(String iri) throws TTFilerException, SQLException, ClassNotFoundException {
      Set<String> aSet= new HashSet<>();
      if (getFilerType()== FilerType.JDBC)
         return importSimpleSetJDBC(aSet,iri);
      else
         return importSimpleSetRdf4j(aSet,iri);
   }

   private static Set<String> importSnomedRDF4J(Set<String> snomedCodes) throws TTFilerException {
      RepositoryConnection conn= getGraphConnection();
      TupleQuery qry= conn.prepareTupleQuery("select ?snomed\n"+
        "where {?concept <"+ IM.HAS_SCHEME.getIri()+"> <"+SNOMED.GRAPH_SNOMED.getIri()+">.\n"+
   "?concept <"+IM.CODE.getIri()+"> ?snomed}");
      try {
         TupleQueryResult rs= qry.evaluate();
         while (rs.hasNext()){
            BindingSet bs=rs.next();
            snomedCodes.add(bs.getValue("snomed").stringValue());
         }
      }catch (RepositoryException e){
         throw new TTFilerException("Unable to retrieve snomed codes");
      }
      return snomedCodes;
   }


   private static Set<String> importSimpleSetRdf4j(Set<String> aSet,String iri) throws TTFilerException {
      RepositoryConnection conn= getGraphConnection();
      TupleQuery qry= conn.prepareTupleQuery("select ?code\n"+
        "where {?member ^<"+SHACL.OR.getIri()+"> ?def.\n" +
        "?def <"+IM.DEFINITION.getIri()+"> ?set.\n        "+
         "?member <"+IM.CODE.getIri()+"> ?code}");
      try {
         TupleQueryResult rs = qry.evaluate();
         while (rs.hasNext()) {
            BindingSet bs = rs.next();
            aSet.add(bs.getValue("code").stringValue());
         }
      }catch (RepositoryException e){
         throw new TTFilerException("unable to retrieve set members : "+e);
      }
      return aSet;
   }

   private static Set<String> importSimpleSetJDBC(Set<String> aSet,String iri) throws SQLException, ClassNotFoundException {
      Connection conn= getConnection();
      try (PreparedStatement getSet= conn.prepareStatement("Select o.iri\n" +
        "from tpl\n" +
        "join entity e on tpl.subject= e.dbid\n" +
        "join entity p on tpl.predicate=p.dbid\n" +
        "join entity o on tpl.object= o.dbid\n" +
        "where e.iri=?set and p.iri='"+ SHACL.OR.getIri()+"'")) {
         DALHelper.setString(getSet,1,iri);
         ResultSet rs = getSet.executeQuery();
         while (rs.next()) {
            String member = rs.getString("iri");
            aSet.add(member);
         }
      }
         return aSet;
   }


   private static Map<String, List<String>> importReadToSnomedRdf4j(Map<String, List<String>> readToSnomed) throws TTFilerException {
     RepositoryConnection conn= getGraphConnection();
     TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed\n"+
       "where {GRAPH <"+IM.GRAPH_VISION.getIri()+"> {?concept <"+IM.CODE.getIri()+"> ?code. \n"+
   "?concept <"+RDFS.SUBCLASSOF.getIri()+"> ?snomed.}}");
     try {
        TupleQueryResult rs= qry.evaluate();
        while (rs.hasNext()){
           BindingSet bs= rs.next();
           String read= bs.getValue("code").stringValue();
           String snomed= bs.getValue("snomed").stringValue();
           List<String> maps = readToSnomed.computeIfAbsent(read, k -> new ArrayList<>());
           maps.add(snomed);

        }

     } catch (RepositoryException e){
        throw new TTFilerException("unable to retrieve vision/read "+e);
     }
     return readToSnomed;
   }

   private static Map<String, List<String>> importReadToSnomedJdbc(Map<String, List<String>> readToSnomed) throws SQLException, ClassNotFoundException {
      Connection conn= getConnection();
      try (PreparedStatement getR2Matches= conn.prepareStatement("select vis.code as code,snomed.code as snomed \n"+
        "from entity snomed \n" +
        "join tpl maps on maps.object= snomed.dbid\n" +
        "join entity p on maps.predicate=p.dbid\n" +
        "join entity vis on maps.subject=vis.dbid\n" +
        "where snomed.iri like '"+ SNOMED.NAMESPACE+"%'\n"+
        "and p.iri='"+ RDFS.SUBCLASSOF.getIri()+"'\n" +
        "and vis.iri like 'http://endhealth.info/VISION#'")) {
         ResultSet rs = getR2Matches.executeQuery();
         while (rs.next()) {
            String snomed = rs.getString("snomed");
            String read = rs.getString("code");
            List<String> maps = readToSnomed.computeIfAbsent(read, k -> new ArrayList<>());
            maps.add(snomed);

         }
      }
      return readToSnomed;
   }

   private static void importEmisToSnomedRdf4j(Map<String, List<String>> emisToSnomed, Map<String,String> emisToTerm) throws TTFilerException {
      RepositoryConnection conn= getGraphConnection();
      TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed  ?name\n"+
        "where {GRAPH <"+IM.GRAPH_EMIS.getIri()+"> \n"+
          "{?concept <"+IM.CODE.getIri()+"> ?code. \n"+
        "?concept <"+RDFS.LABEL.getIri()+"> ?name.\n"+
        "?concept <"+RDFS.SUBCLASSOF.getIri()+"> ?snomedIri.}\n" +
        "GRAPH <"+SNOMED.GRAPH_SNOMED.getIri()+"> {"+
        "?snomedIri <"+IM.CODE.getIri()+"> ?snomed.}}");
      try {
         TupleQueryResult rs= qry.evaluate();
         while (rs.hasNext()){
            BindingSet bs= rs.next();
            String read= bs.getValue("code").stringValue();
            String snomed= bs.getValue("snomed").stringValue();
            List<String> maps = emisToSnomed.computeIfAbsent(read, k -> new ArrayList<>());
            maps.add(snomed);
            emisToTerm.put(read,bs.getValue("name").stringValue());

         }

      } catch (RepositoryException e){
         throw new TTFilerException("unable to retrieve vision/read "+e);
      }
   }

   private static void importEmisToSnomedJDBC(Map<String, List<String>> emisToSnomed,Map<String,String> emisToTerm) throws SQLException, ClassNotFoundException {
      Connection conn= getConnection();

   PreparedStatement getEMIS= conn.prepareStatement("SELECT entity.code as code,snomed.code as snomed,entity.name as name\n" +
        "from entity\n" +
        "join tpl on tpl.subject= entity.dbid\n" +
        "join entity snomed on tpl.object= snomed.dbid\n" +
        "join entity subclass on tpl.predicate=subclass.dbid\n" +
        "where entity.scheme='http://endhealth.info/emis#'\n" +
        "and snomed.scheme='http://snomed.info/sct#'");
      ResultSet rs= getEMIS.executeQuery();
      while (rs.next()){
         String emis= rs.getString("code");
         String snomed=rs.getString("snomed");
         emisToSnomed.computeIfAbsent(emis, k -> new ArrayList<>());
         emisToSnomed.get(emis).add(snomed);
         emisToTerm.put(rs.getString("code"),rs.getString("name"));
      }
      conn.close();
   }

   private static Set<String> importSnomedJDBC(Set<String> snomedCodes) throws TTFilerException, SQLException, ClassNotFoundException {
         Connection conn = getConnection();
      try {
         PreparedStatement getSnomed = conn.prepareStatement("SELECT code from entity "
           + "where iri like 'http://snomed.info/sct%'");
         ResultSet rs = getSnomed.executeQuery();
         while (rs.next())
            snomedCodes.add(rs.getString("code"));
         if (snomedCodes.isEmpty()) {
            System.err.println("Snomed must be loaded first");
            System.exit(-1);
         }
         conn.close();
      } catch (SQLException e){
         throw new RepositoryException("unable to retrieve snomed codes");
      } finally {
         conn.close();
      }
      return snomedCodes;
   }

   private static boolean isMacOs() {
      return (System.getProperty("os.name").toLowerCase().contains("mac"));
   }

   private static String getFilePath(String path) {
       return isMacOs() ? path.replace("/", "\\") : path;
   }

}
