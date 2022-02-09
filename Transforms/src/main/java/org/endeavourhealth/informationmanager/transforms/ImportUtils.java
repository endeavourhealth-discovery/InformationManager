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
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.filer.rdf4j.TTDocumentFilerRdf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


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
       try (Stream<Path> stream = Files.find(Paths.get(path), 16, (file, attr) -> file.toString().replace("/", "\\").matches(filePattern))) {
           List<Path> paths = stream.collect(Collectors.toList());

           if (paths.size() == 1)
               return paths.get(0);

           if (paths.isEmpty())
               throw new IOException("No files found in [" + path + "] for expression [" + filePattern + "]");
           else {
               for (Path p : paths) {
                   System.err.println("Found match : " + p.toString());
               }
               throw new IOException("Multiple files found in [" + path + "] for expression [" + filePattern + "]");
           }
       }
   }

   /**
    * Returns a list of file paths for a file pattern with a root folder
    * @param path  The root folder that contains the files
    * @param filePattern a regex pattern for a file
    * @return List of Paths
    * @throws IOException if the files cannot be found or are invalid
    */
   public static List<Path> findFilesForId(String path, String filePattern) throws IOException {
       try (Stream<Path> stream = Files.find(Paths.get(path), 16, (file, attr) -> getFilePath(file.toString()).matches(filePattern))) {
           return stream.collect(Collectors.toList());
       }
   }

   public static boolean isEmpty(TTArray array){
      if (array==null)
         return true;
      return array.getElements().size()==0;
   }
   public static boolean isEmpty(List list){
      if (list==null)
         return true;
      return list.size()==0;
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
      try (TTDocumentFiler aFiler = TTFilerFactory.getDocumentFiler()) {
          if (TTDocumentFilerRdf4j.class.isAssignableFrom(aFiler.getClass()))
              return FilerType.RDF4J;
          else
              return FilerType.JDBC;
      } catch (Exception e) {
          throw new TTFilerException("Failed to create document filer", e);
      }
   }

   /**
    * Retrieves EMIS to Snomed code maps
    * @throws SQLException
    * @throws ClassNotFoundException
    * @throws TTFilerException
    */
   public static Map<String,Set<String>> importEmisToSnomed() throws SQLException, ClassNotFoundException, TTFilerException {
      if (getFilerType()==FilerType.JDBC)
         return importEmisToSnomedJDBC();
      else
         return importEmisToSnomedRdf4j();
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






   private static Set<String> importSnomedRDF4J(Set<String> snomedCodes) throws TTFilerException {

      try (RepositoryConnection conn= getGraphConnection();){
         TupleQuery qry= conn.prepareTupleQuery("select ?snomed\n"+
           "where {?concept <"+ IM.HAS_SCHEME.getIri()+"> <"+SNOMED.GRAPH_SNOMED.getIri()+">.\n"+
           "?concept <"+IM.CODE.getIri()+"> ?snomed}");
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



   private static Map<String, List<String>> importReadToSnomedRdf4j(Map<String, List<String>> readToSnomed) throws TTFilerException {

      try (RepositoryConnection conn= getGraphConnection()){
     TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed\n"+
       "where {GRAPH <"+IM.GRAPH_VISION.getIri()+"> {?concept <"+IM.CODE.getIri()+"> ?code. \n"+
   "?concept <"+IM.MATCHED_TO.getIri()+"> ?snomed.}}");
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

   public static Map<String,TTEntity> getEMISReadAsVision() throws TTFilerException {
      if (getFilerType()== FilerType.JDBC) {
         throw new TTFilerException("No JDBC version for get emis entities");
      }else {
         return getEMISReadAsVisionRdf4j();
      }

   }

   private static Map<String, TTEntity> getEMISReadAsVisionRdf4j() {
      Map<String,TTEntity> emisRead2= new HashMap<>();
      try (RepositoryConnection conn= getGraphConnection()) {
         StringJoiner sql = new StringJoiner("\n");
         sql.add("SELECT ?code ?name ?snomed");
         sql.add("WHERE {");
         sql.add("Graph <" + IM.GRAPH_EMIS.getIri() + "> {");
         sql.add("?concept <" + IM.CODE.getIri() + "> ?code.");
         sql.add("?concept <" + RDFS.LABEL.getIri() + "> ?name.");
         sql.add("?concept <" + IM.MATCHED_TO.getIri() + "> ?snomed. } }");
         TupleQuery qry = conn.prepareTupleQuery(sql.toString());
         TupleQueryResult rs = qry.evaluate();
         while (rs.hasNext()) {
            BindingSet bs = rs.next();
            String code = bs.getValue("code").stringValue();
            String name = bs.getValue("name").stringValue();
            String snomed = bs.getValue("snomed").stringValue();
            if (isRead(code)) {
               code = (code + ".....").substring(0, 5);
               TTEntity entity = emisRead2.computeIfAbsent(code, k -> new TTEntity());
               entity.setName(name);
               entity.setCode(code);
               entity.setIri(IM.GRAPH_VISION.getIri() + code.replace(".", ""));
               entity.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE + snomed));
            }
         }
      }
      return emisRead2;
   }
   public static Boolean isRead(String s){
      if (s.length()<6)
         return !s.contains("DRG") && !s.contains("SHAPT") && !s.contains("EMIS") && !s.contains("-");
      else
         return false;
   }

   private static Map<String, List<String>> importReadToSnomedJdbc(Map<String, List<String>> readToSnomed) throws SQLException, ClassNotFoundException {
      Connection conn= getConnection();
      String sql = "select vis.code as code,snomed.code as snomed \n"+
          "from entity snomed \n" +
          "join tpl maps on maps.object= snomed.dbid\n" +
          "join entity p on maps.predicate=p.dbid\n" +
          "join entity vis on maps.subject=vis.dbid\n" +
          "where snomed.iri like '"+ SNOMED.NAMESPACE+"%'\n"+
          "and p.iri=?\n" +
          "and vis.iri like 'http://endhealth.info/VISION#%'";
      try (PreparedStatement getR2Matches= conn.prepareStatement(sql)) {
          getR2Matches.setString(1, RDFS.SUBCLASSOF.getIri());
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

   private static Map<String,Set<String>> importEmisToSnomedRdf4j() throws TTFilerException {
      Map<String,Set<String>> emisToSnomed= new HashMap<>();
      RepositoryConnection conn= getGraphConnection();
      TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed  ?name\n"+
        "where {GRAPH <"+IM.GRAPH_EMIS.getIri()+"> \n"+
          "{?concept <"+IM.CODE.getIri()+"> ?code. \n"+
        "?concept <"+RDFS.LABEL.getIri()+"> ?name.\n"+
        "?concept <"+IM.MATCHED_TO.getIri()+"> ?snomedIri.}\n" +
        "GRAPH <"+SNOMED.GRAPH_SNOMED.getIri()+"> {"+
        "?snomedIri <"+IM.CODE.getIri()+"> ?snomed.}}");
      try {
         TupleQueryResult rs= qry.evaluate();
         while (rs.hasNext()){
            BindingSet bs= rs.next();
            String read= bs.getValue("code").stringValue();
            String snomed= bs.getValue("snomed").stringValue();
            Set<String> maps = emisToSnomed.computeIfAbsent(read, k -> new HashSet<>());
            maps.add(snomed);
         }
         return emisToSnomed;

      } catch (RepositoryException e){
         throw new TTFilerException("unable to retrieve vision/read "+e);
      }
   }

   private static Map<String,Set<String>> importEmisToSnomedJDBC() throws SQLException, ClassNotFoundException {
      Map<String,Set<String>> emisToSnomed= new HashMap<>();

      String sql = "SELECT entity.code as code,snomed.code as snomed,entity.name as name\n" +
          "from entity\n" +
          "join tpl on tpl.subject= entity.dbid\n" +
          "join entity snomed on tpl.object= snomed.dbid\n" +
          "join entity matchTo on tpl.predicate=matchTo.dbid\n" +
          "where entity.scheme='http://endhealth.info/emis#'\n" +
          "and snomed.scheme='http://snomed.info/sct#'"+
          "and matchTo.iri='http://endhealth.info/im#matchedTo'";

      try (Connection conn = getConnection();
         PreparedStatement getEMIS= conn.prepareStatement(sql)) {
          ResultSet rs = getEMIS.executeQuery();
          while (rs.next()) {
              String emis = rs.getString("code");
              String snomed = rs.getString("snomed");
              emisToSnomed.computeIfAbsent(emis, k -> new HashSet<>());
              emisToSnomed.get(emis).add(snomed);
          }
      }
      return emisToSnomed;
   }

   private static Set<String> importSnomedJDBC(Set<String> snomedCodes) throws  SQLException, ClassNotFoundException {
       try (Connection conn = getConnection();
            PreparedStatement getSnomed = conn.prepareStatement("SELECT code from entity where iri like 'http://snomed.info/sct%'")) {
           ResultSet rs = getSnomed.executeQuery();
           while (rs.next())
               snomedCodes.add(rs.getString("code"));
           if (snomedCodes.isEmpty()) {
               System.err.println("Snomed must be loaded first");
               System.exit(-1);
           }
       } catch (SQLException e) {
           throw new RepositoryException("unable to retrieve snomed codes");
       }
       return snomedCodes;
   }

   private static boolean isWindows() {
      return (System.getProperty("os.name").toLowerCase().contains("win"));
   }

   private static String getFilePath(String path) {
       return isWindows() ? path : path.replace("/", "\\");
   }

}
