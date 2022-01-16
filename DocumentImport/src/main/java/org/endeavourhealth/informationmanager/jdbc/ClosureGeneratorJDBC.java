package org.endeavourhealth.informationmanager.jdbc;

import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.informationmanager.common.dal.DALHelper;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class ClosureGeneratorJDBC implements TCGenerator {
    private static HashMap<Integer, List<Integer>> parentMap;
    private static HashMap<Integer, List<Closure>> closureMap;
    private static int counter;




    public void generateClosure(String outpath, boolean secure) throws SQLException, IOException, ClassNotFoundException {

        List<TTIriRef> relationships = Arrays.asList(
          RDFS.SUBCLASSOF,
            RDFS.SUBPROPERTYOF,
            IM.IS_CHILD_OF,
            SNOMED.REPLACED_BY,
            IM.HAS_REPLACED
        );

        String outFile = outpath + "/closure.txt";

        try (Connection conn = getConnection()) {
            try (FileWriter fw = new FileWriter(outFile)) {
                for (TTIriRef rel : relationships) {
                    parentMap = new HashMap<>(1000000);
                    closureMap = new HashMap<>(1000000);
                    System.out.println("Generating closure data for [" + rel.getIri() + "]...");
                    Integer predicateDbid = loadRelationships(conn, rel);
                    buildClosure();
                    writeClosureData(fw, predicateDbid);
                }
            }
            importClosure(conn, outpath, secure);
        }
    }

    private static Connection getConnection() throws SQLException, ClassNotFoundException {
        Map<String, String> envVars = System.getenv();

        System.out.println("Connecting to database...");
        String url = envVars.get("CONFIG_JDBC_URL");
        String user = envVars.get("CONFIG_JDBC_USERNAME");
        String pass = envVars.get("CONFIG_JDBC_PASSWORD");
        String driver = envVars.get("CONFIG_JDBC_CLASS");

        if (driver != null && !driver.isEmpty())
            Class.forName(driver);

        Properties props = new Properties();

        props.setProperty("user", user);
        props.setProperty("password", pass);

        Connection connection = DriverManager.getConnection(url, props);

        System.out.println("Done.");
        return connection;
    }


    private static Integer loadRelationships(Connection conn, TTIriRef relationship) throws SQLException {
        System.out.println("Loading relationships...");
        String sql;
        PreparedStatement stmt;
        if (!relationship.equals(IM.HAS_REPLACED)) {
            sql = "select subject as child, object as parent, p.dbid as predicateDbid\n" +
              "from tpl\n" +
              "JOIN entity p ON p.dbid = tpl.predicate\n" +
              "WHERE p.iri = ?\n" +
              "ORDER BY child";
            stmt= conn.prepareStatement(sql);
            DALHelper.setString(stmt,1,relationship.getIri());
        } else{

            sql = "select subject as parent, object as child, p.dbid as predicateDbid\n" +
              "from tpl\n" +
              "JOIN entity p ON p.dbid = tpl.predicate\n" +
              "WHERE p.iri = 'http://snomed.info/sct#370124000'" +
              "ORDER BY child";
            stmt= conn.prepareStatement(sql);

        }
        Integer previousChildId = null;
        Integer predicateDbid = null;
        int entityCount=0;
        try (ResultSet rs = stmt.executeQuery()) {
                List<Integer> parents = null;
                int c = 0;
                while (rs.next()) {
                    if (c++ % 1000 == 0)
                        System.out.print("\rLoaded " + c + " relationships...");

                    if (predicateDbid == null)
                        predicateDbid = rs.getInt("predicateDbid");
                    Integer childId = rs.getInt("child");
                    if (!childId.equals(previousChildId)) {
                        parents = new ArrayList<>();
                        parentMap.put(childId, parents);
                        entityCount++;
                    }
                    parents.add(rs.getInt("parent"));
                    previousChildId = childId;
                }

        }

        System.out.println("\nRelationships loaded for " + parentMap.size() + " entities ("+entityCount);
        if (!relationship.equals(IM.HAS_REPLACED))
            return predicateDbid;
        else {
            sql="SELECT dbid from entity where iri = ?";
            stmt=conn.prepareStatement(sql);
            stmt.setString(1,IM.HAS_REPLACED.getIri());
            try (ResultSet rs = stmt.executeQuery()) {
               if(rs.next()){
                   predicateDbid= rs.getInt("dbid");
                   return predicateDbid;
               }else {
                   return null;
               }
            }
        }
    }

    private static void buildClosure() {
        System.out.println("Generating closures");
        int c = 0;
        for (Map.Entry<Integer, List<Integer>> row : parentMap.entrySet()) {
          //  if (c++ % 1000 == 0)
             //   System.out.print("Generating for child " + c + " / " + parentMap.size());

            Integer childId = row.getKey();
            generateClosure(childId);
        }
        System.out.println(counter+" rows so far");
    }


    private static List<Closure> generateClosure(Integer childId) {
        // Get the parents
        List<Closure> closures = new ArrayList<>();
        closureMap.put(childId, closures);
        counter++;

        // Add self
        closures.add(new Closure()
            .setParent(childId)
            .setLevel(-1)
        );

        List<Integer> parents = parentMap.get(childId);
        if (parents != null) {
            for (Integer parent : parents) {
                // Check do we have its closure?
                List<Closure> parentClosures = closureMap.get(parent);
                if (parentClosures == null) {
                    // No, generate it
                    parentClosures = generateClosure(parent);
                }

                // Add parents closure to this closure
                for (Closure parentClosure : parentClosures) {
                    // Check for existing already

                    if (closures.stream().noneMatch(c -> c.getParent().equals(parentClosure.getParent()))) {
                        closures.add(new Closure()
                            .setParent(parentClosure.getParent())
                            .setLevel(parentClosure.getLevel() + 1)
                        );
                        counter++;
                    }
                }
            }
        }

        return closures;
    }

    private static void writeClosureData(FileWriter fw, Integer predicateDbid) throws IOException {
        int c = 0;

        for (Map.Entry<Integer, List<Closure>> entry : closureMap.entrySet()) {
            if (c++ % 1000 == 0)
                System.out.print("\rSaving entity closure " + c + "/" + closureMap.size());
            for (Closure closure : entry.getValue()) {
                fw.write(closure.getParent() + "\t"
                    + entry.getKey() + "\t"
                    + closure.getLevel() + "\t"
                    + predicateDbid + "\r\n");
            }
        }
        System.out.println();
    }

    private static void importClosure(Connection conn, String outpath, boolean secure) throws SQLException {
        System.out.println("Importing closure");
        try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE tct")) {
            stmt.executeUpdate();
        }

        if (secure) {
            try (PreparedStatement stmt = conn.prepareStatement("SET GLOBAL local_infile=1")) {
                stmt.executeUpdate();
            }
        }

        conn.setAutoCommit(false);

        StringJoiner sql = new StringJoiner("\n")
            .add("LOAD DATA");
        if (secure)
            sql.add("LOCAL");
        sql.add("INFILE ?")
            .add("INTO TABLE tct")
            .add("FIELDS TERMINATED BY '\t'")
            .add("LINES TERMINATED BY '\r\n'")
            .add("(ancestor, descendant, level,type)");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.setString(1, outpath + "/closure.txt");
            stmt.executeUpdate();
            conn.commit();
        }
    }
}
