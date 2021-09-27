package org.endeavourhealth.informationmanager.transforms;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.StringJoiner;

import static org.endeavourhealth.informationmanager.transforms.ImportUtils.getConnection;

public class SearchTermGenerator {
    public static void generateSearchTerms(String outpath, boolean secure) throws Exception {
        System.out.println("Generating search terms");

        try (Connection conn = getConnection()) {
            initEntitySearchTable(conn, secure);

            batchImportTerms(conn, "SELECT DISTINCT code AS term, dbid AS entity FROM entity WHERE code IS NOT NULL", "Entity code", outpath, secure);
            batchImportTerms(conn, "SELECT DISTINCT term, entity FROM term_code WHERE term IS NOT NULL", "Entity term", outpath, secure);
            batchImportTerms(conn, "SELECT DISTINCT code AS term, entity FROM term_code WHERE code IS NOT NULL", "Entity term code", outpath, secure);
        }
    }

    private static void initEntitySearchTable(Connection conn, boolean secure) throws Exception {

        try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE entity_search")) {
            stmt.executeUpdate();
        }

        if (secure) {
            try (PreparedStatement stmt = conn.prepareStatement("SET GLOBAL local_infile=1")) {
                stmt.executeUpdate();
            }
        }
    }

    private static void batchImportTerms(Connection conn, String sql, String type, String outpath, boolean secure) throws Exception {
        int batch = 500000;
        int row = 0;
        String filename = outpath + "/searchTerms.txt";

        System.out.println("Generating " + type + " terms via [" + filename + "]");
        FileWriter fw = new FileWriter(filename);

        try (PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                fw.write(rs.getString("entity") + "\t" + rs.getString("term") + "\r\n");

                if ((++row % batch) == 0 || rs.isLast()) {
                    fw.flush();
                    fw.close();

                    System.out.println("Importing " + row + "...");
                    importTerms(conn, secure, filename);

                    if (!rs.isLast()) {
                        fw = new FileWriter(filename);

                        System.out.println("Generating...");
                    }
                }
            }
        }
        System.out.println("Done");
    }

    private static void importTerms(Connection conn, boolean secure, String filename) throws Exception {
        StringJoiner sql = new StringJoiner("\n")
            .add("LOAD DATA");
        if (secure)
            sql.add("LOCAL");
        sql.add("INFILE ?")
            .add("IGNORE INTO TABLE entity_search")
            .add(" FIELDS TERMINATED BY '\t'")
            .add("LINES TERMINATED BY '\r\n'")
            .add("(entity_dbid, term)");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.setString(1, filename);

            stmt.executeUpdate();
        }
    }
}
