package org.endeavourhealth.informationmanager.transforms;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.endeavourhealth.informationmanager.transforms.ImportUtils.getConnection;

public class SearchTermGenerator {
    public static void generateSearchTerms(String outpath) throws Exception {
        System.out.println("Generating search terms");

        String outFile = outpath + "/searchTerms.txt";

        try (FileWriter fw = new FileWriter(outFile)) {
            writeTerms(fw, "SELECT DISTINCT code AS term, dbid AS entity FROM entity WHERE code IS NOT NULL");
            writeTerms(fw, "SELECT DISTINCT term, entity FROM term_code WHERE term IS NOT NULL");
            writeTerms(fw, "SELECT DISTINCT code AS term, entity FROM term_code WHERE code IS NOT NULL");
        }

        importSearchTerms(outFile);
    }

    private static void writeTerms(FileWriter fw, String sql) throws Exception {
        System.out.println("Reading search terms...");
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                fw.write(rs.getString("entity") + "\t" + rs.getString("term") + "\r\n");
            }
        }
    }

    private static void importSearchTerms(String filename) throws Exception {
        System.out.println("Importing search terms");
        try (Connection conn = getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE entity_search")) {
                stmt.executeUpdate();
            }


            try (PreparedStatement stmt = conn.prepareStatement("SET GLOBAL local_infile=1")) {
                stmt.executeUpdate();
            }

            conn.setAutoCommit(false);

            try (PreparedStatement stmt = conn.prepareStatement("LOAD DATA LOCAL INFILE ?"
                + " IGNORE INTO TABLE entity_search"
                + " FIELDS TERMINATED BY '\t'"
                + " LINES TERMINATED BY '\r\n'"
                + " (entity_dbid, term)")) {
                stmt.setString(1, filename);

                stmt.executeUpdate();
                conn.commit();
            }
        }
    }
}
