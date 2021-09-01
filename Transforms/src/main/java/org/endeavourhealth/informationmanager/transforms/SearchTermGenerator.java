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

        String outFile = outpath + "/searchTerms.txt";

        try (FileWriter fw = new FileWriter(outFile)) {
            writeTerms(fw, "SELECT DISTINCT code AS term, dbid AS entity FROM entity WHERE code IS NOT NULL", "Entity code");
            writeTerms(fw, "SELECT DISTINCT term, entity FROM term_code WHERE term IS NOT NULL", "Entity term");
            writeTerms(fw, "SELECT DISTINCT code AS term, entity FROM term_code WHERE code IS NOT NULL", "Entity term code");
        }

        importSearchTerms(outFile, secure);
    }

    private static void writeTerms(FileWriter fw, String sql, String type) throws Exception {
        System.out.println("Reading search terms [" + type + "]...");
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                fw.write(rs.getString("entity") + "\t" + rs.getString("term") + "\r\n");
            }
        }
    }

    private static void importSearchTerms(String filename, boolean secure) throws Exception {
        System.out.println("Importing search terms");
        try (Connection conn = getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("TRUNCATE TABLE entity_search")) {
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
                .add("IGNORE INTO TABLE entity_search")
                .add(" FIELDS TERMINATED BY '\t'")
                .add("LINES TERMINATED BY '\r\n'")
                .add("(entity_dbid, term)");

            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                stmt.setString(1, filename);

                stmt.executeUpdate();
                conn.commit();
            }
        }
    }
}
