package org.endeavourhealth.informationmanager.transforms;

import java.io.FileWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.StringJoiner;

import static org.endeavourhealth.informationmanager.transforms.ImportUtils.getConnection;

public class LoadDataTester {
    public static void testLoadData(String outpath, boolean secure) throws Exception {
        /*
        System.out.println("Testing LOAD DATA configuration.");

        String filename = outpath + "/test.tst";

        System.out.println("Generating test file(" + filename + ")");
        try (FileWriter fw = new FileWriter(filename)) {
            fw.write("test\n");
        }

        try (Connection conn = getConnection()) {
            try (PreparedStatement stmt = conn.prepareStatement("DROP TABLE IF EXISTS load_data_test;")) {
                stmt.executeUpdate();
            }

            try (PreparedStatement stmt = conn.prepareStatement("CREATE TABLE load_data_test (test VARCHAR(100)) ENGINE=MEMORY;")) {
                stmt.executeUpdate();
            }

            if (secure) {
                try (PreparedStatement stmt = conn.prepareStatement("SET GLOBAL local_infile=1")) {
                    stmt.executeUpdate();
                }
            }

            StringJoiner sql = new StringJoiner("\n")
                .add("LOAD DATA");
            if (secure)
                sql.add("LOCAL");
            sql.add("INFILE ?")
                .add("IGNORE INTO TABLE load_data_test")
                .add("LINES TERMINATED BY '\n'")
                .add("(test)");

            try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
                stmt.setString(1, filename);
                stmt.executeUpdate();
                System.out.println("LOAD DATA correctly configured.");
            } catch (Exception e) {
                String withness = secure ? "without" : "with";
                System.err.println("Incorrect LOAD DATA config, try " + withness + " secure flag and/or 'allowLoadLocalInfile=true' in JDBC url");
                throw e;
            }
        }

         */
    }


}