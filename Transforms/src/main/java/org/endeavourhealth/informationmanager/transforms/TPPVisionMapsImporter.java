package org.endeavourhealth.informationmanager.transforms;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.informationmanager.TTDocumentFiler;
import org.endeavourhealth.informationmanager.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.common.transform.TTManager;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public class TPPVisionMapsImporter implements TTImport {

    private static final String[] tppCtv3Lookup = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_lookup_2.csv"};
    private static final String[] tppCtv3ToSnomed = {".*\\\\TPP_Vision_Maps\\\\tpp_ctv3_to_snomed.csv"};
    private static final String[] visionRead2Code = {".*\\\\TPP_Vision_Maps\\\\vision_read2_code"};
    private static final String[] visionRead2toSnomed = {".*\\\\TPP_Vision_Maps\\\\vision_read2_to_snomed_map"};

    private TTDocument document;
    private Connection conn;
    private final TTManager manager= new TTManager();


    @Override
    public TTImport importData(String inFolder, boolean bulkImport, Map<String, Integer> entityMap) throws Exception {
        System.out.println("importing");
        importTppCtv3Lookup(inFolder);


        TTDocumentFiler filer= new TTDocumentFilerJDBC();
        filer.fileDocument(document,bulkImport,entityMap);
        return this;
    }

    private void importTppCtv3Lookup(String folder) throws IOException {

        Path file = ImportUtils.findFileForId(folder, tppCtv3Lookup[0]);
        System.out.println("Importing Tpp Ctv3 Lookup");

        try (CSVReader reader = new CSVReader(new FileReader(file.toFile()))) {
            reader.readNext();
            int count=0;
            String[] fields;
            while ((fields = reader.readNext()) != null) {
                count++;
                if(count%50000 == 0){
                    System.out.println("Processed " + count +" terms");
                }

            }
            System.out.println("Process ended with " + count + " data");
        }
    }

    @Override
    public TTImport validateFiles(String inFolder) {
        return null;
    }

    @Override
    public TTImport validateLookUps(Connection conn) throws SQLException, ClassNotFoundException {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
