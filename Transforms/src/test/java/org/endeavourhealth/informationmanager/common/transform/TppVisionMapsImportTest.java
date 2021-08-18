package org.endeavourhealth.informationmanager.common.transform;

import org.endeavourhealth.informationmanager.transforms.TPPImporter;
import org.endeavourhealth.informationmanager.transforms.VisionImport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import static org.junit.jupiter.api.Assertions.*;

public class TppVisionMapsImportTest {

    private VisionImport visionImport;
    private TPPImporter tppImporter;

    @BeforeEach
    void setup(){
        visionImport = new VisionImport();
        tppImporter = new TPPImporter();
    }

    @Test
    void readQuotedCSVLine_newLines() throws IOException {
        String line = "\"4....\",\"Action Comment: ASK FOR NOTES";
        Reader stringReader = new StringReader("Action Outcome: Message viewed\",0,1,\"2020-04-23 01:05:00\"");
        BufferedReader reader = new BufferedReader(stringReader);
        String[] actual = visionImport.readQuotedCSVLine(reader,line);
        assertEquals("\"4....\"",actual[0]);
        assertEquals("\"Action Comment: ASK FOR NOTES\nAction Outcome: Message viewed\"",actual[1]);
    }

    @Test
    void readQuotedCSVLine_comas() throws IOException {
        String line = "\"12C1.\",\"FH: Hypertension (Father,Mother,Sister,Brother)\",0,1,\"2020-10-08 01:05:00\"";
        Reader stringReader = new StringReader("");
        BufferedReader reader = new BufferedReader(stringReader);
        String[] actual = visionImport.readQuotedCSVLine(reader,line);
        assertEquals("\"12C1.\"",actual[0]);
        assertEquals("\"FH: Hypertension (Father,Mother,Sister,Brother)\"",actual[1]);
    }

    @Test
    void readQuotedCSVLine_quotes() throws IOException {
        String line = "\"12C1.\",\"FH: Hypertension (Father,\"Mother\",Sister,Brother)\",0,1,\"2020-10-08 01:05:00\"";
        Reader stringReader = new StringReader("");
        BufferedReader reader = new BufferedReader(stringReader);
        String[] actual = visionImport.readQuotedCSVLine(reader,line);
        assertEquals("\"12C1.\"",actual[0]);
        assertEquals("\"FH: Hypertension (Father,\"Mother\",Sister,Brother)\"",actual[1]);
    }

    @Test
    void readQuotedCSVLine_comas_tpp() {
        String line = "\"2BQ1.\",\"O/E - cranial nerves 3,4,6 -OK\",\"2021-04-18 06:04:00\"";
        String[] actual = tppImporter.readQuotedCSVLine(line);
        assertEquals("\"2BQ1.\"",actual[0]);
        assertEquals("\"O/E - cranial nerves 3,4,6 -OK\"",actual[1]);
    }

    @Test
    void readQuotedCSVLine_quotes_tpp() {
        String line = "\"2BQ1.\",\"O/E - \"cranial\" nerves 3,4,6 -OK\",\"2021-04-18 06:04:00\"";
        String[] actual = tppImporter.readQuotedCSVLine(line);
        assertEquals("\"2BQ1.\"",actual[0]);
        assertEquals("\"O/E - \"cranial\" nerves 3,4,6 -OK\"",actual[1]);
    }


}
