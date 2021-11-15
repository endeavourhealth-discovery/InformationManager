package org.endeavourhealth.informationmanager;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class OntologyImport {

    private static final Logger LOG = LoggerFactory.getLogger(OntologyImport.class);

   /**
    * Files an ontology which may or may not be classified.
    * @param inputFile input file containing the ontology in Discovery syntax
    * @throws Exception in the event of a general document filer failure
    */
    public static void fileOntology(File inputFile) throws Exception {

            System.out.println("Importing [" + inputFile + "]");

            LOG.info("Initializing");
            ObjectMapper objectMapper = new ObjectMapper();

            LOG.info("Loading JSON");

            TTDocument document = objectMapper.readValue(inputFile, TTDocument.class);


        try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length != 1) {
            LOG.error("Provide an Information Model json file");
            System.exit(-1);
        }
        File inputFile = new File(argv[0]);
        fileOntology(inputFile);
    }
}
