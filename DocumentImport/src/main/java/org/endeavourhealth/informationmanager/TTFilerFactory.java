package org.endeavourhealth.informationmanager;

import org.endeavourhealth.informationmanager.jdbc.TTDocumentFilerJDBC;
import org.endeavourhealth.informationmanager.rdf4j.TTDocumentFilerRdf4j;
// import org.endeavourhealth.informationmanager.rdf4j.TTDocumentFilerRdf4j;

public class TTFilerFactory {
    public static boolean skipDeletes=false;
    private TTFilerFactory() {}

    public static TTDocumentFiler getDocumentFiler() throws TTFilerException {
        return new TTDocumentFilerRdf4j();
        //return new TTDocumentFilerJDBC();
    }
}
