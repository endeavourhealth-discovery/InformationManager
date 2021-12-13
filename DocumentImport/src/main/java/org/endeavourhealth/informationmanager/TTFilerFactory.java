package org.endeavourhealth.informationmanager;

import org.endeavourhealth.informationmanager.jdbc.ClosureGeneratorJDBC;
import org.endeavourhealth.informationmanager.rdf4j.ClosureGeneratorRdf4j;
import org.endeavourhealth.informationmanager.rdf4j.TTDocumentFilerRdf4j;
// import org.endeavourhealth.informationmanager.rdf4j.TTDocumentFilerRdf4j;

public class TTFilerFactory {
    public static boolean skipDeletes=false;
    private TTFilerFactory() {}

    public static TTDocumentFiler getDocumentFiler() throws TTFilerException {
        return new TTDocumentFilerRdf4j();
        //return new TTDocumentFilerJDBC();
    }

    public static TCGenerator getClosureGenerator() throws TTFilerException {
        return new ClosureGeneratorRdf4j();
        //return new ClosureGeneratorJDBC();
    }
}
