package org.endeavourhealth.informationmanager;

public class TTFilerFactory {
    public static TTDocumentFiler getDocumentFiler() throws TTFilerException {
        return new TTDocumentFilerJDBC();
    }
}
