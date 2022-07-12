package org.endeavourhealth.informationmanager.utils.sets;

import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;

public class Main {

    public static void main(String[] args) throws Exception {

        TTImportConfig cfg = new TTImportConfig();
        String arg=args[0].toLowerCase();
        if (arg.startsWith("source=")) {
            cfg.setFolder(arg.split("=")[1]);
            TTBulkFiler.setConfigTTl(arg.split("=")[1]);
        }
        new SetsTypeFixer().importData(cfg);

    }



}
