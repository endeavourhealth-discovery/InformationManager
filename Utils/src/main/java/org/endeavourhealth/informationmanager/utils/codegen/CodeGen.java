package org.endeavourhealth.informationmanager.utils.codegen;

import org.endeavourhealth.imapi.logic.codegen.CodeGenJava;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;

public class CodeGen {
    private static final Logger LOG = LoggerFactory.getLogger(CodeGen.class);

    public static void main(String[] args) throws IOException {

        LOG.info("IMCodeGenerator");

        if (args.length != 1) {
            LOG.error("Requires one arg (filename)");
            System.exit(-1);
        }

        try (PrintWriter os = new PrintWriter(args[0])) {
            new CodeGenJava().generate(os);
        }
    }
}