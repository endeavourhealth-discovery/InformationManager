package org.endeavourhealth.informationmanager.utils.codegen;

import org.endeavourhealth.imapi.logic.codegen.CodeGenJava;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.*;
import java.util.zip.ZipOutputStream;

public class CodeGen {
  private static final Logger LOG = LoggerFactory.getLogger(CodeGen.class);

  public static void main(String[] args) throws IOException {

    LOG.info("IMCodeGenerator");

    if (args.length != 1) {
      LOG.error("Requires one arg (filename)");
      System.exit(-1);
    }

    try (FileOutputStream fos = new FileOutputStream(args[0]);
         BufferedOutputStream bos = new BufferedOutputStream(fos);
         ZipOutputStream result = new ZipOutputStream(bos)) {

      new CodeGenJava().generate(result, Graph.IM);
    }
  }
}