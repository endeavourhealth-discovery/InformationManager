package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;

import java.io.File;
import java.nio.file.Path;

public class SchemeInjector {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: SchemeInjector <input-file> <scheme-iri>");
      System.exit(1);
    }
    try (TTManager manager = new TTManager()) {
      Path path = new File(args[0]).toPath();
      manager.loadDocument(path.toFile());
      TTDocument document = manager.getDocument();
      for(TTEntity entity : document.getEntities()) {
        if (!entity.has(IM.HAS_SCHEME))
          entity.set(IM.HAS_SCHEME, Namespace.from(args[1]).asIri());
      }

      manager.setDocument(document);
      manager.saveDocument(path.toFile());
    }
  }
}
