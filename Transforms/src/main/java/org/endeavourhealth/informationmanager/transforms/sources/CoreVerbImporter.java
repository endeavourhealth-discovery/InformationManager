package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.library.model.imq.QueryException;
import org.endeavourhealth.library.model.tripletree.TTDocument;
import org.endeavourhealth.library.model.tripletree.TTEntity;
import org.endeavourhealth.library.model.tripletree.TTIriRef;
import org.endeavourhealth.library.model.tripletree.TTLiteral;
import org.endeavourhealth.library.transforms.TTManager;
import org.endeavourhealth.library.vocabulary.GRAPH;
import org.endeavourhealth.library.vocabulary.IM;
import org.endeavourhealth.library.vocabulary.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;

import java.util.List;
import java.util.Map;

public class CoreVerbImporter implements TTImport {
  private TTDocument document;

  @Override
  public void importData(TTImportConfig ttImportConfig) throws ImportException {
    try (TTManager manager = new TTManager()) {
      document = manager.createDocument();
      verbs();
      ownerships();
      try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
        filer.fileDocument(document);
      }
    } catch (Exception e) {
      throw new ImportException(e.getMessage(),e);
    }
  }

  private void verbs() {


  }

  private void ownerships() {
  }

  private void addEntity(String iri, Map<String, String> keyValue, String superClass) {
    TTEntity entity = new TTEntity()
      .setIri(NAMESPACE.IM + iri)
      .setName(iri);
    keyValue.entrySet().stream().forEach(e ->
      entity.set(TTIriRef.iri(NAMESPACE.IM + e.getKey()), TTLiteral.literal(e.getValue())));
    document.addEntity(entity);
  }

  @Override
  public void validateFiles(String s) throws TTFilerException {

  }

  @Override
  public void close() throws Exception {

  }
}
