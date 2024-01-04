package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.util.Map;

public class CoreVerbImporter implements TTImport {
	private TTDocument document;
	@Override
	public void importData(TTImportConfig ttImportConfig) throws Exception {
		TTManager manager = new TTManager();
		document = manager.createDocument();
		verbs();
		ownerships();
			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}

	}

	private void verbs() {


	}

	private void ownerships() {
	}

	private void addEntity(String iri, Map<String,String> keyValue,String superClass){
		TTEntity entity= new TTEntity()
			.setIri(IM.NAMESPACE+iri)
			.setName(iri);
		keyValue.entrySet().stream().forEach(e->
				entity.set(TTIriRef.iri(IM.NAMESPACE+e.getKey()),TTLiteral.literal(e.getValue())));
		document.addEntity(entity);
	}

	@Override
	public void validateFiles(String s) throws TTFilerException {

	}

	@Override
	public void close() throws Exception {

	}
}
