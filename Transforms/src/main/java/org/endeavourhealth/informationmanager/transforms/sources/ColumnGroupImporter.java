package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.NAMESPACE;
import org.endeavourhealth.informationmanager.transforms.models.ColumnGroups;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.imapi.model.imq.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class ColumnGroupImporter implements TTImport {
	private static final String[] columnGroups = {
		".*\\\\DiscoveryCore\\\\ColumnGroups.json"};
	private static final Logger LOG = LoggerFactory.getLogger(ColumnGroupImporter.class);


	@Override
	public void importData(TTImportConfig config) throws ImportException {
		try (TTManager manager = new TTManager()) {
			TTDocument document= manager.createDocument();
			for (String groupDocument : columnGroups) {
				Path path = ImportUtils.findFileForId(config.getFolder(), groupDocument);
				File file = path.toFile();
				ObjectMapper mapper = new ObjectMapper();
				ColumnGroups groups = mapper.readValue(file, ColumnGroups.class);
				for (Query columnGroup : groups.getColumnGroups()) {
					StringBuilder name = new StringBuilder(columnGroup.getName());
					String groupIri = NAMESPACE.IM + "ColumnGroup_" + (name.toString().replace(" ", "_").toLowerCase());
					name.append(" (column group)");
					Query newGroup = new Query();
					newGroup.setTypeOf(columnGroup.getTypeOf());
					newGroup.setName(name.toString());
					newGroup.setPath(columnGroup.getPath());
					newGroup.setReturn(columnGroup.getReturn());
					newGroup.setOrderBy(columnGroup.getOrderBy());
					TTEntity columnGroupEntity = new TTEntity()
						.setIri(groupIri)
						.setName(name.toString())
						.addType(iri(IM.QUERY))
						.setScheme(iri(NAMESPACE.IM))
						.addObject(iri(IM.IS_CONTAINED_IN), iri(NAMESPACE.IM + "ColumnGroups"))
						.set(iri(IM.DEFINITION), TTLiteral.literal(newGroup));
					document.addEntity(columnGroupEntity);
				}
				LOG.info("Filing {}", file);
				try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
					try {
						filer.fileDocument(document);
					} catch (TTFilerException | QueryException e) {
						throw new IOException(e.getMessage());
					}
				}
			}
		}
		catch(Exception ex) {
			throw new ImportException(ex.getMessage(),ex);
		}

	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {

	}

	@Override
	public void close() throws Exception {

	}
}
