package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.library.model.imq.QueryException;
import org.endeavourhealth.library.model.tripletree.TTDocument;
import org.endeavourhealth.library.model.tripletree.TTEntity;
import org.endeavourhealth.library.model.tripletree.TTIriRef;
import org.endeavourhealth.library.model.tripletree.TTLiteral;
import org.endeavourhealth.library.vocabulary.GRAPH;
import org.endeavourhealth.library.vocabulary.IM;
import org.endeavourhealth.library.vocabulary.NAMESPACE;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.endeavourhealth.library.model.tripletree.TTIriRef.iri;


public class SemanticMapImporter {

	public void importSemanticMaps(String prefix,String groupPrefix,String file,String mapFolder) throws IOException {
		Map<String, TTEntity> iriToMap= new HashMap<>();
			TTDocument document = new TTDocument();
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null &&!line.isEmpty()) {
					String[] fields = line.split("\t");
					String mapIri = fields[0];
					String entryNumber= fields[1];
					String name = prefix+ "- map "+ fields[3]+ "(semantic map)";
					String mapHeader= fields[2];
					String targetText= fields[4];
					String targetValue= fields[5];
					String description= fields[6];
					String codeGroup = fields[7];
					String sourceEntity= fields[8];
					String defaultText= fields[9];
					String defaultValue= fields[10];
					String property= fields[11];
					String from= null;
					String to= null;
					String sourceType= null;
					if (fields.length >12) {
						from= fields[12];
						if (fields.length >13) to= fields[13];
					}
					if (fields.length >14) {
						sourceType= fields[14];
					}
					if (sourceEntity!=null) {
						TTEntity mapEntity = iriToMap.get(mapIri);
						if (mapEntity == null) {
							mapEntity = new TTEntity()
								.setIri(mapIri)
								.setName(name)
								.addType(iri(IM.SEMANTIC_MAP));
							mapEntity.setScheme(NAMESPACE.QR.asIri());
							mapEntity.set(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(mapFolder));
							mapEntity.set(IM.HAS_MAP_TYPE,iri(IM.DIRECT_MAP));
							iriToMap.put(mapIri, mapEntity);
							document.addEntity(mapEntity);
						}
						setMapEntry(targetText, targetValue, mapEntity, entryNumber,sourceEntity, "Map - "+ mapHeader+" - "+description, property, from, to,sourceType,document);
						if (!defaultText.isEmpty()) {
							mapEntity.set(IM.DEFAULT_TEXT, TTLiteral.literal(defaultText));
							mapEntity.set(IM.DEFAULT_VALUE, TTLiteral.literal(Integer.parseInt(defaultValue)));
						}
					}

					line = reader.readLine();
				}
			}
		try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
			filer.fileDocument(document);
		} catch (QueryException e) {
			throw new RuntimeException(e);
		} catch (TTFilerException e) {
			throw new RuntimeException(e);
		}
	}

	private void setMapEntry(String targetText, String targetValue, TTEntity mapEntity, String entryNumber,String sourceEntity,String description,
													 String property,String from,String to,
													 String sourceType,
													 TTDocument document) {
		TTEntity mapEntry = new TTEntity();
		mapEntry.setIri(mapEntity.getIri() + "_entry_" + entryNumber);
		mapEntry.setScheme(NAMESPACE.QR.asIri());
		mapEntry.setName(description);
		mapEntry.addType(iri(IM.MAP_ENTRY));
		document.addEntity(mapEntry);
		mapEntity.addObject(iri(IM.HAS_MAP_ENTRY), iri(mapEntry.getIri()));
		if (!sourceEntity.isEmpty()) {
			mapEntry.set(IM.SOURCE_ENTITY, iri(sourceEntity));
		}
		mapEntry.set(IM.TARGET_TEXT, TTLiteral.literal(targetText));
		mapEntry.set(IM.TARGET_VALUE, TTLiteral.literal(Integer.parseInt(targetValue)));
		mapEntry.set(IM.SOURCE_PROPERTY, iri(NAMESPACE.IM+property));
		if (from != null &&!from.isEmpty()) {
			mapEntry.set(IM.RANGE_FROM, TTLiteral.literal(from));
		}
		if (to != null&&!to.isEmpty()) {
			mapEntry.set(IM.RANGE_TO, TTLiteral.literal(to));
		}
		if (sourceType != null&&!sourceType.isEmpty()) {
			mapEntry.set(IM.SOURCE_TYPE, iri(sourceType));
		}
	}

}
