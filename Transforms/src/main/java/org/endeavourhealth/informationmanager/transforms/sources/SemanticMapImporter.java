package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.NAMESPACE;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;


public class SemanticMapImporter {
	private String mapFolder;
	private TTDocument document;
	private String prefix;

	public void importSemanticMaps(TTDocument document,String folder,String mapFolder, String prefix) throws ImportException {
		this.mapFolder= mapFolder;
		this.document= document;
		this.prefix= prefix;
		try {
			Map<String,TTEntity> mapFolders= new HashMap<>();
			Map<String,TTEntity> maps= new HashMap<>();
			importMapFolders(folder,mapFolders);
			importMaps(folder,mapFolders,maps);
			importMapRules(folder,maps);


		} catch (Exception e) {
			throw new ImportException(e.getMessage(),e);
		}
	}

	private void importMaps(String folder, Map<String, TTEntity> maps, Map<String, TTEntity> mapEntries) throws Exception{
		try (BufferedReader reader = new BufferedReader(new FileReader(folder +"/SemanticMaps.txt"))) {
			reader.readLine();
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				line = line.replace("\"", "");
				String[] fields = line.split("\t");
				String mapIri= fields[0];
				String folderIri= fields[1];
				TTEntity mapEntity = mapEntries.get(mapIri);
				if (mapEntity == null) {
					mapEntity = new TTEntity()
						.setIri(mapIri)
						.setScheme(iri(mapIri.substring(0, mapIri.lastIndexOf("#") + 1)))
						.addType(iri(IM.SEMANTIC_MAP));
					mapEntity.addObject(iri(IM.IS_CONTAINED_IN), iri(folderIri));
					mapEntries.put(mapIri, mapEntity);
					document.addEntity(mapEntity);
				}
				line= reader.readLine();
			}
		}
	}

	private void importMapFolders(String folder,Map<String,TTEntity> maps) throws Exception{
		try (BufferedReader reader = new BufferedReader(new FileReader(folder +"/MapFolders.txt"))) {
			reader.readLine();
			String line = reader.readLine();
			while (line != null &&!line.isEmpty()) {
				line = line.replace("\"","");
				String[] fields = line.split("\t");
				String folderIri = fields[0];
				String name= fields[1];
				TTEntity folderEntity = new TTEntity()
					.setIri(folderIri)
					.setName(name)
					.setScheme(iri(folderIri.substring(0,folderIri.lastIndexOf("#")+1)))
					.addType(iri(IM.FOLDER));
				folderEntity.addObject(iri(IM.IS_CONTAINED_IN),iri(mapFolder));
				maps.put(folderIri,folderEntity);
				document.addEntity(folderEntity);
				line= reader.readLine();
			}
		}
	}
		private void importMapRules(String folder,Map<String,TTEntity> maps) throws Exception {
		Map<String,Integer> mapOrder = new HashMap<>();
			try (BufferedReader reader = new BufferedReader(new FileReader(folder +"/MapEntries.txt"))) {
				reader.readLine();
				String line = reader.readLine();
				while (line != null &&!line.isEmpty()) {
					line = line.replace("\"", "");
					String[] fields = line.split("\t");
					String mapIri = fields[0];
					String name= fields[1];
					if (prefix != null)
						name= prefix+" - "+name;
					String rank= fields[2];
					TTEntity map = maps.get(mapIri);
					map.setName(name);
					String sourceType= fields[3];
					String property = fields[4];
					String function = fields[5];
					String sourceSets = fields[6];
					String from = fields[7];
					String to = fields[8];
					String valueProperty = fields[9];
					String targetText = fields[10];
					String defaultText = fields[11];
					String targetValue = fields[12];
					String defaultValue = fields[13];
					if (!defaultText.isEmpty()) {
						map.set(IM.DEFAULT_TEXT, TTLiteral.literal(defaultText));
					}
					if (!defaultValue.isEmpty()) {
						map.set(IM.DEFAULT_VALUE, TTLiteral.literal(defaultValue));
					}
					TTEntity mapEntry = new TTEntity();
					document.addEntity(mapEntry);
					Integer order = mapOrder.getOrDefault(mapIri, 0) + 1;
					mapEntry.setIri(mapIri+"_"+targetText+"_"+order);
					map.set(IM.SOURCE_TYPE, iri(NAMESPACE.IM+sourceType));
					mapOrder.put(mapIri, order);
					mapEntry.addType(iri(IM.MAP_ENTRY));
					mapEntry.setName(map.getName()+" - "+targetText+"_"+order);
					mapEntry.setScheme(map.getScheme());
					mapEntry.set(SHACL.ORDER, TTLiteral.literal(rank));
					mapEntry.addObject(IM.IN_SEMANTIC_MAP.asIri(), iri(mapIri));
					map.addObject(IM.HAS_ENTRY.asIri(), iri(mapEntry.getIri()));
					for (String sourceSet : sourceSets.split(",")) {
						mapEntry.addObject(iri(IM.SOURCE_ENTITY), iri(sourceSet));
					}
					if (!property.isEmpty()) {
						map.set(IM.SOURCE_ENTITY_PROPERTY.asIri(), iri(NAMESPACE.IM + property));
					}
					if (!valueProperty.isEmpty()) {
						map.set(IM.SOURCE_VALUE_PROPERTY.asIri(), iri(NAMESPACE.IM+ valueProperty));
					}
					if (!function.isEmpty()){
						map.set(NAMESPACE.IM+"function", iri(NAMESPACE.IM+function));
					}
					mapEntry.set(IM.TARGET_TEXT, TTLiteral.literal(targetText));
					mapEntry.set(IM.TARGET_VALUE, TTLiteral.literal(Integer.parseInt(targetValue)));
					if (!from.isEmpty()) {
						mapEntry.set(IM.RANGE_FROM, TTLiteral.literal(from));
					}
					if (!to.isEmpty()) {
						mapEntry.set(IM.RANGE_TO, TTLiteral.literal(to));
					}

					line= reader.readLine();
				}
			}
	}


	private void setCase(TTEntity mapEntry,String property,String from,String to,String defaultText) throws JsonProcessingException {
		Query match= null;
		if (mapEntry.get(IM.DEFINITION)==null) {
			match = new Query();
			Return ret = new Return();
			match.addReturn(ret);
			Case case_ = new Case();
			case_.setElse(new Expression().setValue(defaultText));
			ret.setCase(case_);
			mapEntry.set(IM.DEFINITION, TTLiteral.literal(match));;
		}
		else match = mapEntry.get(IM.DEFINITION).asLiteral().objectValue(Query.class);
		Case case_ = match.getReturn().getFirst().getCase();
		When when = new When();
		case_.addWhen(when);
		if (!property.equals("Count")) {
			when.setIri(NAMESPACE.IM + property);
		}
		else {
			when.setFunction(new FunctionClause().setIri(NAMESPACE.IM + "Count"));
		}
		if (!from.isEmpty()&& !to.isEmpty()) {
				Range range = new Range();
				range.setFrom(new Value().setValue(from).setOperator(Operator.gte));
				range.setTo(new Value().setValue(to).setOperator(Operator.lte));
				when.setRange(range);
			}
			else if (!from.isEmpty()) {
				when.setValue(from);
				when.setOperator(Operator.gte);
			}
			else if (!to.isEmpty()) {
				when.setValue(to);
				when.setOperator(Operator.lte);
			}
			mapEntry.addObject(IM.DEFINITION.asIri(), TTLiteral.literal(match));
	}

}
