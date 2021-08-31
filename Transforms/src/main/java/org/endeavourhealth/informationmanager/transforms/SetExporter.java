package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTNode;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.common.transform.TTToECL;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SetExporter {
	/**
	 * Exports all set  definitions and expansions on the database
	 * @param path  the output folder to place the output
	 * @param type  IRI of the set type
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public void exportAll(String path, TTIriRef type) throws SQLException, ClassNotFoundException, IOException {

		FileWriter definitions= new FileWriter(path+"\\ConceptSetDefinitions.txt");
		FileWriter expansions= new FileWriter(path+"\\ConceptSetCoreExpansions.txt");
		FileWriter legacies= new FileWriter(path+"\\ConceptSetLegacyExpansions.txt");
		FileWriter subsets= new FileWriter(path+"\\ConceptSetHierarchy.txt");
		subsets.write("Parent set iri\tParent set name\tChild set iri\tChild set name\n");
		FileWriter im1maps= new FileWriter(path+"\\IM1Map.txt");
		definitions.write("Set iri\tSet name\tSet definition ECL\tSet definition json-LD\n");
		expansions.write("Set iri\tSetName\tCore member code\tScheme\tCore member name\tiri\n");
		legacies.write("Set iri\tSet name\tLegacy member code\tScheme\tLegacy member name\tIri\n");
		im1maps.write("Set iri\tIM1 dbid\tMember IM2 iri\n");
		SetService setService= new SetService();
		Set<TTEntity> conceptSets= setService.getAllSets(type);
		Map<String,Integer> setdbids= new HashMap<>();
		Set<String> conceptIris= new HashSet<>();


		for (TTEntity conceptSet:conceptSets) {
			String setIri = conceptSet.getIri();
			setdbids.put(conceptSet.getIri(),conceptSet.get(IM.DBID).asLiteral().intValue());
			System.out.println("Exporting " + setIri+ "..");
			if (conceptSet.get(IM.HAS_MEMBER) != null) {
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String json = objectMapper.writeValueAsString(conceptSet);
				TTToECL eclConverter = new TTToECL();
				String ecl = eclConverter.getConceptSetECL(conceptSet, null);
				definitions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + ecl + "\t" + json + "\n");

				TTEntity expanded = setService.getExpansion(conceptSet);
				for (TTValue value : expanded.get(IM.HAS_MEMBER).asArray().getElements()) {
					TTEntity member = (TTEntity) value.asNode();
					String code = member.getCode();
					String scheme = member.getScheme().getIri();
					expansions.write(
						conceptSet.getIri()+"\t"+ conceptSet.getName()+"\t"+ code + "\t" + scheme + "\t" + member.getName()+"\t"+member.getIri() + "\n");
				}
				TTEntity legacy = setService.getLegacyExpansion(conceptSet);
				if (legacy.get(IM.HAS_MEMBER)!=null) {
					for (TTValue value : legacy.get(IM.HAS_MEMBER).asArray().getElements()) {
						TTEntity member = (TTEntity) value.asNode();
						String code = member.getCode();
						String scheme = member.getScheme().getIri();
						legacies.write(
							conceptSet.getIri()+"\t"+conceptSet.getName()+"\t"+ code + "\t" + scheme + "\t" + member.getName()+"\t"+member.getIri() + "\n");
					}
				}
				TTEntity im1 = setService.getIM1Expansion(conceptSet);
				if (im1.get(IM.HAS_MEMBER)!=null){
					for (TTValue value : im1.get(IM.HAS_MEMBER).asArray().getElements()) {
						TTEntity member = (TTEntity) value.asNode();
						String code = member.getCode();
						String scheme = member.getScheme().getIri();
						String im1id = member.get(TTIriRef.iri(IM.NAMESPACE + "im1dbid")).asLiteral().getValue();
						im1maps.write(conceptSet.getIri()+"\t" + im1id + "\t"+ member.getIri()+"\n");
					}
				}
			}
		}
		for (TTEntity conceptSet:conceptSets){
			String setIri = conceptSet.getIri();

			if (conceptSet.get(IM.HAS_SUBSET) != null) {
				System.out.println("Exporting subset " + setIri + "..");

				for (TTValue value : conceptSet.get(IM.HAS_SUBSET).asArray().getElements()) {
					subsets.write(conceptSet.getIri() + "\t" + conceptSet.getName()+"\t"+
						value.asIriRef().getIri()+ "\t"+ value.asIriRef().getName()+"\n");
				}
			}
		}

		definitions.flush();
		definitions.close();
		subsets.flush();
		subsets.close();
		expansions.flush();
		expansions.close();

	}
}
