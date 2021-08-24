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
		FileWriter expansions= new FileWriter(path+"\\ConceptSetExpansions.txt");
		FileWriter legacies= new FileWriter(path+"\\ConceptSetLegacyExpansions.txt");
		FileWriter subsets= new FileWriter(path+"\\ConceptSetHierarchy.txt");subsets.write("Concept set iri\tConcept set name\tSubset iri\tSubset name\n");
		definitions.write("Concept set iri\tConcept set name\tSet definition ECL\tSet definition json-LD\n");
		expansions.write("Concept set iri\tConcept set name\tCode\tScheme\tMember iri\n");
		legacies.write("Concept set iri\tConcept set name\tCode\tScheme\tim1 concept dbid\n");
		SetService setService= new SetService();
		Set<TTEntity> conceptSets= setService.getAllSets(type);


		for (TTEntity conceptSet:conceptSets) {
			String setIri = conceptSet.getIri();
			System.out.println("Exporting " + setIri + ": " + conceptSet.getName() + "..");

			if (conceptSet.get(IM.HAS_SUBSET) != null) {
				for (TTValue value : conceptSet.get(IM.HAS_SUBSET).asArray().getElements()) {
					subsets.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + value.asIriRef().getIri() + "\t" + value.asIriRef().getName() + "\n");
				}
			} else if (conceptSet.get(IM.HAS_MEMBER) != null) {
				ObjectMapper objectMapper = new ObjectMapper();
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
				objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
				String json = objectMapper.writeValueAsString(conceptSet);
				TTToECL eclConverter = new TTToECL();
				String ecl = eclConverter.getConceptSetECL(conceptSet, null);
				definitions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + ecl + "\t" + json + "\n");

				TTEntity expanded = setService.getExpansion(conceptSet, false);
				for (TTValue value : expanded.get(IM.HAS_MEMBER).asArray().getElements()) {
					TTEntity member = (TTEntity) value.asNode();
					String code = member.getCode();
					String scheme = member.getScheme().getIri();
					expansions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + code + "\t" + scheme + "\t" + member.getIri() + "\n");
				}
				TTEntity legacy = setService.getExpansion(conceptSet, true);
				for (TTValue value : legacy.get(IM.HAS_MEMBER).asArray().getElements()) {
					TTEntity member = (TTEntity) value.asNode();
					String code = member.getCode();
					String scheme = member.getScheme().getIri();
					String im1Dbid = member.get(IM.IM1_DBID).asLiteral().getValue();
					expansions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + code + "\t" + scheme + "\t" + member.getIri() + "\n");
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
