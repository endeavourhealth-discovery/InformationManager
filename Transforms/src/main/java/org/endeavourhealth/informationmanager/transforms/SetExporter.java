package org.endeavourhealth.informationmanager.transforms;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.TTToECL;
import org.endeavourhealth.imapi.vocabulary.IM;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Set;
import java.util.zip.DataFormatException;

public class SetExporter {
    /**
     * Exports a single set definitions and expansions on the database
     * @param path  the output folder to place the output
     * @param setIri  IRI of the concept set
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    public void exportSingle(String path, String setIri) throws SQLException, ClassNotFoundException, IOException {
        SetService setService = new SetService();


        try (FileWriter definitions = new FileWriter(path + "\\ConceptSetDefinitions.txt");
             FileWriter expansions = new FileWriter(path + "\\ConceptSetCoreExpansions.txt");
             FileWriter legacies = new FileWriter(path + "\\ConceptSetLegacyExpansions.txt");
             FileWriter subsets = new FileWriter(path + "\\ConceptSetHierarchy.txt");
             FileWriter im1maps = new FileWriter(path + "\\IM1Map.txt");) {

            subsets.write("Parent set iri\tParent set name\tChild set iri\tChild set name\n");
            definitions.write("Set iri\tSet name\tSet definition ECL\tSet definition json-LD\n");
            expansions.write("Set iri\tSetName\tCore member code\tScheme\tCore member name\tiri\n");
            legacies.write("Set iri\tSet name\tLegacy member code\tScheme\tLegacy member name\tIri\n");
            im1maps.write("Set iri\tIM1 dbid\tMember IM2 iri\n");

            exportSingle(setIri, setService, definitions, expansions, legacies, subsets, im1maps);

        }
    }

    private void exportSingle(String setIri, SetService setService, FileWriter definitions, FileWriter expansions, FileWriter legacies, FileWriter subsets, FileWriter im1maps) throws SQLException, IOException {
        System.out.println("Exporting " + setIri + "..");

        TTEntity conceptSet = setService.getSetDefinition(setIri);
        if (conceptSet.get(IM.HAS_MEMBER) != null)
            exportMembers(definitions, expansions, legacies, im1maps, setService, conceptSet);

        if (conceptSet.get(IM.HAS_SUBSET) != null)
            exportSubsetWithExpansion(setService, definitions, expansions, legacies, subsets, im1maps, conceptSet, setIri);
    }

    /**
	 * Exports all set  definitions and expansions on the database
	 * @param path  the output folder to place the output
	 * @param type  IRI of the set type
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public void exportAll(String path, TTIriRef type) throws SQLException, ClassNotFoundException, IOException {
        SetService setService = new SetService();
        Set<TTEntity> conceptSets = setService.getAllSets(type);

        try (FileWriter definitions = new FileWriter(path + "\\ConceptSetDefinitions.txt");
             FileWriter expansions = new FileWriter(path + "\\ConceptSetCoreExpansions.txt");
             FileWriter legacies = new FileWriter(path + "\\ConceptSetLegacyExpansions.txt");
             FileWriter subsets = new FileWriter(path + "\\ConceptSetHierarchy.txt");
             FileWriter im1maps = new FileWriter(path + "\\IM1Map.txt");) {

            subsets.write("Parent set iri\tParent set name\tChild set iri\tChild set name\n");
            definitions.write("Set iri\tSet name\tSet definition ECL\tSet definition json-LD\n");
            expansions.write("Set iri\tSetName\tCore member code\tScheme\tCore member name\tiri\n");
            legacies.write("Set iri\tSet name\tLegacy member code\tScheme\tLegacy member name\tIri\n");
            im1maps.write("Set iri\tIM1 dbid\tMember IM2 iri\n");

            for (TTEntity conceptSet : conceptSets) {
                String setIri = conceptSet.getIri();
                System.out.println("Exporting " + setIri + "..");

                if (conceptSet.get(IM.HAS_MEMBER) != null)
                    exportMembers(definitions, expansions, legacies, im1maps, setService, conceptSet);

                if (conceptSet.get(IM.HAS_SUBSET) != null) {
                    exportSubsets(subsets, conceptSet, setIri);
                }
            }
        }
    }


    private void exportMembers(FileWriter definitions, FileWriter expansions, FileWriter legacies, FileWriter im1maps, SetService setService, TTEntity conceptSet) throws IOException, SQLException {
        exportDefinition(definitions, conceptSet);

        exportExpansion(expansions, setService, conceptSet);
        exportLegacy(legacies, setService, conceptSet);
        exportIMv1(im1maps, setService, conceptSet);
    }

    private void exportExpansion(FileWriter expansions, SetService setService, TTEntity conceptSet) throws SQLException, IOException {
        TTEntity expanded = setService.getExpansion(conceptSet);
        for (TTValue value : expanded.get(IM.HAS_MEMBER).asArray().getElements()) {
            TTEntity member = (TTEntity) value.asNode();
            String code = member.getCode();
            String scheme = member.getScheme().getIri();
            expansions.write(
                conceptSet.getIri()+"\t"+ conceptSet.getName()+"\t"+ code + "\t" + scheme + "\t" + member.getName()+"\t"+member.getIri() + "\n");
        }
    }

    private void exportLegacy(FileWriter legacies, SetService setService, TTEntity conceptSet) throws SQLException, IOException {
        TTEntity legacy = setService.getLegacyExpansion(conceptSet);
        if (legacy.get(IM.HAS_MEMBER)!=null) {
            for (TTValue value : legacy.get(IM.HAS_MEMBER).asArray().getElements()) {
                TTEntity member = (TTEntity) value.asNode();
                String code = member.getCode();
                String scheme = member.getScheme().getIri();
                legacies.write(
                    conceptSet.getIri()+"\t"+ conceptSet.getName()+"\t"+ code + "\t" + scheme + "\t" + member.getName()+"\t"+member.getIri() + "\n");
            }
        }
    }

    private void exportIMv1(FileWriter im1maps, SetService setService, TTEntity conceptSet) throws SQLException, IOException {
        TTEntity im1 = setService.getIM1Expansion(conceptSet);
        if (im1.get(IM.HAS_MEMBER)!=null){
            for (TTValue value : im1.get(IM.HAS_MEMBER).asArray().getElements()) {
                TTEntity member = (TTEntity) value.asNode();
                String code = member.getCode();
                String scheme = member.getScheme().getIri();
                String im1id = member.get(TTIriRef.iri(IM.NAMESPACE + "im1dbid")).asLiteral().getValue();
                im1maps.write(conceptSet.getIri()+"\t" + im1id + "\t"+scheme + code+"\n");
            }
        }
    }

    private void exportDefinition(FileWriter definitions, TTEntity conceptSet) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        String json = objectMapper.writeValueAsString(conceptSet);
        TTToECL eclConverter = new TTToECL();
        //ecl only supports snomed and discovery concepts
        try {
            String ecl = eclConverter.getConceptSetECL(conceptSet, null);
            definitions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + ecl + "\t" + json + "\n");
        } catch (DataFormatException e){
            definitions.write(conceptSet.getIri() + "\t" + conceptSet.getName() + "\t" + "" + "\t" + json + "\n");
        }
    }

    private void exportSubsets(FileWriter subsets, TTEntity conceptSet, String setIri) throws IOException {
        System.out.println("Exporting subset " + setIri + "..");

        for (TTValue value : conceptSet.get(IM.HAS_SUBSET).asArray().getElements()) {
            subsets.write(conceptSet.getIri() + "\t" + conceptSet.getName()+"\t"+
                value.asIriRef().getIri()+ "\t"+ value.asIriRef().getName()+"\n");
        }
    }


    private void exportSubsetWithExpansion(SetService setService, FileWriter definitions, FileWriter expansions, FileWriter legacies, FileWriter subsets, FileWriter im1maps, TTEntity conceptSet, String setIri) throws IOException, SQLException {
        System.out.println("Exporting subset " + setIri + "..");

        for (TTValue value : conceptSet.get(IM.HAS_SUBSET).asArray().getElements()) {
            subsets.write(conceptSet.getIri() + "\t" + conceptSet.getName()+"\t"+
                value.asIriRef().getIri()+ "\t"+ value.asIriRef().getName()+"\n");

            exportSingle(value.asIriRef().getIri(), setService, definitions, expansions, legacies, subsets, im1maps);
        }
    }
}
