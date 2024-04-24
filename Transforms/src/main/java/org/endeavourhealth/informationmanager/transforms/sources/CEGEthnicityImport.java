package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.logic.exporters.ImportMaps;
import org.endeavourhealth.imapi.logic.service.SetService;
import org.endeavourhealth.imapi.model.imq.Bool;
import org.endeavourhealth.imapi.model.imq.Match;
import org.endeavourhealth.imapi.model.imq.Node;
import org.endeavourhealth.imapi.model.imq.Query;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class CEGEthnicityImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(CEGEthnicityImport.class);

    private static final String[] lookups = {".*\\\\Ethnicity\\\\Ethnicity_Lookup_v3.txt"};
	private final TTManager manager = new TTManager();
	private TTDocument document;
	private TTDocument nhsDocument;
	private TTManager nhsManager= new TTManager();
	private final Map<String,String> ethnicCensusMap= new HashMap<>();
	private final Map<String,String> raceMap = new HashMap<>();
	private final Map<String,TTEntity> nhsCatmap= new HashMap<>();
	private final Map<String,TTEntity> cegCatMap= new HashMap<>();
	private final Map<String,String> spellMaps= new HashMap<>();
	private final Set<String> dropWords= new HashSet<>();
	private final String UNCLASSIFIED = "unclassified";
	private TTEntity nhsSet;
	private TTEntity cegSet;
	private ImportMaps importMaps = new ImportMaps();
	private SetService setService= new SetService();

	Map<String,Set<String>> census2001;


	@Override
	public void importData(TTImportConfig config) throws Exception {

		document = manager.createDocument(GRAPH.CEG16);
		nhsDocument= nhsManager.createDocument(GRAPH.NHSDD_ETHNIC_2001);
		document.addEntity(manager.createGraph(GRAPH.NHSDD_ETHNIC_2001,
				"NHS Ethnicity scheme and graph"
				,"NHS Ethnicity scheme and graph"));
		setConceptSetGroups();
		retrieveEthnicity(config.isSecure());
		spellCorrections();
		importEthnicGroups(config.getFolder());

			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(document);
			}

			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
				filer.fileDocument(nhsDocument);
			}

	}


	private void spellCorrections() {
		spellMaps.put("british","british or mixed british");
		spellMaps.put("other black african or caribbean background",
			"other black background - ethnic category 2001 census");
		spellMaps.put("not stated","ethnic category not stated");
		dropWords.add("any");
		dropWords.add("or multiple ethnic");
		dropWords.add("ethnic group");
	}


	private void retrieveEthnicity(boolean secure) throws TTFilerException, IOException {
			census2001= importMaps.getDescendants(SNOMED.NAMESPACE+"92381000000106");
		for (Map.Entry<String,Set<String>> entry:census2001.entrySet()) {
			String snomed = entry.getKey();
			for (String term : entry.getValue()) {
				term=term.toLowerCase();
				term = term.replace(",", " ");
				term=term.replace("  ", " ");
				if (spellMaps.get(term) != null)
					term = spellMaps.get(term);
				ethnicCensusMap.put(term, snomed);
				if (term.contains("(")) {
					term = term.substring(0, term.indexOf("("));
					term = term.substring(0, term.lastIndexOf(" "));
					ethnicCensusMap.put(term,snomed);
				}
				if (term.contains(": ")) {
					term = term.split(": ")[1];
					ethnicCensusMap.put(term, snomed);
				}
				if (term.contains(" - ")) {
					term = term.split(" - ")[0];
					ethnicCensusMap.put(term, snomed);
				}
			}
		}

	}

	private String matchEthnicity(String oterm){
		String term=oterm.toLowerCase().replace("\"","");
		term=term.replace(","," ");
		term=term.replace("  "," ");
		if (UNCLASSIFIED.equals(term))
			return UNCLASSIFIED;
		if (spellMaps.get(term)!=null)
			term=spellMaps.get(term);

		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);

		String sterm=dropSomeWords(term);
		if (ethnicCensusMap.get(sterm)!=null)
			return ethnicCensusMap.get(sterm);
		term=term+" - ethnic category 2001 census (finding)";
		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);
		term=dropSomeWords(term);
		if (ethnicCensusMap.get(term)!=null)
			return ethnicCensusMap.get(term);
		return null;
	}

	private String dropSomeWords(String term) {
		for (String word:dropWords) {
			if ((" " + term + " ").contains((" " + word + " ")))
				term = term.replace(word + " ", "");
		}
		return term;
	}

	private void importEthnicGroups(String folder) throws IOException {

		Path file = ImportUtils.findFileForId(folder, lookups[0]);

        LOG.info("Importing Categories");

		try( BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))){
			reader.readLine();  // NOSONAR - Skipping CSV header line
			int count=0;
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				count++;
                processEthnicGroupLine(count, line);

                line=reader.readLine();

			}
			LOG.info("Process ended with {} terms", count);
		}
	}

    private void processEthnicGroupLine(int count, String line) throws JsonProcessingException {
        String[] fields;
        if(count %50000 == 0){
            LOG.info("Processed {} terms", count);
        }
        fields= line.split("\t");
        String snomed=fields[1];
        String cat16=fields[11];
        String catTerm=fields[12].replace("\"","");
        String nhs16=fields[19];
        String nhsTerm= fields[20].replace("\"","");
        String pdsTerm=fields[22];
        String snoNhs= matchEthnicity(nhsTerm);
        if (snoNhs==null)
            snoNhs= matchEthnicity(pdsTerm);
        TTEntity cegSubset= cegCatMap.get(cat16);
        if (cegSubset==null){
            cegSubset= new TTEntity()
                .setIri(GRAPH.CEG16+"CSET_EthnicCategoryCEG16_"+cat16)
                .addType(iri(IM.CONCEPT_SET))
                .setName("Value set - "+ catTerm)
                .setCode(cat16)
                .setScheme(iri(GRAPH.CEG16))
                .setDescription("QMUL CEG 16+ Ethnic category "+cat16)
				.set(iri(IM.IS_SUBSET_OF),TTIriRef.iri(cegSet.getIri()))
                .set(iri(IM.DEFINITION),TTLiteral.literal(new Query().addMatch(new Match().setBoolMatch(Bool.or))));
            document.addEntity(cegSubset);
            cegCatMap.put(cat16,cegSubset);

        }
				Query cegQuery= cegSubset.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
				cegQuery.getMatch().get(0).match(f->f.setInstanceOf(new Node().setIri(SNOMED.NAMESPACE+snomed)));
				cegSubset.set(iri(IM.DEFINITION),TTLiteral.literal(setService.setQueryLabels(cegQuery)));
        if (cegSubset.get(iri(IM.HAS_TERM_CODE))==null)
            TTManager.addTermCode(cegSubset,catTerm,null);
        if (!UNCLASSIFIED.equals(snoNhs)){
            TTEntity nhsSubset= nhsCatmap.get(snoNhs);
            if (nhsSubset==null) {
                nhsSubset = new TTEntity()
                .setIri(IM.NAMESPACE + "CSET_EthnicCategoryNHS2001_"+nhs16)
				.setCode(nhs16)
                .addType(iri(IM.CONCEPT_SET))
				.setName("Value set - "+ nhsTerm+" (2001 census ethnic category "+nhs16+")")
                .setDescription("NHS Data Dictionary 2001 ethnic category " + nhs16)
				.set(iri(IM.IS_SUBSET_OF),TTIriRef.iri(nhsSet.getIri()))
				.set(iri(IM.DEFINITION),TTLiteral.literal(new Query().addMatch(new Match().setBoolMatch(Bool.or))));
                nhsDocument.addEntity(nhsSubset);
                nhsCatmap.put(snoNhs, nhsSubset);
            }
            if (nhsSubset.get(iri(IM.HAS_TERM_CODE))==null)
                TTManager.addTermCode(nhsSubset,nhsTerm,null);
			Query nhsQuery= nhsSubset.get(iri(IM.DEFINITION)).asLiteral().objectValue(Query.class);
			nhsQuery.getMatch().get(0).match(f->f.setInstanceOf(new Node().setIri(SNOMED.NAMESPACE+snomed)));
			nhsSubset.set(iri(IM.DEFINITION),TTLiteral.literal(setService.setQueryLabels(nhsQuery)));
        }
    }

    private void setConceptSetGroups() {
		cegSet= new TTEntity()
			.setIri(GRAPH.CEG16+"CSET_EthnicCategoryCEG16")
			.addType(iri(IM.CONCEPT_SET))
			.setName("CEG 16+1 Ethnic category (set group)")
			.setDescription("QMUL-CEG categorisations of ethnic groups");
		cegSet.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"EthnicitySets")));
		document.addEntity(cegSet);
		nhsSet= new TTEntity()
			.setIri(IM.NAMESPACE+"CSET_EthnicCategory2001")
			.addType(iri(IM.CONCEPT_SET))
			.setName("Value set - 2001 census Ethnic category (set group")
			.setDescription("NHS Data Dictionary 2001 census based categorisations of ethnic groups");
		nhsSet.set(iri(IM.IS_CONTAINED_IN), new TTArray().add(TTIriRef.iri(IM.NAMESPACE+"EthnicitySets")));
		document.addEntity(nhsSet);
	}

	@Override
	public void validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,lookups);
	}

    @Override
    public void close() throws Exception {
        ethnicCensusMap.clear();
        raceMap.clear();
        nhsCatmap.clear();
        cegCatMap.clear();
        spellMaps.clear();
        dropWords.clear();
        census2001.clear();

        importMaps.close();
        manager.close();
        nhsManager.close();
    }
}



