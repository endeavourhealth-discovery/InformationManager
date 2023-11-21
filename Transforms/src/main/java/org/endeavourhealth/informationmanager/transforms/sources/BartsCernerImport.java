package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class BartsCernerImport implements TTImport {
    private static final Logger LOG = LoggerFactory.getLogger(BartsCernerImport.class);

	private static final String[] used = {".*\\\\Barts\\\\Barts-Cerner-Codes.txt"};
	private static final String[] codes = {".*\\\\Barts\\\\V500_event_code.txt"};
	private static final String[] sets = {".*\\\\Barts\\\\V500_Event_Set_Code.txt"};
	private static final String[] hierarchy = {".*\\\\Barts\\\\V500_event_set_canon.txt"};
	private static final String[] maps = {".*\\\\Barts\\\\Snomed-Barts-Cerner.txt"};

    private static final String BARTS_CERNER_CODES = IM.CODE_SCHEME_BARTS_CERNER.getIri()+"BartsCernerCodes";
    private static final String UNCLASSIFIED = IM.CODE_SCHEME_BARTS_CERNER.getIri()+"UnClassifiedBartsCernerCode";

	private final Map<String, TTEntity> codeToConcept= new HashMap<>();
	private final Map<String,TTEntity> codeToSet= new HashMap<>();
	private final Map<String,TTEntity> termToSet= new HashMap<>();
	private final Set<TTEntity> usedSets= new HashSet<>();
	Map<String,Set<String>> childToParent= new HashMap<>();
	private TTDocument document;


	private final TTManager manager= new TTManager();

	private final Map<String,TTEntity> entityMap = new HashMap<>();


	@Override
	public void importData(TTImportConfig config) throws Exception {
		LOG.info("retrieving snomed codes from IM");
		document= manager.createDocument(IM.GRAPH_BARTS_CERNER.getIri());
		document.addEntity(manager.createGraph(IM.GRAPH_BARTS_CERNER.getIri(),"Barts Cerner code scheme and graph"
		,"The Barts Cerner local code scheme and graph i.e. local codes with links to cor"));
		importSets(config.getFolder());
		importHierarchy(config.getFolder());
		importCodes(config.getFolder());
		importUsed(config.getFolder());
		setUsedEventSets();
		setTopLevel();
		importMaps(config.getFolder());

        try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
            filer.fileDocument(document);
        }
	}

	private void setUsedEventSets() {
		Set<TTEntity> doneAlready= new HashSet<>();
		for (TTEntity eventSet:usedSets){
			document.addEntity(eventSet);
			setParentSet(eventSet,doneAlready);
		}
	}

	private void setParentSet(TTEntity childSet,Set<TTEntity> doneAlready){
		String childCode= childSet.getCode();
		if (childToParent.get(childCode)==null)
			return;
		for (String parent:childToParent.get(childCode)) {
			TTEntity parentSet = codeToSet.get(parent);
			childSet.addObject(IM.IS_CHILD_OF, iri(parentSet.getIri()));
			if (!doneAlready.contains(parentSet)) {
				doneAlready.add(parentSet);
				document.addEntity(parentSet);
				setParentSet(parentSet, doneAlready);
			}
		}
	}

	private void importMaps(String inFolder) throws IOException, DataFormatException {
		int count = 0;
		for (String conceptFile : maps) {
			Path file =  ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			LOG.info("Processing  Snomed maps {}", file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();  // NOSONAR - Skipping CSV header line
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
					String[] fields = line.split("\t");
					String code = fields[0];
					String iri= IM.CODE_SCHEME_BARTS_CERNER.getIri()+code;
					String snomed = fields[2];
					TTEntity barts=codeToConcept.get(code);
					if (snomed.contains("1000252"))
						barts.addObject(IM.MATCHED_TO,TTIriRef.iri(IM.NAMESPACE+snomed));
					else
						barts.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE+snomed));
					line = reader.readLine();
					}
				}
			}
		LOG.info("Imported {} maps", count);
	}


	private void setTopLevel() {
		TTEntity topConcept= new TTEntity()
			.setIri(BARTS_CERNER_CODES)
			.addType(IM.CONCEPT)
			.setName("Barts Cerner codes")
			.setCode("BartsCernerCodes")
			.setScheme(IM.GRAPH_BARTS_CERNER)
			.setDescription("The Cerner codes used in Barts NHS Trust Millennium system");
		topConcept.addObject(IM.IS_CHILD_OF,iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
		document.addEntity(topConcept);
		TTEntity unmatchedConcept= new TTEntity()
			.setIri(UNCLASSIFIED)
			.addType(IM.CONCEPT)
			.setName("Unclassified Barts Cerner codes")
			.setDescription("The Cerner codes used in Barts NHS Trust Millennium system"
				+"that have not yet been placed in the Barts event set hierarchy");
		unmatchedConcept.addObject(IM.IS_CHILD_OF,iri(BARTS_CERNER_CODES));
		document.addEntity(unmatchedConcept);
	}


	private void importHierarchy(String inFolder) throws IOException {
		int count = 0;
		for (String conceptFile : hierarchy) {
			Path file =  ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			LOG.info("Processing  cerner event set V500 canon in ", file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();  // NOSONAR - Skipping CSV header line
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
                    processHierarchyLine(line);
                    line = reader.readLine();
				}
			}
		}

		for (Map.Entry<String,TTEntity> entry:codeToSet.entrySet()){
			String code=entry.getKey();
			if (childToParent.get(code)==null)
				entry.getValue().addObject(IM.IS_CHILD_OF,iri(BARTS_CERNER_CODES));

		}


		LOG.info("Imported {} hierarchy links", count);


	}

    private void processHierarchyLine(String line) {
        String[] fields= line.split("\t");
        String parent= fields[0];
        String child= fields[2];
        if (parent.equals(child))
            LOG.info("? top level {} {}", parent, fields[1]);
        if (codeToSet.get(parent)==null)
            LOG.info("missing event set cd {} {}", parent, fields[1]);
        Integer order= Integer.parseInt(fields[4]);
        TTEntity eventSet= codeToSet.get(child);
        eventSet.addObject(IM.IS_CHILD_OF,iri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+parent));
        eventSet.set(IM.DISPLAY_ORDER, TTLiteral.literal(order));
        if (childToParent.get(child)==null)
            childToParent.put(child,new HashSet<>());
        childToParent.get(child).add(parent);
    }

    private void importUsed(String inFolder) throws Exception {
		int count = 0;
		for (String conceptFile : used) {
			Path file =  ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			LOG.info("Processing cerner event codes in {}", file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();  // NOSONAR - Skipping CSV header line
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
                    importUsedLine(line);
                    line = reader.readLine();
				}
			}
		}
		LOG.info("Imported {} codes from look up", count);

	}

    private void importUsedLine(String line) throws Exception {
        String[] fields= line.split("\t");
        String code= fields[7];
        if (codeToSet.get(code)!=null)
            throw new Exception("duplicate event code and set code");
        String iri=IM.CODE_SCHEME_BARTS_CERNER.getIri()+fields[7];
        String term= fields[4].replace("\"","");
        TTEntity usedConcept= codeToConcept.get(code);
        if (usedConcept==null) {
            usedConcept = new TTEntity()
                .setIri(iri)
                .addType(IM.CONCEPT)
                .setCode(code)
                .setScheme(IM.CODE_SCHEME_BARTS_CERNER);
            usedConcept.addObject(IM.IS_CHILD_OF,iri(UNCLASSIFIED));
            document.addEntity(usedConcept);
            codeToConcept.put(code,usedConcept);
        }
        usedConcept.setName(term);
    }


    private void importSets(String inFolder) throws IOException {
		int count = 0;
		for (String conceptFile : sets) {
			Path file =  ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			LOG.info("Processing  cerner event set codes in {}", file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();  // NOSONAR - Skipping CSV header line
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
                    importSetLine(line);
                    line = reader.readLine();
				}
			}
		}
		LOG.info("Imported {} sets", count);


	}

    private void importSetLine(String line) {
        String[] fields= line.split("\t");
        String code= fields[7];
        String term= fields[11].replace("\"","");
        String xterm= term.toLowerCase();
        String iri=IM.CODE_SCHEME_BARTS_CERNER.getIri()+code;
        TTEntity eventSet = new TTEntity()
                .setIri(iri)
                .addType(IM.CONCEPT)
                .setName(term)
                .setCode(code)
                .setScheme(IM.CODE_SCHEME_BARTS_CERNER);
        codeToSet.put(code, eventSet);
        termToSet.put(xterm,eventSet);
    }

    private void importCodes(String inFolder) throws Exception {
		int count = 0;
		for (String conceptFile : codes) {
			Path file =  ImportUtils.findFilesForId(inFolder, conceptFile).get(0);
			LOG.info("Processing cerner event codes in {}", file.getFileName().toString());
			try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
				reader.readLine();  // NOSONAR - Skipping CSV header line
				String line = reader.readLine();
				while (line != null && !line.isEmpty()) {
					count++;
                    importCodeLine(line);
                    line = reader.readLine();
				}
			}
		}
		LOG.info("Imported {} codes from look up", count);


	}

    private void importCodeLine(String line) throws Exception {
        String[] fields= line.split("\t");
        String code= fields[0];
        if (codeToSet.get(code)!=null)
            throw new Exception("duplicate code used for code and set");
        String term= fields[3].replace("\"","");
        String setTerm = fields[15].toLowerCase().replace("\"","");
        TTEntity eventSet=termToSet.get(setTerm);
        String iri=IM.CODE_SCHEME_BARTS_CERNER.getIri()+code;
        TTEntity codeConcept= new TTEntity()
            .setIri(iri)
            .addType(IM.CONCEPT)
            .setCode(code)
            .setScheme(IM.CODE_SCHEME_BARTS_CERNER);
		if(term.equals("")){
			codeConcept.setName("no name assigned");
		} else {
			codeConcept.setName(term);
		}
        TTEntity parentSet=null;
        if (eventSet!=null) {
            Set<String> parents = childToParent.get(eventSet.getCode());
            if (parents != null) {
                for (String parent:parents) {
                    parentSet = codeToSet.get(parent);
                    codeConcept.addObject(IM.IS_CHILD_OF, iri(parentSet.getIri()));
                    usedSets.add(parentSet);
                }
            } else
                codeConcept.addObject(IM.IS_CHILD_OF,iri(UNCLASSIFIED));
        }
        else
            codeConcept.addObject(IM.IS_CHILD_OF,iri(UNCLASSIFIED));
        document.addEntity(codeConcept);
        codeToConcept.put(code,codeConcept);
    }


    @Override
	public void validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder, maps, used);
    }

    @Override
    public void close() throws Exception {
        codeToConcept.clear();
        codeToSet.clear();
        termToSet.clear();
        usedSets.clear();
        childToParent.clear();
        entityMap.clear();
        manager.close();
    }
}
