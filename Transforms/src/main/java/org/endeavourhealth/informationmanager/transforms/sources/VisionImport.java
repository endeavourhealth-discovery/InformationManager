package org.endeavourhealth.informationmanager.transforms.sources;

import com.opencsv.CSVReader;
import org.endeavourhealth.imapi.filer.*;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.common.Logger;
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

public class VisionImport implements TTImport {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(VisionImport.class);
	private static final String[] r2Terms = {".*\\\\READ\\\\Term.csv"};
	private static final String[] r2Desc = {".*\\\\READ\\\\DESC.csv"};
	private static final String[] visionRead2Code = {".*\\\\TPP_Vision_Maps\\\\vision_read2_code.csv"};
	private static final String[] visionRead2toSnomed = {".*\\\\TPP_Vision_Maps\\\\vision_read2_to_snomed_map.csv"};

	private final Map<String,TTEntity> codeToConcept= new HashMap<>();
	private Set<String> snomedCodes;


	private TTDocument document;
	private final Map<String,TTEntity> r2TermIdMap= new HashMap<>();
	private final Set<String> preferredId = new HashSet<>();
	private  Map<String,TTEntity> emisRead;
	private final ImportMaps importMaps = new ImportMaps();




	@Override
	public void importData(TTImportConfig config) throws Exception {

		System.out.println("importing vision codes");
		System.out.println("retrieving snomed codes from IM");
        try (TTManager manager= new TTManager()) {
            snomedCodes = importMaps.importSnomedCodes();
            document = manager.createDocument(IM.GRAPH_VISION.getIri());
            document.addEntity(manager.createGraph(IM.GRAPH_VISION.getIri(), "Vision (including Read) codes",
                "The Vision local code scheme and graph including Read 2 and Vision local codes"));

            importEmis();
            importR2Desc(config.getFolder());
            importR2Terms(config.getFolder());
            importVisionCodes(config.getFolder());
            addMoreReadCodes();
            createHierarchy();
            addVisionMaps(config.getFolder());
            addMissingMaps();
            try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler()) {
                filer.fileDocument(document);
            }

        }
	}

	private void addMoreReadCodes() throws TTFilerException {
		for (Map.Entry<String,TTEntity> entry:emisRead.entrySet()){
			String code= entry.getKey();
			if (codeToConcept.get(code)==null)
				document.addEntity(entry.getValue());
			else {
				TTEntity vision= codeToConcept.get(code);
				TTEntity read= entry.getValue();
				if (read.get(IM.MATCHED_TO)!=null){
					if (vision.get(IM.MATCHED_TO)==null){
						vision.set(IM.MATCHED_TO,read.get(IM.MATCHED_TO));
					} else {
						for (TTValue snoExtra:read.get(IM.MATCHED_TO).iterator()) {
							if (!vision.get(IM.MATCHED_TO).contains(snoExtra)) {
								vision.get(IM.MATCHED_TO).add(snoExtra);
							}
						}
					}
				}
			}
		}
	}

	private void addMissingMaps() {
		for (Map.Entry<String,TTEntity> entry:codeToConcept.entrySet()){
			String code= entry.getKey();
			TTEntity vision= entry.getValue();
			if (vision.get(IM.MATCHED_TO)==null){
				if (emisRead.get(code)!=null){
					vision.addObject(IM.MATCHED_TO,emisRead.get(code).get(IM.MATCHED_TO).asIriRef());
				}
			}

		}
	}


	private void importR2Terms(String folder) throws IOException {

		Path file =  ImportUtils.findFileForId(folder, r2Terms[0]);
		System.out.println("Importing official R2 terms as vision");

		try( CSVReader reader = new CSVReader(new FileReader(file.toFile()))){
			reader.readNext();
			int count=0;
			String[] fields;
			while ((fields = reader.readNext()) != null) {
				count++;
				if("C".equals(fields[1])) {
					String termid= fields[0];
					String term= fields[3];
					TTEntity readConcept= r2TermIdMap.get(termid);
					if (readConcept!=null){
						if (preferredId.contains(termid))
							readConcept.setName(term);
						else {
							TTManager.addTermCode(readConcept,term,null);
						}
					}
				}
			}
			System.out.println("Process ended with " + count +" read 2 terms");
		}
	}

	private void importEmis() throws IOException {
		System.out.println("Importing EMIS/Read from IM for look up....");
		emisRead= importMaps.getEMISReadAsVision();

		}


	private void importR2Desc(String folder) throws IOException {

		Path file =  ImportUtils.findFileForId(folder, r2Desc[0]);
		System.out.println("Importing R2 entities");

		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine(); // NOSONAR - Skip header
			String line = reader.readLine();

			int count = 0;
			while (line != null && !line.isEmpty()) {

				String[] fields = line.split(",");
				if ("C".equals(fields[6])) {
					String code= fields[0];
					if (!code.startsWith(".")){
						if (!Character.isLowerCase(code.charAt(0))) {
						TTEntity readConcept = codeToConcept.get(code);
						if (readConcept == null) {
							readConcept = new TTEntity()
								.setIri(IM.CODE_SCHEME_VISION.getIri() + code.replace(".",""))
								.setCode(code)
								.setStatus(IM.ACTIVE)
								.setScheme(IM.CODE_SCHEME_VISION)
								.addType(IM.CONCEPT);
							document.addEntity(readConcept);
							codeToConcept.put(code, readConcept);
						}
						String termId = fields[1];
						String preferred = fields[2];
						if (preferred.equals("P"))
							preferredId.add(termId);
						r2TermIdMap.put(termId, readConcept);
						}
						count++;
						if (count % 50000 == 0) {
							System.out.println("Processed " + count + " read code termid links");
						}
					}
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count + " read code term id links");
		}
	}


	public Boolean isEMIS(String s){
		if (s.length()>5)
			return true;
		else if (s.contains("DRG") || s.contains("SHAPT") || s.contains("EMIS"))
			return true;
		else
			return false;
	}



	private void createHierarchy() {
		Logger.info("Creating child parent hierarchy");
		TTEntity vision= new TTEntity()
			.setIri(IM.GRAPH_VISION.getIri()+"VisionCodes")
			.setName("Vision read 2 and localcodes")
			.addType(IM.CONCEPT)
			.setCode("VisionCodes")
			.setScheme(IM.GRAPH_VISION)
			.setDescription("Vision and read 2 codes mapped to core");
		vision.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"CodeBasedTaxonomies"));
		for (TTEntity entity:document.getEntities()){
			String shortCode = entity.getCode();
			if (shortCode!=null) {
				if (shortCode.contains("."))
					shortCode = shortCode.substring(0, shortCode.indexOf("."));
				if (shortCode.length() == 1)
					entity.set(IM.IS_CHILD_OF, new TTArray().add(iri(vision.getIri())));
				else {
					String parent = shortCode.substring(0, shortCode.length() - 1);
					entity.set(IM.IS_CHILD_OF, new TTArray().add(iri(IM.CODE_SCHEME_VISION.getIri() + parent)));
				}
			}
		}
	}


	private void importVisionCodes(String folder) throws IOException {
		Path file =  ImportUtils.findFileForId(folder, visionRead2Code[0]);
		System.out.println("Retrieving terms from vision read+lookup2");
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine(); // NOSONAR - Skip header
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = readQuotedCSVLine(reader, line);
				count++;
				if (count % 50000 == 0) {
					LOG.info("{} codes imported ",count);
				}
				String code = fields[0];
				String term = fields[1];
				code = code.replace("\"", "");
				term = term.substring(1, term.length() - 1);
				if (!code.startsWith(".") && !Character.isLowerCase(code.charAt(0)) && codeToConcept.get(code) == null) {
                    TTEntity c = new TTEntity();
                    c.setIri(IM.CODE_SCHEME_VISION.getIri() + code.replace(".", ""));
                    c.setName(term);
                    c.setCode(code);
										c.setScheme(IM.CODE_SCHEME_VISION);
                    document.addEntity(c);
                    codeToConcept.put(code, c);
                }
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count + " additional Vision read like codes created");
		}
	}

	public String[] readQuotedCSVLine(BufferedReader reader, String line) throws IOException {
		if (line.split(",").length < 5) {
			do {
				String nextLine = reader.readLine();
				line = line.concat("\n").concat(nextLine);
			} while (line.split(",").length < 5);
		}
		String[] fields = line.split(",");
		if (fields.length > 5) {
			for (int i = 2; i < fields.length - 3; i++) {
				fields[1] = fields[1].concat(",").concat(fields[i]);
			}
		}
		return fields;
	}


	private void addVisionMaps(String folder) throws IOException {
		Path file =  ImportUtils.findFileForId(folder, visionRead2toSnomed[0]);
		System.out.println("Retrieving Vision snomed maps");
		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine(); // NOSONAR - Skip header
			String line = reader.readLine();
			int count = 0;
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split(",");
				count++;
				if (count % 50000 == 0) {
					LOG.info("{} maps added ",count);
				}
				String code= fields[0];
				String snomed= fields[1];
				TTEntity vision = codeToConcept.get(code);
				if (vision!=null) {
					if (isSnomed(snomed)) {
						String iri = SNOMED.NAMESPACE + snomed;
						vision.addObject(IM.MATCHED_TO, iri(SNOMED.NAMESPACE+snomed));
					}
				}
				line = reader.readLine();
			}
			System.out.println("Process ended with " + count);
		}
	}


	public Boolean isSnomed(String s){
		return snomedCodes.contains(s);
	}

	@Override
	public void validateFiles(String inFolder) {
		 ImportUtils.validateFiles(inFolder,r2Terms,r2Desc,visionRead2Code,visionRead2toSnomed);
	}

    @Override
    public void close() throws Exception {
        if (snomedCodes != null) snomedCodes.clear();
        if (emisRead != null) emisRead.clear();
        codeToConcept.clear();
        r2TermIdMap.clear();
        preferredId.clear();

        importMaps.close();

    }
}
