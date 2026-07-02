package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.NAMESPACE;

import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

public class SingleEqdQueryImport {
	private Properties dataMap;
	private Properties uuidLabels;
	private final EqdToIMQ converter = new EqdToIMQ(false);
	private NAMESPACE namespace;

	public void importEqd(String folder,String subFolder,String reportId,NAMESPACE namespace) throws Exception {;
		Path startDir = Paths.get(subFolder);
		if (!Files.isDirectory(startDir)) {
			System.err.println("The path provided is not a directory.");
			System.exit(2);
		}
		this.namespace=namespace;
		converter.setSingleEntity(reportId);
		dataMap= new Properties();
		dataMap.load(new FileReader(folder+"/EQD/EqdDataMap.properties"));
		uuidLabels= new Properties();
		uuidLabels.load(new FileReader(folder+"/EQD/UUIDLabels.properties"));
		try (TTManager manager = new TTManager()) {
			TTDocument document = manager.createDocument();
			try (Stream<Path> paths = Files.walk(startDir)) {
				paths.forEach(path -> {
					try {
						if (Files.isRegularFile(path) &&
							path.toString().toLowerCase().endsWith(".xml")) {
							this.convertEqd(path, dataMap, document);
						}
					} catch (Exception e) {
						System.err.println("Failed on: " + path);
						e.printStackTrace();
					}
				});
			}


			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(GRAPH.IM)) {
				filer.fileDocument(document);
			}
		}
	}

	private void setAlternativeCodes(TTDocument document) throws JsonProcessingException {
		for (TTEntity entity:document.getEntities()) {
			if (uuidLabels.get(entity.getIri())!=null){
				entity.set(IM.ALTERNATIVE_CODE, TTLiteral.literal(uuidLabels.get(entity.getIri())));
			}
		}
	}


	private void convertEqd(Path path, Properties dataMap,TTDocument document){
		File fileEntry = path.toFile();
		if (fileEntry.getName().equals("GP Contract Apr 2026 - V50 Release 1.0 [SNOMED CT].xml"))
			System.out.println(fileEntry.getAbsoluteFile().getName());
		try {
			JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
			EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
				.unmarshal(fileEntry);
			converter.convertEQD(document, eqd, dataMap, namespace);
			if (document.getEntities()!=null){
				document.getEntities().forEach(e->System.out.println(e.getName()));
				System.out.println("Found "+document.getEntities().get(0).getName()+fileEntry.getName());
			}
		} catch (Exception ignored) {

		}


	}



}
