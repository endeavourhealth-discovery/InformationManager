package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.xml.bind.JAXBContext;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.EqdToIMQ;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.transforms.eqd.EnquiryDocument;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.online.ImportApp;

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
	private Namespace namespace;

	public void importEqd(String folder,String reportId,Namespace namespace) throws Exception {;
		Path startDir = Paths.get(folder);
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
				paths
					.filter(Files::isRegularFile)
					.filter(path -> path.toString().toLowerCase().endsWith(".xml"))
					.forEach(path -> this.convertEqd(path,dataMap,document));
			}

			try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
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
		try {
			JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
			EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
				.unmarshal(fileEntry);
			converter.convertEQD(document, eqd, dataMap, namespace);
			if (!document.getEntities().isEmpty()){
				document.getEntities().forEach(e->System.out.println(e.getName()));
				System.out.println("Found "+document.getEntities().get(0).getName()+fileEntry.getName());
			}
		} catch (Exception ignored) {

		}
	}

}
