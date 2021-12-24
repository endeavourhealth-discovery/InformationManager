package org.endeavourhealth.informationmanager.transforms;

import org.apache.commons.compress.utils.FileNameUtils;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.endeavourhealth.informationmanager.TTImport;
import org.endeavourhealth.informationmanager.TTImportConfig;
import org.endeavourhealth.informationmanager.transforms.eqd.EnquiryDocument;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.zip.DataFormatException;

public class CEGEqdImporter implements TTImport {
	private TTDocument document;
	private TTEntity owner;

	private static final String[] queries = {".*\\\\CEG\\\\UCLP-CEG SMI EMIS v5.xml"};
	@Override
	public TTImport importData(TTImportConfig config) throws Exception {
		TTManager manager= new TTManager();
		document= manager.createDocument(IM.GRAPH_CEG_QUERY.getIri());
		createOrg();
		loadAndConvert(config.folder);
		return this;
	}

	private void createOrg() {
		owner= new TTEntity()
			.setIri("http://org.endhealth.info/im#QMUL_CEG")
			.addType(TTIriRef.iri(IM.NAMESPACE+"Organisation"))
			.setName("Clinical Effectiveness Group of Queen Mary Universitly of London - CEG");
		document.addEntity(owner);
	}

	public void loadAndConvert(String folder) throws JAXBException, IOException, DataFormatException {
		for (File fileEntry : Objects.requireNonNull(new File(folder).listFiles())) {
			if (!fileEntry.isDirectory()) {
				String ext = FileNameUtils.getExtension(fileEntry.getName());
				if (ext.equalsIgnoreCase("xml")) {
					JAXBContext context = JAXBContext.newInstance(EnquiryDocument.class);
					EnquiryDocument eqd = (EnquiryDocument) context.createUnmarshaller()
						.unmarshal(new FileReader(fileEntry));
					EqdToTT converter= new EqdToTT();
					List<TTEntity> entities= converter.convertDoc(eqd,
						IM.DOMAIN+"ceg",
						TTIriRef.iri(owner.getIri()));
					for (TTEntity entity:entities)
						document.addEntity(entity);
				}
			}
		}

	}


	@Override
	public TTImport validateFiles(String inFolder) throws TTFilerException {
		ImportUtils.validateFiles(inFolder,queries);
		return this;
	}


}
