package org.endeavourhealth.informationmanager.transforms.sources;


import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTLiteral;
import org.endeavourhealth.imapi.transforms.SnomedConcept;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.OWL;
import org.endeavourhealth.imapi.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public class ImportUnmapped {
	private static final Logger LOG = LoggerFactory.getLogger(ImportUnmapped.class);
	private static final String[] unMapped = {".*\\\\IMv1\\\\UnmappedConcepts.txt"};

	private TTDocument im1;
	private TTDocument encounters;
	private TTDocument im2;
	private int incremental=101196;
	private TTEntity nhsDDProperties;
	private TTEntity nhsDDValues;
	private TTDocument barts;
	private TTDocument tpp;
	private TTDocument vision;
	String tpplocal= "http://endhealth.info/tpp#XaB9E";
	private String visionNamespace= "1000027";
	public void importUnmapped(String inFolder) throws Exception {
		TTManager manager = new TTManager();
		im1 = manager.createDocument(IM.GRAPH_IM1.getIri());
		encounters = manager.createDocument(IM.CODE_SCHEME_ENCOUNTERS.getIri());
		im2 = manager.createDocument(IM.NAMESPACE);
		barts= manager.createDocument(IM.CODE_SCHEME_BARTS_CERNER.getIri());
		tpp= manager.createDocument(IM.CODE_SCHEME_TPP.getIri());
		vision= manager.createDocument(IM.CODE_SCHEME_VISION.getIri());
		importData(inFolder);
		try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(encounters);
		}

		try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(im2);
		}

		try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(barts);
		}
		try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(tpp);
		}
		try (TTDocumentFiler filer= TTFilerFactory.getDocumentFiler()) {
			filer.fileDocument(vision);
		}

	}
	private void importData(String inFolder) throws IOException {
		Path file = ImportUtils.findFileForId(inFolder, unMapped[0]);
		TTEntity unmapped= newEntity(IM.NAMESPACE+"ImportUnmapped",null,IM.FOLDER,"Unmapped concepts","concepts or codes waiting mapping",null,null);
		unmapped.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"im:CodeBasedTaxonomies"));
    im2.addEntity(unmapped);
		TTEntity unmappedEncounters= newEntity(IM.NAMESPACE+"UnmappedEncounters",null,IM.FOLDER,"Unmapped legacy encounter types","Local text or code representations of encounter types",null,null);
		unmappedEncounters.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"ImportUnmapped"));
		encounters.addEntity(unmappedEncounters);
		List<String> skip= List.of("CM_Org_BHRUT","CM_Sys_Medway","CM_NHS_DD");
		nhsDDValues= newEntity(IM.NAMESPACE+"NHSDDUnmappedValue","CM_NHS_DD",IM.CONCEPT,"NHS Data Dictionary unmapped field values",null,null,null);
		nhsDDValues.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"ImportUnmapped"));
		im2.addEntity(nhsDDValues);
		nhsDDProperties= newEntity(IM.NAMESPACE+"NHSDDUnmappedProperty","CM_NHS_DD_P", RDF.PROPERTY,"NHS Data Dictionary unmapped fields",null,null,null);
		nhsDDProperties.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"ImportUnmapped"));
		im2.addEntity(nhsDDProperties);
		TTEntity orphanBarts= newEntity(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"UncategorisedBartsCodes",null,IM.FOLDER,"Uncategorised Barts Cerner codes",null,null,null);
		orphanBarts.addObject(IM.IS_CHILD_OF,TTIriRef.iri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"BartsCernerCodes"));
		barts.addEntity(orphanBarts);
		TTEntity orphanVision= newEntity(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"UncategorisedVisionCodes",null,IM.FOLDER,"Uncategorised Vision codes",null,null,null);
		orphanVision.addObject(IM.IS_CHILD_OF,TTIriRef.iri(IM.CODE_SCHEME_VISION.getIri()+"VisionCodes"));
		vision.addEntity(orphanVision);


		try (BufferedReader reader = new BufferedReader(new FileReader(file.toFile()))) {
			reader.readLine();
			String line = reader.readLine();
			while (line != null && !line.isEmpty()) {
				String[] fields = line.split("\t");
				String oldIri = fields[0];
				String term = fields[1];
				String im1Scheme = fields[2];
				String code = fields[3];
				String description = fields[4];
				String usage= fields[5];
				if (im1Scheme.equals("LE_TYPE")) {
					TTEntity legacyEncounter= newEntity(IM.CODE_SCHEME_ENCOUNTERS.getIri() + oldIri, oldIri,IM.CONCEPT,term, description, code, IM.CODE_SCHEME_ENCOUNTERS);
					legacyEncounter.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"UnmappedEncounters"));
					legacyEncounter.set(IM.PRIVACY_LEVEL,	TTLiteral.literal(1));
					if (!usage.equals("0"))
						legacyEncounter.set(IM.USAGE_TOTAL,TTLiteral.literal(usage));
					encounters.addEntity(legacyEncounter);
				}
				else if (im1Scheme.equals("SNOMED")) {
					if (SnomedConcept.getNameSpace(code).equals(visionNamespace)) {
						TTEntity visionConcept= newEntity(IM.CODE_SCHEME_VISION.getIri()+code,oldIri,IM.CONCEPT,term,description,code,IM.CODE_SCHEME_VISION);
						visionConcept.addObject(IM.IS_CHILD_OF,TTIriRef.iri(IM.CODE_SCHEME_VISION.getIri()+"UncategorisedVisionCodes"));
						vision.addEntity(visionConcept);

					}
				}
				else if (im1Scheme.equals("VISION_LOCAL")){
					TTEntity visionConcept= newEntity(IM.CODE_SCHEME_VISION.getIri()+code,oldIri,IM.CONCEPT,term,description,code,IM.CODE_SCHEME_VISION);
					visionConcept.addObject(IM.IS_CHILD_OF,TTIriRef.iri(IM.CODE_SCHEME_VISION.getIri()+"UncategorisedVisionCodes"));
					vision.addEntity(visionConcept);
				}
				else if (im1Scheme.equals("CM_DiscoveryCode")){
					if (oldIri.startsWith("DM_")){
						String iri= oldIri.split("_")[1];
						TTEntity property= newEntity(IM.NAMESPACE+iri,oldIri, OWL.OBJECTPROPERTY,term,description,null,null);
						property.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"NHSDDUnmappedProperty"));
						im2.addEntity(property);
					}
					else if (oldIri.startsWith("CM_"))
						if (!skip.contains(oldIri)){
							String concept= SnomedConcept.createConcept(incremental,true);
							incremental++;
							TTEntity value= newEntity(IM.NAMESPACE+concept,oldIri,IM.CONCEPT,term,description,concept,IM.CODE_SCHEME_DISCOVERY);
							value.addObject(IM.IS_CONTAINED_IN,TTIriRef.iri(IM.NAMESPACE+"NHSDDUnmappedValue"));
							im2.addEntity(value);
						}
				}
				else if (im1Scheme.equals("BartsCerner")){
					if (StringUtils.isNumeric(code)) {
						TTEntity bartsConcept= newEntity(IM.CODE_SCHEME_BARTS_CERNER.getIri()+code,oldIri,IM.CONCEPT,term,description,code,IM.CODE_SCHEME_BARTS_CERNER);
						bartsConcept.addObject(IM.IS_CHILD_OF,TTIriRef.iri(IM.CODE_SCHEME_BARTS_CERNER.getIri()+"UncategorisedBartsCodes"));
						barts.addEntity(bartsConcept);
					}
				}
				else if (im1Scheme.equals("TPP_LOCAL")){
					TTEntity tppConcept= newEntity(IM.CODE_SCHEME_TPP.getIri()+code,oldIri,IM.CONCEPT,term,description,code,IM.CODE_SCHEME_TPP);
					tppConcept.addObject(IM.IS_CHILD_OF,TTIriRef.iri(tpplocal));
					tpp.addEntity(tppConcept);
				}


				line = reader.readLine();
			}

		}
	}

	private TTEntity newEntity(String iri, String oldIri,TTIriRef type,String term, String description, String code, TTIriRef scheme) {
		TTEntity entity= new TTEntity()
			.setIri(iri)
			.setName(term)
			.addType(type)
			.setStatus(IM.DRAFT);
		if (oldIri!=null)
		    entity.set(IM.IM1ID, TTLiteral.literal(oldIri));
		if (description!=null)
			if (!description.equals(term))
				entity.setDescription(description);
		if (code!=null){
			entity.setCode(code);
			entity.setScheme(scheme);
		}
		return entity;


	}

}
