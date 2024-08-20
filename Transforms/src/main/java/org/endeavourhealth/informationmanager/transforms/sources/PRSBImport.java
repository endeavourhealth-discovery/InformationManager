package org.endeavourhealth.informationmanager.transforms.sources;

import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;
import org.endeavourhealth.imapi.vocabulary.GRAPH;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class PRSBImport implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(PRSBImport.class);

  private static final String[] prsbEntities = {".*\\\\PRSB\\\\RetrieveInstance.json"};
  private TTDocument document;
  private Map<String, TTArray> axiomMap;

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    validateFiles(config.getFolder());
    try (TTManager dmanager = new TTManager()) {
      document = dmanager.createDocument(GRAPH.PRSB);
      document.addEntity(dmanager.createGraph(GRAPH.PRSB, "PRSB code scheme and graph"
        , "The professional records standards board code scheme and graph"));
      importEntityFiles(config.getFolder());
      //TTDocumentFiler filer = new TTDocumentFilerJDBC(document.getGraph());
      dmanager.saveDocument(new File(config.getFolder() + "prsb.json"));
      //filer.fileDocument(document);
    } catch (Exception e) {
      throw new ImportException(e.getMessage(), e);
    }
  }

  private void initializeMaps() {
    axiomMap = new HashMap<>();
    axiomMap.put("prsb03-dataelement-10868", getAxioms(IM.NAMESPACE + "Patient"));

  }

  private TTArray getAxioms(String prsb) {
    return null;
  }

  private void importEntityFiles(String path) throws IOException {
    int i = 0;
    for (String prsbFile : prsbEntities) {
      Path file = ImportUtils.findFilesForId(path, prsbFile).get(0);
      LOG.info("Processing entities in {}", file.getFileName().toString());
      JSONParser jsonParser = new JSONParser();
      try (FileReader reader = new FileReader(file.toFile())) {
        //Read JSON file
        Object obj = jsonParser.parse(reader);

        JSONArray prsbModel = (JSONArray) ((JSONObject) obj).get("dataset");

        //Iterate over prsbModel
        prsbModel.forEach(mod -> {
          try {
            parsePRSBModel((JSONObject) mod);
          } catch (DataFormatException e) {
            e.printStackTrace();
          }
        });

      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      } catch (ParseException e) {
        e.printStackTrace();
      }
    }
    LOG.info("Imported {} entities", i);
  }

  private void parsePRSBModel(JSONObject dataModel) throws DataFormatException {
    TTEntity dm = newEntity(dataModel, iri(SHACL.NODESHAPE));
    dm.addObject(iri(IM.IS_CONTAINED_IN), TTIriRef.iri(IM.NAMESPACE + "DiscoveryOntology"));
    JSONArray recordTypes = (JSONArray) dataModel.entrySet();
    dataModel.entrySet().forEach(c -> {
      try {
        parseRecordType((JSONObject) c);
      } catch (DataFormatException e) {
        e.printStackTrace();
      }
    });
  }

  private void parseRecordType(JSONObject c) throws DataFormatException {
    String prsbId = (String) c.get("iddisplay");
    TTEntity rt = newEntity(c, iri(SHACL.NODESHAPE));
    TTArray axioms = axiomMap.get(prsbId);

  }

  private TTIriRef mapStatus(String status) throws DataFormatException {
    if (status.equals("draft"))
      return iri(IM.DRAFT);
    else
      throw new DataFormatException("unknown status type - " + status);

  }

  private TTEntity newEntity(JSONObject c, TTIriRef... types) throws DataFormatException {
    TTEntity entity = new TTEntity();
    entity.set(iri(IM.HAS_STATUS), mapStatus(c.get("statusCode").toString()));
    Arrays.stream(types).forEach(type -> entity.addType(type));
    String prsbId = c.get("iddisplay").toString();
    entity.setCode(prsbId);
    String name = getObjectArrayliteral(c, "name", "#text");
    String iri = (PRSB.NAMESPACE + prsbId);

    entity.setName(name);
    if (c.get("shortName") != null) {
      String shortName = (String) c.get("shortName");
      entity.set(TTIriRef.iri(IM.NAMESPACE + "shortName"), TTLiteral.literal((shortName)));
    }
    entity.setIri(iri);
    String description = getObjectArrayliteral(c, "desc", "#text");
    if (description != null)
      entity.setDescription(description);
    String background = getObjectArrayliteral(c, "context", "#text");
    if (background != null)
      entity.set(TTIriRef.iri(IM.NAMESPACE + "backgroundContext"), TTLiteral.literal(background));
    if (entity.isType(iri(SHACL.NODESHAPE)))
      return entity;
    return entity;

  }

  private String getObjectArrayliteral(JSONObject ob, String name, String subname) {
    JSONArray arr = (JSONArray) ob.get(name);
    JSONObject subob = (JSONObject) arr.get(0);
    if (subob.get(subname) != null)
      return (String) subob.get(subname);
    else
      return null;
  }

  @Override
  public void validateFiles(String inFolder) {
    ImportUtils.validateFiles(inFolder, prsbEntities);
  }


  @Override
  public void close() throws Exception {
    axiomMap.clear();
  }
}
