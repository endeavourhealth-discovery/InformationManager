package org.endeavourhealth.informationmanager.utils.sets;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.endeavourhealth.imapi.dataaccess.helpers.ConnectionManager;
import org.endeavourhealth.imapi.filer.TTImport;
import org.endeavourhealth.imapi.filer.TTImportConfig;
import org.endeavourhealth.imapi.model.tripletree.TTArray;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

public class SetsTypeFixer implements TTImport {
    private static final String[] sets = {".*\\\\DiscoveryCore\\\\Sets.json"};

    private final TTManager manager= new TTManager();
    private TTDocument newDocument = new TTDocument();


    @Override
    public TTImport importData(TTImportConfig config) throws Exception {
        List<String> setsIris = findSets();
        TTDocument document = loadFile(config.getFolder());
        newDocument.setContext(document.getContext());
        newDocument.setGraph(document.getGraph());
        newDocument = fixSetsTypes(setsIris, document);
        manager.setDocument(newDocument);
        manager.saveDocument(new File(config.getFolder() + sets[0].substring(2)));
        return this;
    }

    private List<String> findSets() {
        List<String> result = new ArrayList<>();

        String sql = new StringJoiner(System.lineSeparator())
                .add("PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>")
                .add("PREFIX im: <http://endhealth.info/im#>")
                .add("PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>")
                .add("SELECT ?set")
                .add("WHERE {")
                .add("  ?s rdf:type ?st .")
                .add("  FILTER(?st IN(im:ConceptSet,im:ValueSet))")
                .add("  ?s im:isContainedIn ?set .")
                .add("  FILTER NOT EXISTS {?s (im:isSubsetOf) ?set .}")
                .add("  ?set rdf:type ?sett .")
                .add("  FILTER(?sett IN(im:ConceptSet,im:ValueSet))")
                .add("  FILTER NOT EXISTS {?set (im:hasMember|im:definition) ?m .}")
                .add("  GRAPH ?g { ?set rdfs:label ?setname .}")
                .add("    VALUES ?g { im: } .")
                .add("}")
                .add("GROUP BY ?set").toString();
        try (RepositoryConnection conn = ConnectionManager.getIMConnection()) {
            TupleQuery tupleQuery = conn.prepareTupleQuery(sql);
            try (TupleQueryResult qr = tupleQuery.evaluate()) {
                while (qr.hasNext()) {
                    BindingSet rs = qr.next();
                    result.add(rs.getValue("set").stringValue());
                }
            }
        }
        return result;
    }

    private TTDocument loadFile(String inFolder) throws IOException {
        Path file = ImportUtils.findFileForId(inFolder, sets[0]);
        return manager.loadDocument(file.toFile());
    }

    public TTDocument fixSetsTypes(List<String> sets, TTDocument document) {
        for(TTEntity entity: document.getEntities()){
            if(sets.contains(entity.getIri())){
                entity.setType(new TTArray().add(IM.FOLDER));
            }
            newDocument.addEntity(entity);
        }
        return newDocument;
    }

    @Override
    public TTImport validateFiles(String inFolder) {
        ImportUtils.validateFiles(inFolder,sets);
        return this;
    }
}
