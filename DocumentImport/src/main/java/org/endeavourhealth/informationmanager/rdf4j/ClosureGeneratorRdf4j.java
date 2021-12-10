package org.endeavourhealth.informationmanager.rdf4j;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.http.HTTPRepository;
import org.endeavourhealth.imapi.model.tripletree.TTContext;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTPrefix;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;
import org.endeavourhealth.informationmanager.TCGenerator;
import org.endeavourhealth.informationmanager.TTFilerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class ClosureGeneratorRdf4j implements TCGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(TTDocumentFilerRdf4j.class);
	private Repository repo;
	private RepositoryConnection conn;
	private static HashMap<String, Set<String>> parentMap;
	private static HashMap<String, Set<String>> closureMap;
	private int counter;


	public ClosureGeneratorRdf4j() throws TTFilerException {
		LOG.info("Connecting");
		//repo = new SailRepository(new NativeStore(new File("Z:\\rdf4j")));
		repo = new HTTPRepository("http://localhost:7200/", "im");

		try {
			repo = new HTTPRepository("http://localhost:7200/", "im");
			repo.initialize();
			conn = repo.getConnection();
			LOG.info("Connected");
		} catch (RepositoryException e) {
			LOG.info("Failed");
			throw new TTFilerException("Failed to open repository connection", e);
		}

	}

	@Override
	public void generateClosure(String outpath, boolean secure) throws TTFilerException, IOException {
		List<TTIriRef> relationships = Arrays.asList(
			RDFS.SUBCLASSOF,
			RDFS.SUBPROPERTYOF,
			//IM.HAS_REPLACED,
			SNOMED.REPLACED_BY);

		parentMap = new HashMap<>(1000000);

		for (TTIriRef rel : relationships) {

					loadRelationships(conn, rel);
			}

		String outFile = outpath + "/closure.ttl";
		try(FileWriter fw = new FileWriter(outFile)) {
		buildClosure();
		writeClosureData(fw);
		importClosure(outpath, secure);
	}

	}


	private void loadRelationships(RepositoryConnection conn, TTIriRef relationship) {
		System.out.println("Extracting " + relationship.getIri());
		String sql;
		TupleQuery stmt;
		if (relationship.equals(IM.HAS_REPLACED)) {
			stmt = conn.prepareTupleQuery(getDefaultPrefixes() + "\nSelect ?child ?parent\n" +
				"where {?child ^<" + SNOMED.REPLACED_BY.getIri() + "> ?parent }\n");
		} else {
			stmt = conn.prepareTupleQuery(getDefaultPrefixes() + "\nSelect ?child ?parent\n" +
				"where {?child <" + relationship.getIri() + "> ?parent }\n");
		}
		try (TupleQueryResult rs = stmt.evaluate()) {
			int c = 0;
			while (rs.hasNext()) {
				BindingSet bs = rs.next();
				String child = bs.getValue("child").stringValue();
				String parent = bs.getValue("parent").stringValue();
				Set<String> parents = parentMap.computeIfAbsent(child, k -> new HashSet<>());
				parents.add(parent);
			}
		}
		System.out.println("Relationships loaded for " + relationship.getIri() + " " + parentMap.size() + " entities");
	}

	private void buildClosure() {
		closureMap = new HashMap<>(10000000);
		System.out.println("Generating closure map");
		int c = 0;
		counter=0;
		for (Map.Entry<String, Set<String>> row : parentMap.entrySet()) {
			c++;
			String child = row.getKey();
			if (closureMap.get(child)==null) {
				if (c % 100000 == 0)
					System.out.println("Processed " + c + " entities");
				generateClosure(child);
			}
		}
		System.out.println("Closure built with  "+counter+" triples with  "+closureMap.size()+" keys");
	}

	private String getDefaultPrefixes() {
		TTManager manager = new TTManager();
		StringJoiner prefixes = new StringJoiner("\n");
		TTContext context = manager.createDefaultContext();
		for (TTPrefix pref : context.getPrefixes()) {
			prefixes.add("PREFIX " + pref.getPrefix() + ": <" + pref.getIri() + ">");
		}
		return prefixes.toString();
	}

	private Set<String> generateClosure(String child) {
		Set<String> closures= closureMap.get(child);
		if (closures==null) {
			closures = new HashSet<>();
			closureMap.put(child, closures);
		}


		// Add self
		closures.add(child);
		counter++;

		Set<String> parents = parentMap.get(child);
		if (parents != null) {
			for (String parent : parents) {
				// Check do we have its closure?
				Set<String> parentClosures = closureMap.get(parent);
				if (parentClosures == null) {
					parentClosures = generateClosure(parent);
				}
				// Add parents closure to this closure
				for (String parentClosure : parentClosures) {
					if (!closures.contains(parentClosure)){
						closures.add(parentClosure);
						counter++;
					}
				}
			}
		}
		return closures;
	}

	private void writeClosureData(FileWriter fw) throws IOException {
		int c = 0;
		counter=0;
		System.out.println("Writing closure data");
		for (Map.Entry<String, Set<String>> entry : closureMap.entrySet()) {
			for (String closure : entry.getValue()) {
				counter++;
				fw.write("<" + entry.getKey() + "> <" + IM.IS_A.getIri() + "> <" + closure + ">.\n");
			}
		}
		fw.close();
		System.out.println(counter + " Closure triples written");
	}

	private void importClosure(String outpath, boolean secure) throws IOException, TTFilerException {
		System.out.println("Importing closure ...");

		StringJoiner sql = new StringJoiner("\n");
			sql.add("INSERT DATA {");
			int lineCount = 0;
			BufferedReader reader = new BufferedReader(new FileReader(outpath + "/closure.ttl"));
			String triple = reader.readLine();
			while (triple != null) {
				if (!triple.isEmpty()) {
					lineCount++;
					sql.add(triple);
					if (lineCount % 200000 == 0) {
						System.out.println("Importing " + lineCount + " of " + counter + " triples :" + triple);
						sql.add("}");
						Update upd = conn.prepareUpdate(sql.toString());
						conn.begin();
						upd.execute();
						conn.commit();
						sql = new StringJoiner("\n");
						sql.add("INSERT DATA {");
					}
				}
				triple = reader.readLine();
			}
		  System.out.println("Importing " + lineCount + " of " + counter + " triples :");
			if (sql.length()>20) {
				sql.add("}");
				Update upd = conn.prepareUpdate(sql.toString());
				conn.begin();
				upd.execute();
				conn.commit();
			}
			conn.close();
		}


}
