package org.endeavourhealth.informationmanager.transforms.sources;

import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.impl.ValidatingValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.endeavourhealth.imapi.dataaccess.EntityRepository2;
import org.endeavourhealth.imapi.dataaccess.FileRepository;
import org.endeavourhealth.imapi.dataaccess.helpers.ConnectionManager;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SNOMED;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ImportMaps {
	private FileRepository fileRepo= new FileRepository(TTBulkFiler.getDataPath());
	private ValueFactory valueFactory= new ValidatingValueFactory(SimpleValueFactory.getInstance());


	/**
	 * Retrieves EMIS to Snomed code maps
	 * @throws TTFilerException when code maps are missing
	 */
	public Map<String, Set<String>> importEmisToSnomed() throws TTFilerException, IOException {
		if (TTFilerFactory.isBulk())
			return fileRepo.getCodeCoreMap(IM.CODE_SCHEME_EMIS.getIri());
		return importEmisToSnomedRdf4j();
	}

	/**
	 * Retrieves snomed codes from im
	 * @return a set of snomed codes
	 * @throws TTFilerException if using rdf4j
	 */
	public  Set<String> importSnomedCodes() throws TTFilerException, IOException {

		if (TTFilerFactory.isBulk()) {
			return fileRepo.getCodes(SNOMED.NAMESPACE);
		}
		else {
			Set<String> snomedCodes = new HashSet<>();
			return importSnomedRDF4J(snomedCodes);
		}
	}

	/**
	 * Returns A core entity iri and name from a core term
	 * @param term the code or description id or term code
	 * @return iri and name of entity
	 */
	public TTIriRef getReferenceFromCoreTerm(String term) throws IOException {
		if (TTFilerFactory.isBulk())
			return fileRepo.getReferenceFromCoreTerm(term);
		else
			return new EntityRepository2().getReferenceFromCoreTerm(term);
	}

	public Set<TTIriRef> getCoreFromCode(String code, List<String> schemes) {
		if (TTFilerFactory.isBulk())
			return fileRepo.getCoreFromCode(code,schemes);
		else
			return new EntityRepository2().getCoreFromCode(code,schemes);
	}

	public Map<String, Set<String>> getAllMatchedLegacy() throws IOException {
		if (TTFilerFactory.isBulk())
			return fileRepo.getAllMatchedLegacy();
		else
			return new EntityRepository2().getAllMatchedLegacy();
	}

	public Set<TTIriRef> getCoreFromLegacyTerm(String term, String scheme) throws IOException {
		if (TTFilerFactory.isBulk()) {
			return fileRepo.getCoreFromLegacyTerm(term, scheme);
		} else {
			return new EntityRepository2().getCoreFromLegacyTerm(term, scheme);
		}
	}

	/**
	 * Gets the matched core concept from an emis code ide
	 * @param codeId the emis code id
	 * @param scheme the scheme
	 * @return the set of core entities
	 * @throws IOException
	 */
	public Set<TTIriRef> getCoreFromCodeId(String codeId, String scheme) throws IOException {
		if (TTFilerFactory.isBulk()) {
			return fileRepo.getCoreFromCodeId(codeId, List.of(scheme));
		} else {
			return new EntityRepository2().getCoreFromCodeId(codeId, List.of(scheme));
		}
	}





	/**
	 * Retrieves entities from IM
	 * @return a set of snomed codes
	 * @throws TTFilerException if using rdf4j
	 */
	public  Set<String> importEntities() throws TTFilerException, IOException {
		if (TTFilerFactory.isBulk())
			return fileRepo.getAllEntities();
		else {
			Set<String> entities = new HashSet<>();
			return importAllRDF4J(entities);
		}
	}



	/**
	 * Gets all entities and includes their legacy map if they have one
	 * @return A Map of all entites and the set of iris they match to
	 * @throws IOException if using the file repository
	 * @throws TTFilerException if using the graph repository
	 */
	public Map<String,Set<String>> getAllPlusMatches() throws IOException, TTFilerException {
		Set<String> all= importEntities();
		Map<String,Set<String>> legacyCore= getAllMatchedLegacy();
		Map<String,Set<String>> allAndMatched= new HashMap<>();
		for (String iri:all){
			allAndMatched.put(iri,legacyCore.get(iri));
		}
		return allAndMatched;
	}

	/**
	 * Retieves read to Snomed maps, using the Vision code scheme as a proxy for read
	 * @return the code to Snomed code one to many map
	 * @throws TTFilerException when code maps are missing
	 */
	public Map<String,Set<String>> importReadToSnomed() throws TTFilerException, IOException {
		Map<String,Set<String>> readToSnomed= new HashMap<>();
		if (TTFilerFactory.isBulk()){
			return fileRepo.getCodeCoreMap(IM.CODE_SCHEME_EMIS.getIri());
		}
		return importReadToSnomedRdf4j(readToSnomed);
	}

	/**
	 * Gets descendant codes for an iri and its terms;
	 * @param concept the iri for the parent concept
	 * @return A map from code to many terms;
	 * @throws TTFilerException when descendants of concept are missing. Set as specialConcept in TTBulkFiler
	 */
	public Map<String,Set<String>> getDescendants(String concept) throws TTFilerException, IOException {
		if (TTFilerFactory.isBulk())
			return fileRepo.getDescendants(concept);
		return getDescendantsRDF(concept);
	}

	public Map<String,Set<String>> getDescendantsRDF(String concept) throws TTFilerException {
		Map<String, Set<String>> codeToTerm = new HashMap<>();
		RepositoryConnection conn = ConnectionManager.getIMConnection();
		TupleQuery qry = conn.prepareTupleQuery("select ?child ?name\n" +
			"where {GRAPH <"+SNOMED.GRAPH_SNOMED.getIri()+"> { ?child <" + RDFS.SUBCLASSOF.getIri() + ">+ ?concept.\n" +
			"?child <" + RDFS.LABEL.getIri() + "> ?name.}}");
		qry.setBinding("concept", valueFactory.createIRI(concept));
		try {
			TupleQueryResult rs = qry.evaluate();
			while (rs.hasNext()) {
				BindingSet bs = rs.next();
				String child = bs.getValue("child").stringValue();
				String term = bs.getValue("name").stringValue();
				Set<String> maps = codeToTerm.computeIfAbsent(child, k -> new HashSet<>());
				maps.add(term);
			}

		} catch (RepositoryException e) {
			throw new TTFilerException("Unable to retrieve snomed codes");
		}
		return codeToTerm;
	}





	private Set<String> importAllRDF4J(Set<String> entities) throws TTFilerException {

		try (RepositoryConnection conn= ConnectionManager.getIMConnection()){
			TupleQuery qry= conn.prepareTupleQuery("select distinct ?entity\n"+
				"where {?entity  <http://www.w3.org/2000/01/rdf-schema#label> ?label." +
				" filter (isIri(?entity))}");
			TupleQueryResult rs= qry.evaluate();
			while (rs.hasNext()){
				BindingSet bs=rs.next();
				entities.add(bs.getValue("entity").stringValue());
			}
		}catch (RepositoryException e){
			throw new TTFilerException("Unable to retrieve entities");
		}
		return entities;
	}





	private Set<String> importSnomedRDF4J(Set<String> snomedCodes) throws TTFilerException {

		try (RepositoryConnection conn= ConnectionManager.getIMConnection()){
			TupleQuery qry= conn.prepareTupleQuery("select ?snomed\n"+
				"where {?concept <"+ IM.HAS_SCHEME.getIri()+"> <"+SNOMED.GRAPH_SNOMED.getIri()+">.\n"+
				"?concept <"+IM.CODE.getIri()+"> ?snomed}");
			TupleQueryResult rs= qry.evaluate();
			while (rs.hasNext()){
				BindingSet bs=rs.next();
				snomedCodes.add(bs.getValue("snomed").stringValue());
			}
		}catch (RepositoryException e){
			throw new TTFilerException("Unable to retrieve snomed codes");
		}
		return snomedCodes;
	}



	private Map<String, Set<String>> importReadToSnomedRdf4j(Map<String, Set<String>> readToSnomed) throws TTFilerException {

		try (RepositoryConnection conn= ConnectionManager.getIMConnection()){
			TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed\n"+
				"where {GRAPH <"+IM.GRAPH_VISION.getIri()+"> {?concept <"+IM.CODE.getIri()+"> ?code. \n"+
				"?concept <"+IM.MATCHED_TO.getIri()+"> ?snomed.}}");
			TupleQueryResult rs= qry.evaluate();
			while (rs.hasNext()){
				BindingSet bs= rs.next();
				String read= bs.getValue("code").stringValue();
				String snomed= bs.getValue("snomed").stringValue();
				Set<String> maps = readToSnomed.computeIfAbsent(read, k -> new HashSet<>());
				maps.add(snomed);
			}
		} catch (RepositoryException e){
			throw new TTFilerException("unable to retrieve vision/read "+e);
		}
		return readToSnomed;
	}

	public Map<String, TTEntity> getEMISReadAsVision() throws IOException {
		if (TTFilerFactory.isBulk()) {
			Map<String,Set<String>> emisToCore= fileRepo.getCodeCoreMap(IM.CODE_SCHEME_EMIS.getIri());
			Map<String,TTEntity> emisRead2= new HashMap<>();
			for (Map.Entry<String,Set<String>> entry:emisToCore.entrySet()) {
				String code = entry.getKey();
				if (isRead(code)) {
					code = (code + ".....").substring(0, 5);
					TTEntity entity = emisRead2.computeIfAbsent(code, k -> new TTEntity());
					entity.setCode(code);
					entity.setScheme(IM.CODE_SCHEME_VISION);
					entity.setIri(IM.GRAPH_VISION.getIri() + code.replace(".", ""));
					for (String snomed : entry.getValue()) {
						entity.addObject(IM.MATCHED_TO, TTIriRef.iri(snomed));
					}
				}
			}
			return emisRead2;

		} else
			return getEMISReadAsVisionRdf4j();
	}

	private Map<String, TTEntity> getEMISReadAsVisionRdf4j() {
		Map<String,TTEntity> emisRead2= new HashMap<>();
		try (RepositoryConnection conn= ConnectionManager.getIMConnection()) {
			StringJoiner sql = new StringJoiner("\n");
			sql.add("SELECT ?code ?name ?snomed");
			sql.add("WHERE {");
			sql.add("Graph <" + IM.GRAPH_EMIS.getIri() + "> {");
			sql.add("?concept <" + IM.CODE.getIri() + "> ?code.");
			sql.add("?concept <" + RDFS.LABEL.getIri() + "> ?name.");
			sql.add("?concept <" + IM.MATCHED_TO.getIri() + "> ?snomed. } }");
			TupleQuery qry = conn.prepareTupleQuery(sql.toString());
			TupleQueryResult rs = qry.evaluate();
			while (rs.hasNext()) {
				BindingSet bs = rs.next();
				String code = bs.getValue("code").stringValue();
				String name = bs.getValue("name").stringValue();
				String snomed = bs.getValue("snomed").stringValue();
				if (isRead(code)) {
					code = (code + ".....").substring(0, 5);
					TTEntity entity = emisRead2.computeIfAbsent(code, k -> new TTEntity());
					entity.setName(name);
					entity.setCode(code);
					entity.setIri(IM.GRAPH_VISION.getIri() + code.replace(".", ""));
					entity.addObject(IM.MATCHED_TO, TTIriRef.iri(SNOMED.NAMESPACE + snomed));
				}
			}
		}
		return emisRead2;
	}
	public Boolean isRead(String s){
		if (s.length()<6)
			return !s.contains("DRG") && !s.contains("SHAPT") && !s.contains("EMIS") && !s.contains("-");
		else
			return false;
	}


	private Map<String,Set<String>> importEmisToSnomedRdf4j() throws TTFilerException {
		Map<String,Set<String>> emisToSnomed= new HashMap<>();
		RepositoryConnection conn= ConnectionManager.getIMConnection();
		TupleQuery qry= conn.prepareTupleQuery("select ?code ?snomed  ?name\n"+
			"where {GRAPH <"+IM.GRAPH_EMIS.getIri()+"> \n"+
			"{?concept <"+IM.CODE.getIri()+"> ?code. \n"+
			"?concept <"+RDFS.LABEL.getIri()+"> ?name.\n"+
			"?concept <"+IM.MATCHED_TO.getIri()+"> ?snomedIri.}\n" +
			"GRAPH <"+SNOMED.GRAPH_SNOMED.getIri()+"> {"+
			"?snomedIri <"+IM.CODE.getIri()+"> ?snomed.}}");
		try {
			TupleQueryResult rs= qry.evaluate();
			while (rs.hasNext()){
				BindingSet bs= rs.next();
				String read= bs.getValue("code").stringValue();
				String snomed= bs.getValue("snomed").stringValue();
				Set<String> maps = emisToSnomed.computeIfAbsent(read, k -> new HashSet<>());
				maps.add(snomed);
			}
			return emisToSnomed;

		} catch (RepositoryException e){
			throw new TTFilerException("unable to retrieve vision/read "+e);
		}
	}


	/**
	 * Extracts term codes from Snomed entities
	 * @return Map of description code to entity
	 */
	public Map<String, String> getDescriptionIds() throws TTFilerException {
		Map<String,String> termMap= new HashMap<>();
		try (RepositoryConnection conn= ConnectionManager.getIMConnection();){
			TupleQuery qry= conn.prepareTupleQuery("PREFIX im: <http://endhealth.info/im#>\n" +
				"select ?snomed ?descid\n" +
				"where { graph <http://snomed.info/sct#> {\n" +
				"    ?snomed im:hasTermCode ?node.\n" +
				"    ?node im:code ?descid. }}");
			TupleQueryResult rs= qry.evaluate();
			while (rs.hasNext()){
				BindingSet bs=rs.next();
				termMap.put(bs.getValue("descid").stringValue(),bs.getValue("snomed").stringValue());
			}
		}catch (RepositoryException e){
			throw new TTFilerException("Unable to retrieve snomed term codes");
		}
		return termMap;
	}


	public Set<TTIriRef> getLegacyFromTermCode(String originalCode, String iri) throws IOException {
		if (TTFilerFactory.isBulk()){
			return fileRepo.getReferenceFromTermCode(originalCode,iri);
		}
		else
			return new EntityRepository2().getReferenceFromTermCode(originalCode,iri);
	}
}
