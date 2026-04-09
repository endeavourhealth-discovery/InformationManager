package compass;

import org.apache.commons.logging.Log;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.endeavourhealth.imapi.dataaccess.databases.IMDB;
import org.endeavourhealth.informationmanager.transforms.sources.SnomedImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class CompassIMGenerator {
	private static final Logger LOG = LoggerFactory.getLogger(CompassIMGenerator.class);
	public void generate(String folder) throws Exception{
		if (!folder.endsWith("\\")) folder += "\\";
		LOG.info("Generating Compass tct files");
		outputTct(folder);
		LOG.info("Generating Compass concept files");
		outputConcepts(folder);
	}

	private void outputConcepts(String folder) throws Exception {
		Map<String, String> conceptMap = new HashMap<>();
		try (FileWriter conceptWriter = new FileWriter(folder + "concept_im2.txt")) {
			try (IMDB conn = IMDB.getConnection()) {
				String spq = """
					select ?iri ?im1Dbid ?label ?code
					where {
					?iri im:im1DbId ?im1Dbid.
					?iri rdfs:label ?label.
					?iri im:code ?code.
					}
					""";
				TupleQuery qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String iri = bs.getValue("iri").stringValue();
						String im1Dbid = bs.getValue("im1Dbid").stringValue();
						String label = bs.getValue("label").stringValue();
						String code = bs.getValue("code").stringValue();
						conceptMap.put(im1Dbid,code+"\t"+label+"\t"+iri);

					}
				}
			}
			for (Map.Entry<String, String> entry : conceptMap.entrySet()) {
				conceptWriter.write(entry.getKey()+"\t"+ entry.getValue() + "\n");
			}
		}
	}

	private void outputTct(String folder) throws Exception{
		try (FileWriter tctWriter = new FileWriter(folder+"tct.txt")) {
			try (IMDB conn = IMDB.getConnection()){
				String spq = """
					select ?parent ?child ?childDbId
					where {
					?child im:isA ?parent.
					?child im:im1DbId ?childDbId.
					}
					""";
				TupleQuery qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String childDbId = bs.getValue("childDbId").stringValue();
						String parent = bs.getValue("parent").stringValue();
						String child = bs.getValue("child").stringValue();
						tctWriter.write(parent + "\t" + childDbId + "\t" + (parent.equals(child) ? 1 : 0) + "\n");
					}
				}
				spq = """
					select ?set ?memberDbId
					where {
						?set im:hasMember ?member.
						?member im:im1DbId ?memberDbId.
					}
					""";
				qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String set = bs.getValue("set").stringValue();
						String memberDbId = bs.getValue("memberDbId").stringValue();
						tctWriter.write(set + "\t" + memberDbId + "\t" + "0" + "\n");
					}
				}
				spq = """
					select ?subset ?set ?memberDbId
					where {
					  ?subset im:isSubsetOf ?set.
						?subset im:hasMember ?member.
						?member im:im1DbId ?memberDbId.
					}
					""";
				qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String set = bs.getValue("set").stringValue();
						String subset = bs.getValue("subset").stringValue();
						String memberDbId = bs.getValue("memberDbId").stringValue();
						tctWriter.write(set + "\t" + memberDbId + "\t" + "0" + "\n");
						tctWriter.write(subset + "\t" + memberDbId + "\t" + "0" + "\n");
					}
				}
			}
		}
	}

}

