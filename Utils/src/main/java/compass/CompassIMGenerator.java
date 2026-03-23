package compass;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.endeavourhealth.imapi.dataaccess.databases.IMDB;

import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;

public class CompassIMGenerator {
	public void generate(String folder) throws Exception{
		if (!folder.endsWith("\\")) folder += "\\";
		outputTct(folder);
		outputConcepts(folder);
	}

	private void outputConcepts(String folder) throws Exception {
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
						conceptWriter.write(im1Dbid + "\t" + label + "\t" + code + "\t"+ iri+"\n");
					}
				}
			}
		}
	}

	private void outputTct(String folder) throws Exception{
		Map<String,String> concepts= new HashMap<>();
		try (FileWriter tctWriter = new FileWriter(folder+"tct.txt")) {
			try (IMDB conn = IMDB.getConnection()){
				String spq = """
					select ?parentDbId ?childDbId
					where {
					?child im:isA ?parent.
					?child im:im1DbId ?childDbId.
					?parent im:im1DbId ?parentDbId.
					}
					""";
				TupleQuery qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String childDbId = bs.getValue("childDbId").stringValue();
						String parentDbId = bs.getValue("parentDbId").stringValue();
						tctWriter.write(parentDbId + "\t" + childDbId + "\t" + (parentDbId.equals(childDbId) ? 1 : 0) + "\n");
					}
				}
				spq = """
					select ?set ?member ?setLabel
					where {
						?set im:hasMember ?member.
						?set rdfs:label ?setLabel.
						?set im:im1DbId ?setDbId.
						?member im:im1DbId ?memberDbId.
					}
					""";
				qry = conn.prepareTupleSparql(spq);
				try (TupleQueryResult rs = qry.evaluate()) {
					while (rs.hasNext()) {
						BindingSet bs = rs.next();
						String setDbId = bs.getValue("setDbId").stringValue();
						String memberDbId = bs.getValue("memberDbId").stringValue();
						tctWriter.write(setDbId + "\t" + memberDbId + "\t" + "0" + "\n");
					}
				}
			}

		}
		try (FileWriter conceptWriter = new FileWriter("C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\concept.txt")) {
			for (Map.Entry<String,String> entry : concepts.entrySet()) {
				conceptWriter.write(entry.getKey() + "\t" + entry.getValue() + "\n");
			}
		}
	}

}

