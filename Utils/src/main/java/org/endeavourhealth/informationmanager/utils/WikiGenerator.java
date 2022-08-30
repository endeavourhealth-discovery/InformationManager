package org.endeavourhealth.informationmanager.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.endeavourhealth.imapi.logic.service.SearchService;
import org.endeavourhealth.imapi.model.sets.ConceptRef;
import org.endeavourhealth.imapi.model.sets.QueryRequest;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.model.tripletree.TTIriRef;
import org.endeavourhealth.imapi.model.tripletree.TTNode;
import org.endeavourhealth.imapi.model.tripletree.TTValue;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.IM;
import org.endeavourhealth.imapi.vocabulary.RDFS;
import org.endeavourhealth.imapi.vocabulary.SHACL;
import org.endeavourhealth.imapi.vocabulary.XSD;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

public class WikiGenerator {
	private StringBuilder table;
	private final List<String> shapesToDo= new ArrayList<>();
	private final List<String> shapesDone= new ArrayList<>();
	private final List<String> veto= new ArrayList<>();

	public String generateDocs() throws DataFormatException, JsonProcessingException {
		StringBuilder documentation = new StringBuilder();
		veto.add(IM.NAMESPACE+"Organisation");
		veto.add(IM.NAMESPACE+"ComputerSystem");
		List<String> folders=List.of("BasicShapes","QueryShapes","DataModelShapes","ConceptShapes","TransactionalShapes");
		for (String folder:folders) {
			String folderIri = IM.NAMESPACE + folder;
			TTEntity heading = getEntity(folderIri);
			documentation.append("== ").append(heading.getName()).append(" ==\n");
			documentation.append(heading.getDescription()).append("\n");
			List<TTEntity> ordered= getFolderContent(folderIri);
			documentation.append(generateTable(ordered));
			documentation.append("\n");

		}
		return documentation.toString();
	}


	public  String generateTable(List<TTEntity> shapes) throws DataFormatException, JsonProcessingException {
		table= new StringBuilder();
		table.append("{| class=\"wikitable\"\n" +
			"|+\n" +
			"!Shape \n" +
			"!colspan=\"2\"|Property\n" +
			"!Card.\n"+
			"!Value type\n"+
			"!Comment\n"+
			"|-\n");
		for (TTEntity shape:shapes) {

			processClass(shape.getIri());

			for (int i = 0; i < shapesToDo.size(); i++) {
				processClass(shapesToDo.get(i));
				table.append("\n|-\n");
			}
		}
		table.append("\n|}\n");
		return table.toString();
	}

	private String localName(String iri){
		return iri.substring(iri.lastIndexOf("#")+1);
	}

	private int getRowSpan(TTEntity shape){
		int rowSpan=0;
		if (shape.get(SHACL.PROPERTY)!=null){
			for (TTValue prop:shape.get(SHACL.PROPERTY).getElements()){
				if (prop.asNode().get(IM.INHERITED_FROM)==null) {
					if (prop.asNode().get(SHACL.OR) != null) {
						rowSpan = rowSpan + prop.asNode().get(SHACL.OR).size();
					} else
						rowSpan++;
				}
			}
		}
		return rowSpan;
	}

	private void processClass(String iri) throws DataFormatException, JsonProcessingException {
		if (shapesDone.contains(iri))
			return;
		shapesDone.add(iri);
		TTEntity shape = getShape(iri);
		String name= shape.getIri().substring(shape.getIri().lastIndexOf("#")+1);
		TTIriRef target= null;
		if (shape.get(SHACL.TARGETCLASS)!=null)
			target= shape.get(SHACL.TARGETCLASS).asIriRef();
		String link;
		if (target!=null) {
			name= target.getIri().substring(target.getIri().lastIndexOf("#")+1);
			link= getLink(target.getIri());
		}
		else
			link= getLink(iri);
		int rowSpan=0;
		if (shape.get(SHACL.PROPERTY)!=null)
			rowSpan= getRowSpan(shape);
		if (rowSpan>0)
			table.append("|rowspan=\"").append(rowSpan + 1).append("\"");
		table.append("|").append("<span id=\"class_").append(name).append("\">").append("[")
			.append(link).append(" ").append(" <span style=\"color:navy\"> '''")
			.append(name).append("'''</span>]");
		if (shape.get(RDFS.SUBCLASSOF)!=null){
			TTEntity superShape=  getEntity(shape.get(RDFS.SUBCLASSOF).asIriRef().getIri());
			table.append("<br> (subtype of " + "[[#class_").append(superShape.getName()).append("|").append(superShape.getName()).append("]])");
			shapesToDo.add(superShape.getIri());
		}
		table.append("</span>");

		table.append("\n|colspan=\"4\"|");
		table.append("\n|");
		table.append(shape.getDescription());
		table.append("\n|-");
		table.append("\n|");
		if (shape.get(SHACL.PROPERTY)==null){
			table.append("\n|-");
		}
		else {

			List<TTNode> properties = shape.get(SHACL.PROPERTY).getElements().stream().map(TTValue::asNode)
				.sorted(Comparator.comparing((TTNode p) -> p.get(SHACL.ORDER).asLiteral().intValue()))
				.collect(Collectors.toList());
			int propCount = 0;
			for (TTNode property : properties) {
				if (property.get(IM.INHERITED_FROM)!=null)
					continue;
				propCount++;
				if (property.get(SHACL.OR) == null) {
					processField(property, 2);
					processCardinality(property);
					processType(property);
					processComment(property);
				} else {
					table.append("rowspan=\"").append(property.get(SHACL.OR).size()).append("\"|");
					String card= getCardinality(property);
					table.append("or<br>").append(card).append("\n|");
					int orCount = 0;
					for (TTValue orProp : property.get(SHACL.OR).getElements()) {
						orCount++;
						processField(orProp.asNode(), 1);
						if (orCount==1) {
							processCardinality(orProp.asNode());
						}
						else {
							processCardinality(orProp.asNode());
						}
						processType(orProp.asNode());
						processComment(orProp.asNode());
						if (orCount < property.get(SHACL.OR).getElements().size())
							table.append("\n|\n");
					}
				}
				if (propCount < properties.size())
					table.append("\n|");
			}
		}
	}




	private void processType(TTNode prop) throws DataFormatException, JsonProcessingException {
		TTIriRef type=null;
		String title="";
		for (TTIriRef test : List.of(SHACL.NODE, SHACL.NODE_KIND, SHACL.DATATYPE,SHACL.CLASS)) {
			if (prop.get(test) != null) {
				type = prop.get(test).asIriRef();
				title = getTitle(type);
			}
		}
		if (type==null)
			throw new DataFormatException("Unknown property type in shape");
		String link= getLink(type.getIri());
		String localType= localName(type.getIri());
		if (prop.get(SHACL.NODE)!=null) {
			if (!veto.contains(type.getIri())){
				if (!shapesToDo.contains(type.getIri())) {
					shapesToDo.add(type.getIri());
				}
			table.append("<span title=\"").append(title).append("\">[[#class_").append(localType).append("|").append(localType).append("]]</span>\n|");
			}
			else
				table.append("<span title=\"").append(title).append("\">").append("[").append(link).append(" ").append(getShort(type.getIri())).append("]</span>\n|");
		}
		else {
			table.append("<span title=\"").append(title).append("\">").append("[").append(link).append(" ").append(getShort(type.getIri())).append("]</span>\n|");
		}
	}

	private String getLink(String iri){
		if (iri.contains("http://www.w3.org/2001/XMLSchema#"))
			return ("https://www.w3.org/TR/xmlschema-2/#"+iri.substring(iri.lastIndexOf("#")+1));
		String link="https://im.endeavourhealth.net/viewer/#/concept/";
		String escaped= iri.replaceAll(":","%3A")
			.replaceAll("/","%2F")
			.replaceAll("#","%23");
		return link+escaped+"/";
	}

	private String getTitle(TTIriRef iri) throws DataFormatException, JsonProcessingException {
		if (iri.equals(SHACL.IRI))
			return "international resource identifier";
		else if (iri.equals(XSD.STRING))
			return "any valid json value characters with json escapes";
		else if (iri.equals(XSD.INTEGER))
			return "whole number";
		else if (iri.equals(TTIriRef.iri(IM.NAMESPACE+"DateTime")))
			return "im date time format";
		else if (iri.equals(XSD.BOOLEAN))
			return "boolean true or false";
		else {
			TTEntity entity = getEntity(iri.getIri());
			if (entity != null) {
				if (entity.get(RDFS.COMMENT) != null)
					return entity.get(RDFS.COMMENT).asLiteral().getValue().replaceAll("<br>","");
				else
					return "";
			} else
				return "";
		}
	}

	private static String getShort(String iri){
		String prefix= TTManager.getDefaultContext().getPrefix(iri.substring(0,iri.lastIndexOf("#")+1));
		return prefix+":"+ iri.substring(iri.lastIndexOf("#")+1);
	}

	private String getCardinality(TTNode property){
		int min = property.get(SHACL.MINCOUNT) == null ? 0 : property.get(SHACL.MINCOUNT).asLiteral().intValue();
		Integer max = property.get(SHACL.MAXCOUNT) == null ? null : property.get(SHACL.MAXCOUNT).asLiteral().intValue();
		if (min == 0 && max == null)
			return "0..*";
		else if (min == 0 )
			return "0.."+max;
		else if (max == null)
			return min+"..*";
		else
			return min+".."+max;
	}

	private void processCardinality(TTNode property){

		String card=getCardinality(property);
		table.append(card).append("\n|");
	}

	private void processField(TTNode property,int colspan) throws DataFormatException, JsonProcessingException {
		if (colspan > 1)
			table.append("colspan=\"").append(colspan).append("\"|");
		String link= getLink(property.get(SHACL.PATH).asIriRef().getIri());
		String title= getTitle(property.get(SHACL.PATH).asIriRef());
		String fieldName = localName(property.get(SHACL.PATH).asIriRef().getIri());
		table.append("<span title=\"").append(title).append("\">");
		table.append("[").append(link).append(" ").append("<span style=\"color:green\">").append(fieldName).append(" ]</span></span>\n");
		//table.append("<span style=\"color:green\">" + " ["+link+" "+ fieldName + "]</span></span>\n");
		table.append("|");
	}

	private void processComment(TTNode property){
		String comment="";
		if (property.get(RDFS.COMMENT)!=null) {
			comment = property.get(RDFS.COMMENT).asLiteral().getValue();
		}
		table.append(comment).append("\n|-\n");
	}

	private TTEntity getEntity(String iri) throws DataFormatException, JsonProcessingException {
		QueryRequest qr= new QueryRequest()
			.query(q->q
				.select(s->s
					.setEntityId(ConceptRef.iri(iri))));
		List<TTEntity> result= new SearchService().entityQuery(qr);
		return result.get(0);
	}

	private TTEntity getShape(String iri) throws DataFormatException, JsonProcessingException {
		QueryRequest qr= new QueryRequest()
			.query(q->q
				.select(s->s
					.setEntityId(ConceptRef.iri(iri))));
		List<TTEntity> result= new SearchService().entityQuery(qr);
		return result.get(0);
	}

	private List<TTEntity> getFolderContent(String iri) throws DataFormatException, JsonProcessingException {
		QueryRequest qr= new QueryRequest()
			.query(q->q
				.select(s->s
					.property(p->p
						.setIri(RDFS.LABEL))
					.property(p->p
						.setIri(RDFS.COMMENT))
					.property(p->p
						.setIri(IM.ORDER))
					.match(m->m
						.property(p->p
							.setIri(IM.IS_CONTAINED_IN)
							.addIsConcept(TTIriRef.iri(iri))))));
		List<TTEntity> queryResult = new SearchService().entityQuery(qr);
		queryResult.sort(Comparator.comparing((TTNode p) -> p.get(IM.ORDER).asLiteral().intValue()));
		return queryResult;
	}


}
