package org.endeavourhealth.informationmanager.transforms.authored;

import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.util.*;
import java.util.stream.Collectors;

public class WikiGenerator {
	private StringBuilder table = new StringBuilder();
	private TTManager manager;
	private List<String> shapesToDo= new ArrayList<>();
	private List<String> shapesDone= new ArrayList<>();



	public  StringBuilder generateTable(TTManager manager){
		this.manager= manager;
		table.append("{| class=\"wikitable\"\n" +
			"|+\n" +
			"!Class\n" +
			"!colspan=\"2\"|Field\n" +
			"!colspan=\"2\"|Card.\n"+
			"!Type\n"+
			"!Comment\n"+
			"|-\n");
		String[] shapes= {"QueryRequest"};
		for (String iri:shapes) {
			processClass(IM.NAMESPACE+iri,"QueryRequest");
			table.append("\n|-\n");


		}

		for (int i=0; i<shapesToDo.size();i++){
				processClass(shapesToDo.get(i),manager.getEntity(shapesToDo.get(i)).getName());
				table.append("\n|-\n");
			}

		table.append("\n|}");
		return table;

	}

	private String localName(String iri){
		return iri.substring(iri.lastIndexOf("#")+1);
	}

	private int getRowSpan(TTEntity shape){
		int rowSpan=0;
		if (shape.get(SHACL.PROPERTY)!=null){
			for (TTValue prop:shape.get(SHACL.PROPERTY).getElements()){
				if (prop.asNode().get(SHACL.OR)!=null) {
					rowSpan = rowSpan + prop.asNode().get(SHACL.OR).size();
				}
				else
					rowSpan++;
			}
		}
		return rowSpan;
	}

	private <T> void processClass(String iri,String name) {
		if (shapesDone.contains(iri))
			return;
		shapesDone.add(iri);
		String link= getLink(iri);
		TTEntity shape = manager.getEntity(iri);
		int rowSpan=0;
		if (shape.get(SHACL.PROPERTY)!=null)
			rowSpan= getRowSpan(shape);
		if (rowSpan>0)
			table.append("|rowspan=\""+rowSpan+"\"");

		table.append("|").append("<span id=\"class_").append(name).append("\">")
			.append("["+link+" "+" <span style=\"color:navy\"> '''"+name+"'''</span>]");
		if (shape.get(RDFS.SUBCLASSOF)!=null){
			TTEntity superShape=  manager.getEntity(shape.get(RDFS.SUBCLASSOF).asIriRef().getIri());
			table.append("<br> (subtype of "+ "[[#class_"+superShape.getName()+"|"+superShape.getName()+"]])");
			shapesToDo.add(superShape.getIri());
		}
		table.append("</span>");
		table.append("\n|");
		if (shape.get(SHACL.PROPERTY)==null){
			table.append("\n|-");
		}
		else {

			List<TTNode> properties = shape.get(SHACL.PROPERTY).getElements().stream().map(TTValue::asNode).collect(Collectors.toList());
			Collections.sort(properties, Comparator.comparing((TTNode p) -> p.get(SHACL.ORDER).asLiteral().intValue()));
			int propCount = 0;
			for (TTNode property : properties) {
				propCount++;
				if (property.get(SHACL.OR) == null) {
					processField(property, 2);
					processCardinality(property);
					processType(property);
					processComment(property);
				} else {
					table.append("rowspan=\"" + property.get(SHACL.OR).size() + "\"|");
					String card= getCardinality(property);
					table.append("or<br>"+ card+"\n|");
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




	private void processType(TTNode prop) {
		TTIriRef type=null;
		String title="";
		for (TTIriRef test : List.of(SHACL.NODE, SHACL.NODE_KIND, SHACL.DATATYPE,SHACL.CLASS)) {
			if (prop.get(test) != null) {
				type = prop.get(test).asIriRef();
				title = getTitle(type);
			}
		}
		String link= getLink(type.getIri());
		String localType= localName(type.getIri());
		if (prop.get(SHACL.NODE)!=null){
			if (!shapesToDo.contains(type.getIri())) {
				shapesToDo.add(type.getIri());
			}
			table.append("<span title=\""+ title+"\">[[#class_"+ localType+"|"+localType+"]]</span>\n|");
		}
		else {
			table.append("<span title=\""+title+"\">"+"["+link+" "+getShort(type.getIri())+"]</span>\n|");
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

	private String getTitle(TTIriRef iri) {
		if (iri.equals(SHACL.IRI))
			return "international resource identifier";
		else if (iri.equals(XSD.STRING))
			return "any valid json value characters with json escapes";
		else if (iri.equals(XSD.INTEGER))
			return "whole number";
		else if (iri.equals(TTIriRef.iri(IM.NAMESPACE+"DateTime")))
			return "im date time format";
		else {
			TTEntity entity = manager.getEntity(iri.getIri());
			if (entity != null) {
				if (entity.get(RDFS.COMMENT) != null)
					return entity.get(RDFS.COMMENT).asLiteral().getValue();
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
		table.append(card+"\n|");
	}

	private void processField(TTNode property,int colspan) {
		if (colspan > 1)
			table.append("colspan=\"" + colspan + "\"|");
		String fieldName = localName(property.get(SHACL.PATH).asIriRef().getIri());
		table.append("<span style=\"color:green\">" + fieldName + "</span>\n");
		table.append("|");
	}

	private void processComment(TTNode property){
		String comment="";
		if (property.get(RDFS.COMMENT)!=null) {
			comment = property.get(RDFS.COMMENT).asLiteral().getValue();
		}
		table.append(comment).append("\n|-\n");
	}


}
