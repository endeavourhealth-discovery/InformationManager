package org.endeavourhealth.informationmanager.transforms.authored;

import org.endeavourhealth.imapi.logic.service.EntityService;
import org.endeavourhealth.imapi.model.tripletree.*;
import org.endeavourhealth.imapi.transforms.TTManager;
import org.endeavourhealth.imapi.vocabulary.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.DataFormatException;

import static org.endeavourhealth.imapi.model.tripletree.TTIriRef.iri;

public class WikiGenerator {
  private StringBuilder table;
  private final List<String> shapesToDo = new ArrayList<>();
  private final List<String> shapesDone = new ArrayList<>();
  private final List<String> veto = new ArrayList<>();
  private String importFolder;


  public String generateDocs(String importFolder) throws DataFormatException, IOException {
    this.importFolder = importFolder;
    StringBuilder documentation = new StringBuilder();
    veto.add(Namespace.IM + "Organisation");
    veto.add(Namespace.IM + "ComputerSystem");
    List<String> folders = List.of("BasicShapes", "DataModelShapes", "ConceptShapes", "QueryShapes", "TransformMapShapes", "TransactionalShapes");
    for (String folder : folders) {
      String folderIri = Namespace.IM + folder;
      TTEntity heading = getEntity(folderIri);
      documentation.append("== ").append(heading.getName()).append(" ==\n");
      String description = convertHtml(heading.getDescription());
      documentation.append(description).append("\n");
      List<TTEntity> ordered = getFolderContent(folderIri);
      for (TTEntity shape : ordered) {
        documentation.append(generateClass(shape));
      }
      documentation.append("\n");

    }
    return documentation.toString();
  }

  public String generateHeader(TTEntity shape) throws DataFormatException, IOException {
    StringBuilder classText = new StringBuilder();

    String name = shape.getName();
    TTIriRef target = null;
    if (shape.get(iri(SHACL.TARGETCLASS)) != null)
      target = shape.get(iri(SHACL.TARGETCLASS)).asIriRef();
    String link = getLink(shape.getIri());

    classText.append("=== ").append("[")
      .append(link).append(" ").append(name).append("] ===\n");
    if (target != null) {
      TTEntity targetClass = getEntity(target.getIri());
      classText.append("Defines the shape of an instance of the target class : ").append("[")
        .append(getLink(targetClass.getIri())).append(" ").append(targetClass.getName()).append("]\n");
    }


    if (shape.get(iri(RDFS.SUBCLASS_OF)) != null) {
      TTEntity superShape = getEntity(shape.get(iri(RDFS.SUBCLASS_OF)).asIriRef().getIri());
      classText.append("\nIs a subtype of " + "[[#").append(superShape.getName()).append("|").append(superShape.getName()).append("]]\n\n");
      shapesToDo.add(superShape.getIri());
    }

    String description = convertHtml(shape.getDescription());

    classText.append(description).append("\n");
    return classText.toString();


  }

  private String convertHtml(String description) {
    if (description == null)
      return null;
    String[] markups = description.split("<a");
    if (markups.length == 1)
      return description;
    StringBuilder result = new StringBuilder();
    for (String markup : markups) {
      if (!markup.startsWith(" href"))
        result.append(markup);
      else {
        result.append("[");
        String url = markup.split(" href=\"")[1].split("\">")[0];
        String rest = markup.split(" href=\"")[1].split("\">")[1];
        String[] extraText = rest.split("</a>");
        String text = extraText[0];
        result.append(url).append(" ");
        result.append(text).append("]");
        for (int i = 1; i < extraText.length; i++)
          result.append(extraText[i]);

      }
    }
    return result.toString();
  }


  public String generateClass(TTEntity shape) throws DataFormatException, IOException {
    if (shapesDone.contains(shape.getIri()))
      return "";
    System.out.println("Generating " + shape.getIri());
    shapesDone.add(shape.getIri());
    StringBuilder classText = new StringBuilder();
    classText.append(generateHeader(shape));
    classText.append(getTable(shape));
    if (shape.get(iri(IM.EXAMPLE)) != null) {
      classText.append("{{Note| Example <br>");
      classText.append(shape.get(iri(IM.EXAMPLE)).asLiteral().getValue()).append(" }}\n");
    }


    for (String iri : shapesToDo) {
      if (iri.contains("/im#")) {
        TTEntity toDo = getEntity(iri);
        if (!shapesDone.contains(toDo.getIri()))
          classText.append(generateClass(toDo));
      }
    }

    return classText.toString();
  }

  private String getTable(TTEntity shape) throws DataFormatException, IOException {
    table = new StringBuilder();
    table.append("""
      {| class="wikitable"
      |+
      |colspan="2"|Property
      |Card.
      |Value type
      |Description
      |-
      """);

    processProperties(shape);
    table.append("\n|}\n");
    return table.toString();
  }

  private String localName(String iri) {
    return iri.substring(iri.lastIndexOf("#") + 1);
  }

  private int getRowSpan(TTEntity shape) {
    int rowSpan = 0;
    if (shape.get(iri(SHACL.PROPERTY)) != null) {
      for (TTValue prop : shape.get(iri(SHACL.PROPERTY)).getElements()) {
        if (prop.asNode().get(iri(IM.INHERITED_FROM)) == null) {
          if (prop.asNode().get(iri(SHACL.OR)) != null) {
            rowSpan = rowSpan + prop.asNode().get(iri(SHACL.OR)).size();
          } else
            rowSpan++;
        }
      }
    }
    return rowSpan;
  }

  private void processProperties(TTEntity shape) throws DataFormatException, IOException {


    int rowSpan = 0;
    if (shape.get(iri(SHACL.PROPERTY)) != null)
      rowSpan = getRowSpan(shape);
    if (rowSpan > 0)


      if (shape.get(iri(SHACL.PROPERTY)) == null) {
        table.append("\n|-");
      } else {

        List<TTNode> properties = shape.get(iri(SHACL.PROPERTY)).getElements().stream().map(TTValue::asNode)
          .sorted(Comparator.comparing((TTNode p) -> p.get(iri(SHACL.ORDER)).asLiteral().intValue()))
          .toList();
        int propCount = 0;
        for (TTNode property : properties) {
          if (property.get(iri(IM.INHERITED_FROM)) != null)
            continue;
          propCount++;
          if (property.get(iri(SHACL.OR)) == null) {
            processField(property, 2);
            processCardinality(property);
            processType(property);
            processComment(property);
          } else {
            table.append("|rowspan=\"").append(property.get(iri(SHACL.OR)).size()).append("\"|");
            String card = getCardinality(property);
            table.append("or<br>").append(card).append("\n|");
            int orCount = 0;
            for (TTValue orProp : property.get(iri(SHACL.OR)).getElements()) {
              orCount++;
              processField(orProp.asNode(), 1);
              if (orCount == 1) {
                processCardinality(orProp.asNode());
              } else {
                processCardinality(orProp.asNode());
              }
              processType(orProp.asNode());
              processComment(orProp.asNode());
              if (orCount < property.get(iri(SHACL.OR)).getElements().size())
                table.append("\n|\n");
            }
          }

        }
      }


  }


  private void processType(TTNode prop) throws DataFormatException, IOException {
    TTIriRef type = null;
    String title = "";
    for (TTIriRef test : List.of(iri(SHACL.NODE), iri(SHACL.NODE_KIND), iri(SHACL.DATATYPE), iri(SHACL.CLASS))) {
      if (prop.get(test) != null) {
        type = prop.get(test).asIriRef();
        title = getTitle(type);
      }
    }
    if (type == null)
      throw new DataFormatException("Unknown property type in shape");
    String link = getLink(type.getIri());
    String localType = localName(type.getIri());
    if (prop.get(iri(SHACL.NODE)) != null) {
      if (!veto.contains(type.getIri())) {
        if (!shapesToDo.contains(type.getIri())) {
          shapesToDo.add(type.getIri());
        }
        table.append("<span title=\"").append(title).append("\">[[#").append(localType).append("|").append(localType).append("]]</span>\n|");
      } else
        table.append("<span title=\"").append(title).append("\">").append("[").append(link).append(" ").append(getShort(type.getIri())).append("]</span>\n|");
    } else {
      table.append("<span title=\"").append(title).append("\">").append("[").append(link).append(" ").append(getShort(type.getIri())).append("]</span>\n|");
    }
  }

  private String getLink(String iri) {
    if (iri.contains("http://www.w3.org/2001/XMLSchema#"))
      return ("https://www.w3.org/TR/xmlschema-2/#" + iri.substring(iri.lastIndexOf("#") + 1));
    String link = "https://im.endeavourhealth.net/viewer/#/concept/";
    String escaped = iri.replaceAll(":", "%3A")
      .replaceAll("/", "%2F")
      .replaceAll("#", "%23");
    return link + escaped + "/";
  }

  private String getTitle(TTIriRef iri) throws DataFormatException, IOException {
    if (iri.getIri().equals(SHACL.IRI.toString())) {
      return "international resource identifier";
    } else if (iri.getIri().equals(XSD.STRING.toString())) {
      return "any valid json value characters with json escapes";
    } else if (iri.getIri().equals(XSD.INTEGER.toString())) {
      return "whole number";
    } else if (iri.getIri().equals((Namespace.IM + "DateTime"))) {
      return "im date time format";
    } else if (iri.getIri().equals(XSD.BOOLEAN.toString())) {
      return "boolean true or false";
    }
    TTEntity entity = getEntity(iri.getIri());
    if (entity != null) {
      if (entity.get(iri(RDFS.COMMENT)) != null)
        return entity.get(iri(RDFS.COMMENT)).asLiteral().getValue().replaceAll("<br>", "");
      else
        return "";
    } else
      return "";
  }

  private static String getShort(String iri) {
    String prefix = TTManager.getDefaultContext().getPrefix(iri.substring(0, iri.lastIndexOf("#") + 1));
    return prefix + ":" + iri.substring(iri.lastIndexOf("#") + 1);
  }

  private String getCardinality(TTNode property) {
    int min = property.get(iri(SHACL.MINCOUNT)) == null ? 0 : property.get(iri(SHACL.MINCOUNT)).asLiteral().intValue();
    Integer max = property.get(iri(SHACL.MAXCOUNT)) == null ? null : property.get(iri(SHACL.MAXCOUNT)).asLiteral().intValue();
    if (min == 0 && max == null)
      return "0..*";
    else if (min == 0)
      return "0.." + max;
    else if (max == null)
      return min + "..*";
    else
      return min + ".." + max;
  }

  private void processCardinality(TTNode property) {

    String card = getCardinality(property);
    table.append(card).append("\n|");
  }

  private void processField(TTNode property, int colspan) throws DataFormatException, IOException {
    if (colspan > 1)
      table.append("|colspan=\"").append(colspan).append("\"|");
    String link = getLink(property.get(iri(SHACL.PATH)).asIriRef().getIri());
    String title = getTitle(property.get(iri(SHACL.PATH)).asIriRef());
    String fieldName = localName(property.get(iri(SHACL.PATH)).asIriRef().getIri());
    table.append("<span title=\"").append(title).append("\">");
    table.append("[").append(link).append(" ").append("<span style=\"color:green\">").append(fieldName).append(" ]</span></span>\n");
    //table.append("<span style=\"color:green\">" + " ["+link+" "+ fieldName + "]</span></span>\n");
    table.append("|");
  }

  private void processComment(TTNode property) {
    String comment = "";
    if (property.get(iri(RDFS.COMMENT)) != null) {
      comment = property.get(iri(RDFS.COMMENT)).asLiteral().getValue();
    }
    table.append(comment).append("\n|-\n");
  }

  private TTEntity getEntity(String iri) throws DataFormatException, IOException {
    for (String document : List.of("/DiscoveryCore/CoreOntology", "/SemanticWeb/RDFOntology", "/SemanticWeb/RDFSOntology",
      "/SemanticWeb/SHACLOntology", "/SemanticWeb/OWLOntology", "/DiscoveryCore/Sets")) {
      String file = importFolder + document + ".json";
      try (TTManager manager = new TTManager()) {
        manager.loadDocument(new File(file));
        TTEntity entity = manager.getEntity(iri);
        if (entity != null)
          return entity;
      }
    }
    EntityService es = new EntityService();
    TTBundle bundle = es.getBundle(iri, null);
    TTEntity entity = bundle.getEntity();
    if (entity != null)
      return entity;
    else
      throw new DataFormatException("Unknown entity :" + iri);
  }


  private List<TTEntity> getFolderContent(String iri) throws IOException {
    try (TTManager manager = new TTManager()) {
      manager.loadDocument(new File(importFolder + "/DiscoveryCore/CoreOntology.json"));
      List<TTEntity> folders = new ArrayList<>();
      for (TTEntity entity : manager.getDocument().getEntities()) {
        if (entity.get(iri(IM.IS_CONTAINED_IN)) != null) {
          for (TTValue value : entity.get(iri(IM.IS_CONTAINED_IN)).getElements()) {
            if (value.asIriRef().getIri().equals(iri))
              folders.add(entity);
          }
        }
      }
      folders.sort(Comparator.comparing((TTNode p) -> p.get(iri(SHACL.ORDER)).asLiteral().intValue()));
      return folders;
    }
  }

}
