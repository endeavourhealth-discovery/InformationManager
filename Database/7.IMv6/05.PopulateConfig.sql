USE im6;
DELETE FROM config WHERE name = 'definition';
INSERT INTO config (name, config) VALUES ('definition', '[
  {
    "label": "Summary",
    "predicate": "None",
    "type": "TextSectionHeader",
    "size": "100%",
    "order": 100
  },
  {
    "label": "Name",
    "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
    "type": "TextWithLabel",
    "size": "50%",
    "order": 101
  },
  {
    "label": "Iri",
    "predicate": "@id",
    "type": "TextWithLabel",
    "size": "50%",
    "order": 102
  },
  {
    "label": "Status",
    "predicate": "http://endhealth.info/im#status",
    "type": "ObjectNameWithLabel",
    "size": "50%",
    "order": 103
  },
  {
    "label": "Types",
    "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
    "type": "ArrayObjectNamesToStringWithLabel",
    "size": "50%",
    "order": 104
  },
  {
    "label": "Description",
    "predicate": "http://www.w3.org/2000/01/rdf-schema#comment",
    "type": "TextHTMLWithLabel",
    "size": "100%",
    "order": 105
  },
  {
    "label": "SummaryInferredDivider",
    "predicate": "None",
    "type": "SectionDivider",
    "size": "100%",
    "order": 200
  },
  {
    "label": "Inferred axioms",
    "predicate": "None",
    "type": "TextSectionHeader",
    "size": "100%",
    "order": 201
  },
  {
    "label": "Is a",
    "predicate": "http://endhealth.info/im#isA",
    "type": "ArrayObjectNameListboxWithLabel",
    "size": "100%",
    "order": 202
  },
  {
    "label": "Has sub types",
    "predicate": "subtypes",
    "type": "ArrayObjectNameListboxWithLabel",
    "size": "100%",
    "order": 203
  },
  {
    "label": "Is child of",
    "predicate": "http://endhealth.info/im#isChildOf",
    "type": "ArrayObjectNameListboxWithLabel",
    "size": "50%",
    "order": 204
  },
  {
    "label": "Has children",
    "predicate": "http://endhealth.info/im#hasChildren",
    "type": "ArrayObjectNameListboxWithLabel",
    "size": "50%",
    "order": 205
  },
  {
    "label": "InferredStatedDivider",
    "predicate": "None",
    "type": "SectionDivider",
    "size": "100%",
    "order": 300
  },
  {
    "label": "Stated axioms",
    "predicate": "None",
    "type": "TextSectionHeader",
    "size": "100%",
    "order": 301
  },
  {
    "label": "Stated axioms",
    "predicate": "axioms",
    "type": "Axioms",
    "size": "100%",
    "order": 302
  },
  {
    "label": "StatedTermsDivider",
    "predicate": "None",
    "type": "SectionDivider",
    "size": "100%",
    "order": 400
  },
  {
    "label": "Terms",
    "predicate": "None",
    "type": "TextSectionHeader",
    "size": "100%",
    "order": 401
  },
  {
    "label": "Terms",
    "predicate": "termCodes",
    "type": "TermsTable",
    "size": "100%",
    "order": 402
  },
  {
    "label": "TermsPropertiesDivider",
    "predicate": "None",
    "type": "SectionDivider",
    "size": "100%",
    "order": 500
  },
  {
    "label": "Properties",
    "predicate": "None",
    "type": "TextSectionHeader",
    "size": "100%",
    "order": 501
  },
  {
    "label": "Semantic properties",
    "predicate": "semanticProperties",
    "type": "SemanticProperties",
    "size": "100%",
    "order": 502
  },
  {
    "label": "Data model properties",
    "predicate": "dataModelProperties",
    "type": "DataModelProperties",
    "size": "100%",
    "order": 503
  }
]');
DELETE FROM config WHERE name = 'filterDefaults';
INSERT INTO config (name, config) VALUES ('filterDefaults', '{
	"schemeOptions": [
		"Discovery namespace",
		"Snomed-CT namespace"
	],
    "statusOptions": [
		"Active",
		"Draft"
	],
    "typeOptions": [
        "Class",
        "Concept Set",
        "Folder",
        "Node shape",
        "ObjectProperty",
        "Property",
        "Query template",
        "Record type",
        "Value set"
    ]
}');