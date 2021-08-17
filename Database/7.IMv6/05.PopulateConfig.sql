USE im6;
DELETE FROM config WHERE name = 'definition';
INSERT INTO config (name, config) VALUES ('definition', '[
    {
        "label": "Name",
        "predicate": "http://www.w3.org/2000/01/rdf-schema#label",
        "type": "TextWithLabel",
        "size": "50%",
        "order": 0
    },
    {
        "label": "Iri",
        "predicate": "@id",
        "type": "TextWithLabel",
        "size": "50%",
        "order": 1
    },
    {
        "label": "Status",
        "predicate": "http://endhealth.info/im#status",
        "type": "ObjectNameWithLabel",
        "size": "50%",
        "order": 2
    },
    {
        "label": "Types",
        "predicate": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
        "type": "ArrayObjectNamesToStringWithLabel",
        "size": "50%",
        "order": 3
    },
    {
        "label": "Description",
        "predicate": "http://www.w3.org/2000/01/rdf-schema#comment",
        "type": "TextHTMLWithLabel",
        "size": "100%",
        "order": 4
    },
    {
        "label": "Divider",
        "predicate": "None",
        "type": "Divider",
        "size": "100%",
        "order": 5
    },
    {
        "label": "Is a",
        "predicate": "http://endhealth.info/im#isA",
        "type": "ArrayObjectNameListboxWithLabel",
        "size": "50%",
        "order": 6
    },
    {
        "label": "Has sub types",
        "predicate": "subtypes",
        "type": "ArrayObjectNameListboxWithLabel",
        "size": "50%",
        "order": 7
    },
    {
        "label": "Is child of",
        "predicate": "http://endhealth.info/im#isChildOf",
        "type": "ArrayObjectNameListboxWithLabel",
        "size": "50%",
        "order": 8
    },
    {
        "label": "Has children",
        "predicate": "http://endhealth.info/im#hasChildren",
        "type": "ArrayObjectNameListboxWithLabel",
        "size": "50%",
        "order": 9
    },
    {
        "label": "Divider",
        "predicate": "None",
        "type": "Divider",
        "size": "100%",
        "order": 10
    },
    {
        "label": "Terms",
        "predicate": "termCodes",
        "type": "TermsTable",
        "size": "100%",
        "order": 11
    },
    {
        "label": "Divider",
        "predicate": "None",
        "type": "Divider",
        "size": "100%",
        "order": 12
    },
    {
        "label": "Semantic properties",
        "predicate": "semanticProperties",
        "type": "SemanticProperties",
        "size": "100%",
        "order": 13
    },
    {
        "label": "Divider",
        "predicate": "None",
        "type": "Divider",
        "size": "100%",
        "order": 14
    },
    {
        "label": "Data model properties",
        "predicate": "dataModelProperties",
        "type": "DataModelProperties",
        "size": "100%",
        "order": 15
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