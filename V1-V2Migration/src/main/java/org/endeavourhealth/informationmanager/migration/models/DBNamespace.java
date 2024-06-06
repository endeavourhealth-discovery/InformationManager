package org.endeavourhealth.informationmanager.migration.models;

import java.util.Arrays;
import java.util.List;

public class DBNamespace {
    public static DBNamespace EMPTY = new DBNamespace(1, "", "");
    public static DBNamespace RDFS = new DBNamespace(2, "rdfs:", "http://www.w3.org/2000/01/rdf-schema#");
    public static DBNamespace PROV = new DBNamespace(3, "prov:", "http://www.w3.org/ns/prov#");
    public static DBNamespace CORE = new DBNamespace(4, ":", "http://endhealth.info/im#");
    public static DBNamespace RDF = new DBNamespace(5, "rdf:", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
    public static DBNamespace SNOMED = new DBNamespace(6, "sn:", "http://snomed.info/sct#");
    public static DBNamespace XML = new DBNamespace(7, "xml:", "http://www.w3.org/XML/1998/namespace#");
    public static DBNamespace OWL = new DBNamespace(8, "owl:", "http://www.w3.org/2002/07/owl#");
    public static DBNamespace XS = new DBNamespace(9, "xs:", "http://www.w3.org/2001/XMLSchema#");
    public static DBNamespace READ2 = new DBNamespace(10, "r2:", "http://www.EndeavourHealth.org/InformationModel/Legacy/READ2#");
    public static DBNamespace CTV3 = new DBNamespace(11, "ctv3:", "http://www.EndeavourHealth.org/InformationModel/Legacy/CTV3#");
    public static DBNamespace EMIS = new DBNamespace(12, "emis:", "http://www.EndeavourHealth.org/InformationModel/Legacy/EMIS#");
    public static DBNamespace TPP = new DBNamespace(14, "tpp:", "http://www.EndeavourHealth.org/InformationModel/Legacy/TPP#");
    public static DBNamespace BARTS = new DBNamespace(15, "bc:", "http://www.EndeavourHealth.org/InformationModel/Legacy/Barts_Cerner#");
    public static DBNamespace OROLE = new DBNamespace(16, "orole:", "https://directory.spineservices.nhs.uk/STU3/CodeSystem/ODSAPI-OrganizationRole-1#");

    public static DBNamespace ICD10 = new DBNamespace(17, "icd10:", "?????");
    public static DBNamespace OPCS4 = new DBNamespace(18, "opcs4:", "?????");
    public static DBNamespace FHIRVS = new DBNamespace(19, "fhirvs:", "http://hl7.org/fhir/ValueSet/");
    public static DBNamespace FHIRESD = new DBNamespace(20, "fhiresd:", "http://endeavourhealth.org/fhir/StructureDefinition/");

    public static List<DBNamespace> namespaces = Arrays.asList(
        EMPTY, RDFS, PROV, CORE, RDF, SNOMED, XML, OWL, XS, READ2, CTV3, EMIS, TPP, BARTS, OROLE, ICD10, OPCS4, FHIRVS, FHIRESD
    );

    public int dbid;
    public String prefix;
    public String iri;

    public DBNamespace(int dbid, String prefix, String iri) {
        this.dbid = dbid;
        this.prefix = prefix;
        this.iri = iri;
    }
}
