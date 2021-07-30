# Importing Source data

To import classification data follow the pattern as described:

## File based imports
Create a root folder of choice. This will hold the files and subfolders for import.

The subfolder and files are specified by each specialist importer as regex file patterns.

Place the core Discovery core ontology (CoreOntologyDocument.json) in the root folder

Place the  Discovery maps  (NoneCoreOntologyDocyment.json in the root folder)

Place the latest Snomed-CT releases (International, UK Clinical and UK Drug) as subfolders without change of name

Place the EMISCodes.csv file in the subfolder EMIS. This is the EMIS/Read2/Local code file with EMIS Snomed map.

Place the OPCS4-9-0-0  NHS provided files in their relevant sub folders without change of name

Place the OPCSChapters.txt file in the same directory. This contains the alhabetical chapter names and terms

Place the ICD1- NHS Trud provided ICD10-5-0  files in the relevant subfolders without change of name

Place the unified pathology list folders as sub folders without chanfge of name

N.B. The opcs 4 and ICD10 complex maps are part of the relevant RF2 Snomed release.

## Imports requiring table look ups
Obtain and restore (e.g. via table SQL) from the Vision look up and TPP look up tables which represent their (partial) supplier maps
between Read 2 or CTV3 and Snomed

vision_read2_to_snomed_map

vision_read2_to_snomed_map

tpp_ctv3_lookup_2

tpp_ctv3_to_snomed

# Importing data

Note this will take a number of hours.

Run the ImportApp application with the following arguments:

1. Root folder

2. "all"   // to imply all types to be imported

Environment variables for establishing DB connection

Also note that the importers may also be run independently but should be done in strict order which is:

a) DiscoveryCoreImporter

b) SnomedImporter

c) R2EMISVisionImporter

d) CTV3TPPImporter,OPCSImporter, ICD1OImporter,Discovery maps,The UTL value set- may be run in any order after that
