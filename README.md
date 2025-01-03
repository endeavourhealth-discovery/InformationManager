# Information Manager

![Version](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/version.svg)
![Build Status](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/build.svg)
![Unit Tests](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/unit-test.svg)

## Transform

The purpose of this information Manager sub project is to load data from external files into the IM2 graph database.

See module specific instructions

To generate a new database from scratch follow these steps:
- Clone ImportData  and copy into a location for use. e.g. C:\ImportData Trud updater generates large files e.g. Snomed so useful to use another folder.
- 
- Run TRUDUpdater (org.endeavourhealth.informationmanager.trud)
- This requires the following arguments
  - API authorisation key
  - The ImportData folder wher the TRUD data will be held  e.g. :\ImportData\TRUD\
- 
- Run Preload (org.endeavourhealth.informationmanager.transforms.preload)
  - Preload should be run with the following arguments
      source // the local folder for Import data    e.g. source=C:\ImportData
      graphs // the name of the file in the import data that holds the list of graphs to file e.g. graphs=Endeavour.json
      preload  // the folder location of the rdf bulk import program   e.g. preload=C:\Users\Fred\AppData\Local\GraphDBDesktop\app\bin
      temp  // the folder where the quads  and look ups are held. Note that this is cleared so use an empty folder   e.g. temp=c:\temp
      cmd // the command for starting graph db  e.g. cmd=C:\Users\david\AppData\Local\GraphDBDesktop\GraphDB" "Desktop.exe
- 
- (Optional) Run OpenSearchSender (package org.endeavourhealth.informationmanager.utils.opensearch) as thjis updates the terms for text searching in elastic/
