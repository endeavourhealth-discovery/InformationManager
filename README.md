# Information Manager

![Version](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/version.svg)
![Build Status](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/build.svg)
![Unit Tests](https://s3.eu-west-2.amazonaws.com/endeavour-codebuild/badges/InformationManager/unit-test.svg)

## Transform

The purpose of this information Manager sub project is to load data from external files into the IM2 graph database.

See module specific instructions

To generate a new database from scratch follow these steps:
- Clone ImportData
- Run TRUDUpdater (org.endeavourhealth.informationmanager.trud)
- Run Preload (org.endeavourhealth.informationmanager.transforms.preload)
- (Optional) Run OpenSearchSender (package org.endeavourhealth.informationmanager.utils.opensearch)