Feature: Validate files by importer type

  Scenario: Valid import type with valid folder
    Given I have the import type "http://endhealth.info/im#SingleFileImporter"
    And the folder path is "/mock/folder"
    And a mock importer is set up
    When I call validateByType
    Then the correct importer should be used
    And validateFiles should be called with "/mock/folder"
    And the method should return successfully

  Scenario: Unknown import type
    Given I have the import type "unknown-type"
    And the folder path is "/mock/folder"
    And a mock importer is set up for unknown type
    When I call validateByType
    Then an ImportException should be thrown with message "Unrecognised import type [unknown-type]"

  Scenario: Importer throws an exception during validation
    Given I have the import type "http://endhealth.info/im#SingleFileImporter"
    And the folder path is "/mock/folder"
    And the importer throws an exception during validateFiles
    When I call validateByType
    Then an Exception should be thrown
