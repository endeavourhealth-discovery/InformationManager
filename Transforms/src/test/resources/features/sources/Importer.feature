Feature: Validate files by importer type

  Scenario: Valid import type with valid folder
    Given I have the import type "singlefile"
    And the folder path is "/mock/folder"
    And a mock importer is set up
    When I call validateByType
    Then the correct importer should be used
    And validateFiles should be called with "/mock/folder"
    And the method should return successfully
