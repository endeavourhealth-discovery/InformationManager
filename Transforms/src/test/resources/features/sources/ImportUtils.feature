Feature: Validate files under a directory

  Scenario: All files exist
    Given the base path is "/mock/path"
    And the following files exist:
      | fileName  |
      | file1.txt |
      | file2.txt |
    When I validate the files
    Then the validation should succeed

  Scenario: Some files do not exist
    Given the base path is "/mock/path"
    And the following files exist:
      | fileName  |
      | file1.txt |
    And I try to validate the files:
      | fileName    |
      | file1.txt   |
      | missing.txt |
    When I validate the files
    Then the validation should fail
    And the error log should contain "missing.txt"

  Scenario: No files provided
    Given the base path is "/mock/path"
    When I validate no files
    Then the validation should succeed
