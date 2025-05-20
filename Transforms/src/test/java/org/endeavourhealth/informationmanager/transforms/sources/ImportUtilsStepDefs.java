package org.endeavourhealth.informationmanager.transforms.sources;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.endeavourhealth.imapi.config.SecurityConfig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ImportUtilsStepDefs {
  private String basePath;
  private List<String> filesToValidate = new ArrayList<>();
  private Set<String> existingFiles = new HashSet<>();
  private boolean validationFailed = false;
  private List<String> errorLog = new ArrayList<>();

  @Given("the base path is {string}")
  public void the_base_path_is(String path) {
    this.basePath = path;
  }

  @Given("the following files exist:")
  public void the_following_files_exist(io.cucumber.datatable.DataTable dataTable) {
    existingFiles.addAll(dataTable.asMaps().stream()
      .map(row -> row.get("fileName"))
      .toList());
  }

  @And("I try to validate the files:")
  public void i_try_to_validate_the_files(io.cucumber.datatable.DataTable dataTable) {
    filesToValidate = dataTable.asMaps().stream()
      .map(row -> row.get("fileName"))
      .toList();
  }

  @When("I validate the files")
  public void i_validate_the_files() {
    try {
      validateFilesMock(basePath, filesToValidate.toArray(new String[0]));
    } catch (RuntimeException e) {
      validationFailed = true;
    }
  }

  @When("I validate no files")
  public void i_validate_no_files() {
    try {
      validateFilesMock(basePath); // empty varargs
    } catch (RuntimeException e) {
      validationFailed = true;
    }
  }

  @Then("the validation should succeed")
  public void the_validation_should_succeed() {
    assertFalse(validationFailed, "Expected validation to succeed");
  }

  @Then("the validation should fail")
  public void the_validation_should_fail() {
    assertTrue(validationFailed, "Expected validation to fail");
  }

  @Then("the error log should contain {string}")
  public void the_error_log_should_contain(String expected) {
    assertTrue(errorLog.contains(expected));
  }

  // Mocked version of validateFiles
  private void validateFilesMock(String path, String[]... values) {
    boolean exit = false;
    for (String[] fileArray : values) {
      for (String file : fileArray) {
        try {
          if (!existingFiles.contains(file)) {
            throw new IOException("File not found: " + file);
          }
        } catch (IOException e) {
          errorLog.add(file);
          exit = true;
        }
      }
    }
    if (exit) throw new RuntimeException("Validation failed");
  }
}
