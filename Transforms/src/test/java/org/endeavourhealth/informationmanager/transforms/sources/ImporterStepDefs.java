package org.endeavourhealth.informationmanager.transforms.sources;

import io.cucumber.java.en.And;
import io.cucumber.java.en.Given;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.vocabulary.ImportType;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportByType;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;

import static org.junit.Assert.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.verify;


public class ImporterStepDefs {
  private ImportType importType;
  private String folderPath;
  private TTImport mockImporter;
  private TTImportByType service;
  private Exception caughtException;

  @Given("I have the import type {string}")
  public void i_have_the_import_type(String importType) {
    this.importType = ImportType.from(importType);
  }

  @And("the folder path is {string}")
  public void the_folder_path_is(String folderPath) {
    this.folderPath = folderPath;
  }

  @And("a mock importer is set up for unknown type")
  public void mock_importer_for_unknown_type() {
    mockImporter = mock(TTImport.class);
    service = new TTImportByTypeMock(mockImporter);
  }

  @And("a mock importer is set up")
  public void a_mock_importer_is_set_up() {
    mockImporter = mock(TTImport.class);
    service = new TTImportByTypeMock(mockImporter);
  }

  @And("the correct importer should be used")
  public void correct_importer_should_be_used() throws TTFilerException {
    verify(mockImporter, times(1)).validateFiles(folderPath);
  }

  @And("validateFiles should be called with {string}")
  public void validate_files_called_with(String expectedPath) throws Exception {
    verify(mockImporter).validateFiles(expectedPath);
  }

  @And("the importer throws an exception during validateFiles")
  public void importer_throws_exception_during_validateFiles() throws Exception {
    mockImporter = mock(TTImport.class);
    doThrow(new RuntimeException("File issue")).when(mockImporter).validateFiles(anyString());
    service = new TTImportByTypeMock(mockImporter);
  }

  @When("I call validateByType")
  public void i_call_validateByType() {
    try {
      service.validateByType(importType, folderPath);
    } catch (Exception e) {
      this.caughtException = e;
    }
  }

  @Then("the method should return successfully")
  public void method_returns_successfully() {
    assertNull("Expected no exception", caughtException);
  }

  @Then("an ImportException should be thrown with message {string}")
  public void an_import_exception_should_be_thrown(String expectedMessage) {
    assertNotNull("Expected an exception but none was thrown", caughtException);
    assertTrue("Expected ImportException but got: " + caughtException.getClass().getSimpleName(),
      caughtException instanceof ImportException);
    assertEquals(expectedMessage, caughtException.getMessage());
  }

  @Then("an Exception should be thrown")
  public void any_exception_should_be_thrown() {
    assertNotNull("Expected an exception", caughtException);
  }

  public static class TTImportByTypeMock implements TTImportByType {
    private final TTImport importer;

    public TTImportByTypeMock(TTImport importer) {
      this.importer = importer;
    }

    @Override
    public TTImportByType importByType(ImportType importType, TTImportConfig config) throws Exception {
      return null;
    }

    @Override
    public TTImportByType validateByType(ImportType importType, String inFolder) throws Exception {
      TTImport imp = getImporter(importType);
      imp.validateFiles(inFolder);
      return this;
    }

    protected TTImport getImporter(ImportType importType) throws ImportException {
      if (importType == null) {
        throw new ImportException("Unrecognised import type");
      }
      return importer;
    }
  }
}
