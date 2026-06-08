package org.endeavourhealth.staticConstGenerator;

import org.gradle.api.provider.Property;

import java.util.List;

public interface VocabGeneratorExtension {
  Property<String> getInputJson();

  Property<String> getPackagePath();

  Property<String> getJavaOutputFolder();

  Property<String> getTypeScriptOutputFolder();
}
