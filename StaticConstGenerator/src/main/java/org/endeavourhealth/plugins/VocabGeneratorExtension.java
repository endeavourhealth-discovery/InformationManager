package org.endeavourhealth.plugins;

import org.gradle.api.provider.Property;

import java.util.List;

public interface VocabGeneratorExtension {
    Property<String> getInputJson();
    Property<String> getJavaOutputFolder();
    Property<String> getTypeScriptOutputFolder();
}
