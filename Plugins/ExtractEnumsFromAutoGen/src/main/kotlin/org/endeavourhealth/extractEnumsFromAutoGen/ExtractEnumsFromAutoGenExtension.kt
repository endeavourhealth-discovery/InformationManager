package org.endeavourhealth.extractEnumsFromAutoGen

import org.gradle.api.provider.Property;

abstract class ExtractEnumsFromAutoGenExtension {
  abstract val inputFile: Property<String>;
  abstract val outputFile: Property<String>;
}