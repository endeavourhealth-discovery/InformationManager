package org.endeavourhealth.typescriptConstEnumToEnum

import org.gradle.api.provider.Property

abstract class TypescriptConstEnumToEnumExtension {
  // Path to the generated TypeScript file to modify, relative to the consumer project's rootDir
  abstract val filePath: Property<String>
}