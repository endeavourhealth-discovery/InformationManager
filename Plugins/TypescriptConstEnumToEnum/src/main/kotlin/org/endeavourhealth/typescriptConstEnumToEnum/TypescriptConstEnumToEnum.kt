package org.endeavourhealth.typescriptConstEnumToEnum

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.GradleException
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.StandardOpenOption

class TypescriptConstEnumToEnum: Plugin<Project> {
  override fun apply(project: Project) {
    val extension = project.extensions.create("typescriptConstEnumToEnum", TypescriptConstEnumToEnumExtension::class.java)

    project.tasks.register("typescriptConstEnumToEnum") {
      group = "org.endeavourhealth.plugins"
      description = "Changes typescript const enums to standard enums"

      dependsOn("generateTypeScript")

      doLast {
        val configuredPath = extension.filePath.orNull
        if (configuredPath.isNullOrBlank()) {
          throw GradleException("typescriptConstEnumToEnum.filePath must be set")
        }

        val file = File(project.rootDir, configuredPath)

        if (!file.exists()) {
          throw GradleException("Autogen file not found at: ${file.absolutePath}")
        }

        val content = file.readText(Charsets.UTF_8)
        val replaced = content.replace(Regex("\\bconst enum\\b"), "enum")

        Files.write(
          file.toPath(),
          replaced.toByteArray(StandardCharsets.UTF_8),
          StandardOpenOption.TRUNCATE_EXISTING
        )
      }
    }
  }
}