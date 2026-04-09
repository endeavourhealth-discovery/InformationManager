package org.endeavourhealth.extractEnumsFromAutoGen

import org.gradle.api.*
import java.io.File

class ExtractEnumsFromAutoGen : Plugin<Project> {
  override fun apply(project: Project) {
    val extension = project.extensions.create("extractEnumsFromAutoGen", ExtractEnumsFromAutoGenExtension::class.java)

    project.tasks.register("extractEnumsFromAutoGen") {
      group = "org.endeavourhealth.plugins"
      description = "Extracts enums from generated TypeScript into a separate file and adds imports"

      // If the project has a TS generation step, ensure we run after it
      dependsOn("typescriptConstEnumToEnum")

      doLast {
        val inPath = extension.inputFile.orNull
        val outPath = extension.outputFile.orNull

        if (inPath.isNullOrBlank()) {
          throw GradleException("extractEnumsFromAutoGen.inputFile must be set")
        }
        if (outPath.isNullOrBlank()) {
          throw GradleException("extractEnumsFromAutoGen.outputFile must be set")
        }

        val inputFile = File(project.rootDir, inPath)
        val outputFile = File(project.rootDir, outPath)

        if (!inputFile.exists()) {
          throw GradleException("AutoGen.ts not found: $inputFile")
        }

        val content = inputFile.readText(Charsets.UTF_8)

        val enumRegex = Regex(
          pattern = """(?ms)^\s*export\s+enum\s+(\w+)\s*\{.*?^\s*\}"""
        )

        val matches = enumRegex.findAll(content).toList()

        if (matches.isEmpty()) {
          println("No enums found.")
          return@doLast
        }

        val enumNames = matches.map { it.groups[1]!!.value }

        outputFile.parentFile?.mkdirs()
        outputFile.writeText(
          "/* Auto-extracted enums */\n\n" +
            matches.joinToString("\n") { it.value }
        , Charsets.UTF_8)

        var updatedContent = content.replace(enumRegex) { "" }

        val importLine =
          "import { ${enumNames.joinToString(", ")} } from '../enums/AutoGen';\n\n"

        updatedContent = importLine + updatedContent.trimStart()

        inputFile.writeText(updatedContent, Charsets.UTF_8)

        println("Extracted ${matches.size} enums to ${outputFile.name}")
      }
    }
  }
}