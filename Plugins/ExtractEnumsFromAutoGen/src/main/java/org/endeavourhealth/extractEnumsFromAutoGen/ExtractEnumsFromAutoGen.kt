package org.endeavourhealth.extractEnumsFromAutoGen

import org.gradle.api.*
import java.io.File

class ExtractEnumsFromAutoGen : Plugin<Project> {
  override fun apply(project: Project) {
    project.tasks.register("extractEnumsFromAutoGen") {
      group = "other"
      description = "Changes TypeScript generated const enums to standard enums and adds imports"

      val extension = project.extensions.create("extractEnumsFromAutoGen", ExtractEnumsFromAutoGenExtension::class.java)


      it.doLast {
        val inputFile = File(project.rootDir, extension.inputFile)
        val outputFile = File(project.rootDir, extension.outputFile)

        if (!inputFile.exists()) {
          throw GradleException("AutoGen.ts not found: $inputFile")
        }

        val content = inputFile.readText()

        val enumRegex = Regex(
          pattern = """(?ms)^\s*export\s+enum\s+(\w+)\s*\{.*?^\s*\}"""
        )

        val matches = enumRegex.findAll(content).toList()

        if (matches.isEmpty()) {
          println("No enums found.")
          return@doLast
        }

        val enumNames = matches.map { it.groups[1]!!.value }

        outputFile.writeText(
          "/* Auto-extracted enums */\n\n" +
            matches.joinToString("\n") { it.value }
        )

        var updatedContent = content.replace(enumRegex) { "" }

        val importLine =
          "import { ${enumNames.joinToString(", ")} } from '../enums/AutoGen';\n\n"

        updatedContent = importLine + updatedContent.trimStart()

        inputFile.writeText(updatedContent)

        println("Extracted ${matches.size} enums to ${outputFile.name}")
      }
    }
  }
}