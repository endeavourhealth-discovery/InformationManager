package org.endeavourhealth.qofextractor

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.ObjectMapper
import org.endeavourhealth.imapi.logic.importers.QOFImportEngine.processFile
import org.endeavourhealth.imapi.model.qof.QOFDocument
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileWriter
import java.io.IOException
import java.util.*
import kotlin.system.exitProcess

private val LOG: Logger = LoggerFactory.getLogger("QOFExtractor")

@Throws(IOException::class)
fun main(args: Array<String>) {
  if (args.size != 1) {
    LOG.error("You must provide a path to the folder containing the QOF documents!")
    exitProcess(1)
  }

  val files = File(args[0]).listFiles { _: File?, name: String ->
    name.lowercase(Locale.getDefault()).endsWith(".docx")
  }

  if (files == null || files.size == 0) {
    LOG.error("No .docx files found in the specified folder!")
    exitProcess(1)
  }

  for (file in files) {
    val qofDoc = processFile(file)

    val filePath = file.getParentFile().absolutePath
    val fileName: String = file.getName().replace(".docx", ".json")
    generateOutput(qofDoc, "$filePath/$fileName")
  }
}

@Throws(IOException::class)
private fun generateOutput(qofDoc: QOFDocument?, fileName: String) {
  val mapper = ObjectMapper()
  mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)
  mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
  val file = FileWriter(fileName)
  mapper.writerWithDefaultPrettyPrinter().writeValue(file, qofDoc)
  file.close()
}
