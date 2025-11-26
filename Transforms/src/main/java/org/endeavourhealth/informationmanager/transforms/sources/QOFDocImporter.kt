package org.endeavourhealth.informationmanager.transforms.sources

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.endeavourhealth.imapi.filer.TTFilerException
import org.endeavourhealth.imapi.filer.TTFilerFactory
import org.endeavourhealth.imapi.logic.importers.QOFImportEngine.processFile
import org.endeavourhealth.imapi.model.imq.*
import org.endeavourhealth.imapi.model.qof.*
import org.endeavourhealth.imapi.model.tripletree.TTDocument
import org.endeavourhealth.imapi.model.tripletree.TTEntity
import org.endeavourhealth.imapi.vocabulary.Graph
import org.endeavourhealth.informationmanager.transforms.models.ImportException
import org.endeavourhealth.informationmanager.transforms.models.TTImport
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.attribute.BasicFileAttributes
import java.util.*
import java.util.function.Consumer

class QOFDocImporter : TTImport {
  private val om = ObjectMapper()

  @Throws(ImportException::class)
  override fun importData(config: TTImportConfig) {
    // TTDocument document = new TTDocument();
    try {
      TTFilerFactory.getDocumentFiler(Graph.IM).use { filer ->
        val specs = findSpecs(config.folder)
        LOG.info("Found {} QOF specification documents", specs.size)
        for (spec in specs) {
          LOG.debug("Importing {}", spec.fileName)
          val qofDoc = processFile(spec.toFile())
          val imDoc = convertQofDocToIMQ(qofDoc)
          if (imDoc != null) {
            LOG.info("Filing...")
            // filer.fileDocument(document);
          }
        }
      }
    } catch (e: TTFilerException) {
      LOG.error("Unable to file QOF document/entities")
      throw ImportException("Error attempting to find QOF specification documents", e)
      //    } catch (IOException e) {
//      throw new ImportException("Error processing QOF files", e);
    }
  }

  private fun convertQofDocToIMQ(qofDoc: QOFDocument): TTDocument {
    val doc = TTDocument()

    qofDoc.selections
      .forEach(Consumer { s: Selection -> doc.addEntity(convertRules(s.name, s.rules)) })
    qofDoc.registers
      .forEach(Consumer { r: Register -> doc.addEntity(convertRules(r.name, r.rules)) })
    qofDoc.extractionFields.forEach(Consumer { f: ExtractionField -> getWhereFromLogic(f.logic) })
    qofDoc.indicators
      .forEach(Consumer { i: Indicator -> doc.addEntity(convertRules(i.name, i.denominator)) })
    qofDoc.indicators
      .forEach(Consumer { i: Indicator -> doc.addEntity(convertRules(i.name, i.numerator)) })

    return doc
  }

  private fun convertRules(name: String?, rules: MutableList<Rule?>): TTEntity {
    val q = Query()
    q.name = name

    rules.forEach(Consumer { r: Rule? -> q.addRule(getMatchFromRule(r!!)) })

    val e = TTEntity()
    e.name = name

    try {
      val json = om.writerWithDefaultPrettyPrinter().writeValueAsString(q)
      LOG.info("JSON: \n{}", json)
    } catch (ex: JsonProcessingException) {
      throw RuntimeException(ex)
    }

    // e.set(IM.DEFINITION, q)
    return e
  }

  private fun getMatchFromRule(r: Rule): Match {
    LOG.info("Processing rule: [{}]", r.logicText)
    LOG.info("Logic: \n{}", if (r.logic != null) r.logic.toFormattedString() else "null")
    val m = Match()
    m.name = r.description
    m.description = r.logicText
    m.ifTrue = getRuleAction(r.getIfTrue().uppercase(Locale.getDefault()))
    m.ifFalse = getRuleAction(r.getIfFalse().uppercase(Locale.getDefault()))
    m.where = getWhereFromLogic(r.logic)
    m.ruleNumber = r.order

    return m
  }


  private fun getRuleAction(action: String): RuleAction {
    return when (action) {
      "SELECT" -> RuleAction.SELECT
      "REJECT" -> RuleAction.REJECT
      "NEXT RULE" -> RuleAction.NEXT
      else -> throw RuntimeException("Unknown rule action: $action")
    }
  }

  private fun getWhereFromLogic(logic: QOFExpressionNode): Where {
    val w = Where()

    when (logic.operator) {
      "AND" -> {
        LOG.debug("AND")
        for (child in logic.children) {
          w.addAnd(getWhereFromLogic(child))
        }
      }

      "OR" -> {
        LOG.debug("OR")
        for (child in logic.children) {
          w.addOr(getWhereFromLogic(child))
        }
      }

      else -> {
        getWhereFromExpression(logic, w)
      }
    }

    return w
  }

  @Throws(TTFilerException::class)
  override fun validateFiles(inFolder: String) {
    findSpecs(inFolder)
  }

  @Throws(TTFilerException::class)
  private fun findSpecs(inFolder: String): MutableList<Path> {
    try {
      Files.find(
        Paths.get(inFolder), 16,
        { file: Path?, _: BasicFileAttributes? ->
          file.toString()
            .replace("/", "\\")
            .matches(".*\\\\QOF\\\\Specs\\\\.*\\.docx".toRegex())
        }).use { stream ->
        val specs: MutableList<Path> = stream.toList()
        if (specs.isEmpty()) {
          LOG.error("Unable to find any QOF specification documents (*.dox)")
          throw TTFilerException("No QOF specification documents found in $inFolder - check folder name and file extension.")
        }
        return specs
      }
    } catch (e: IOException) {
      LOG.error("Error attempting to find any QOF specification documents (*.dox)")
      throw TTFilerException(
        "Error attempting to find QOF specification documents in $inFolder - check folder name and file extension.",
        e
      )
    }
  }

  @Throws(Exception::class)
  override fun close() {
  }

  companion object {
    private val LOG: Logger = LoggerFactory.getLogger(QOFDocImporter::class.java)

    private fun getWhereFromExpression(logic: QOFExpressionNode, w: Where) {
      val condition = logic.condition

      w.nodeRef = condition.leftOperand

      var op = condition.comparator

      if ("Unconditional" != condition.leftOperand) {
        if (op == null || op.isEmpty()) {
          LOG.error("NO COMPARATOR FOUND")
          throw RuntimeException("No COMPARATOR found in expression: ")
        }

        if ("!=" == op) {
          w.isNot = true
        }

        if (mutableListOf<String?>("!=", " on ", " of ", " at ").contains(op)) op = "="

        val opEnum = Operator.get(op)

        if (opEnum.isEmpty) {
          LOG.error("NO OPERATOR FOUND FOR [{}]", op)
          throw RuntimeException("No operator found for: $op")
        } else {
          w.setOperator(opEnum.get()).value = condition.rightOperand
        }
      }
    }
  }
}
