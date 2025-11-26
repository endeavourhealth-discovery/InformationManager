package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.importers.QOFImportEngine;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.qof.QOFCondition;
import org.endeavourhealth.imapi.model.qof.QOFDocument;
import org.endeavourhealth.imapi.model.qof.QOFExpressionNode;
import org.endeavourhealth.imapi.model.qof.Rule;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.vocabulary.Graph;
import org.endeavourhealth.informationmanager.transforms.models.ImportException;
import org.endeavourhealth.informationmanager.transforms.models.TTImport;
import org.endeavourhealth.informationmanager.transforms.models.TTImportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

public class QOFDocImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(QOFDocImporter.class);

  private final ObjectMapper om = new ObjectMapper();

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    // TTDocument document = new TTDocument();
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
      List<Path> specs = findSpecs(config.getFolder());

      LOG.info("Found {} QOF specification documents", specs.size());
      for (Path spec : specs) {
        LOG.debug("Importing {}", spec.getFileName());
        QOFDocument qofDoc = QOFImportEngine.INSTANCE.processFile(spec.toFile());
        TTDocument imDoc = convertQofDocToIMQ(qofDoc);
        if (imDoc != null) {
          LOG.info("Filing...");
          // filer.fileDocument(document);
        }
      }
//    } catch (QueryException e) {
//      throw new RuntimeException(e);
    } catch (TTFilerException e) {
      LOG.error("Unable to file QOF document/entities");
      throw new ImportException("Error attempting to find QOF specification documents", e);
//    } catch (IOException e) {
//      throw new ImportException("Error processing QOF files", e);
    }
  }

  private TTDocument convertQofDocToIMQ(QOFDocument qofDoc) {
    TTDocument doc = new TTDocument();

    qofDoc.getSelections().forEach(s -> doc.addEntity(convertRules(s.getName(), s.getRules())));
    qofDoc.getRegisters().forEach(r -> doc.addEntity(convertRules(r.getName(), r.getRules())));
    qofDoc.getExtractionFields().forEach(f -> getWhereFromLogic(f.getLogic()));
    qofDoc.getIndicators().forEach(i -> doc.addEntity(convertRules(i.getName(), i.getDenominator())));
    qofDoc.getIndicators().forEach(i -> doc.addEntity(convertRules(i.getName(), i.getNumerator())));

    return doc;
  }

  private TTEntity convertRules(String name, List<Rule> rules) {
    Query q = new Query();
    q.setName(name);

    rules.forEach(r -> q.addRule(getMatchFromRule(r)));

    TTEntity e = new TTEntity();
    e.setName(name);

    try {
      String json = om.writerWithDefaultPrettyPrinter().writeValueAsString(q);
      LOG.info("JSON: \n{}", json);
    } catch (JsonProcessingException ex) {
      throw new RuntimeException(ex);
    }

    // e.set(IM.DEFINITION, q)

    return e;
  }

  private Match getMatchFromRule(Rule r) {
    LOG.info("Processing rule: [{}]", r.getLogicText());
    LOG.info("Logic: \n{}", r.getLogic() != null ? r.getLogic().toFormattedString() : "null");
    Match m = new Match();
    m.setName(r.getDescription());
    m.setDescription(r.getLogicText());
    m.setIfTrue(getRuleAction(r.getIfTrue().toUpperCase()));
    m.setIfFalse(getRuleAction(r.getIfFalse().toUpperCase()));
    m.setWhere(getWhereFromLogic(r.getLogic()));
    m.setRuleNumber(r.getOrder());
    
    return m;
  }


  private RuleAction getRuleAction(String action) {
    return switch (action) {
      case "SELECT" -> RuleAction.SELECT;
      case "REJECT" -> RuleAction.REJECT;
      case "NEXT RULE" -> RuleAction.NEXT;
      default -> throw new RuntimeException("Unknown rule action: " + action);
    };
  }

  private Where getWhereFromLogic(QOFExpressionNode logic) {
    Where w = new Where();

    if ("AND".equals(logic.getOperator())) {
      LOG.debug("AND");
      for (QOFExpressionNode child : logic.getChildren()) {
        w.addAnd(getWhereFromLogic(child));
      }
    } else if ("OR".equals(logic.getOperator())) {
      LOG.debug("OR");
      for (QOFExpressionNode child : logic.getChildren()) {
        w.addOr(getWhereFromLogic(child));
      }
    } else {
      getWhereFromExpression(logic, w);
    }

    return w;
  }

  private static void getWhereFromExpression(QOFExpressionNode logic, Where w) {
    QOFCondition condition = logic.getCondition();

    w.setNodeRef(condition.getLeftOperand());

    String op = condition.getComparator();

    if (!"Unconditional".equals(condition.getLeftOperand())) {
      if (op == null || op.isEmpty()) {
        LOG.error("NO COMPARATOR FOUND");
        throw new RuntimeException("No COMPARATOR found in expression: ");
      }

      if ("!=".equals(op)) {
        w.setNot(true);
      }

      if (List.of("!=", " on ", " of ", " at ").contains(op))
        op = "=";

      Optional<Operator> opEnum = Operator.get(op);

      if (opEnum.isEmpty()) {
        LOG.error("NO OPERATOR FOUND FOR [{}]", op);
        throw new RuntimeException("No operator found for: " + op);
      } else {
        w.setOperator(opEnum.get())
          .setValue(condition.getRightOperand());
      }
    }
  }

  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    findSpecs(inFolder);
  }

  private List<Path> findSpecs(String inFolder) throws TTFilerException {
    try (Stream<Path> stream = Files.find(Paths.get(inFolder), 16,
      (file, attr) -> file.toString()
        .replace("/", "\\")
        .matches(".*\\\\QOF\\\\Specs\\\\.*\\.docx"))) {
      List<Path> specs = stream.toList();

      if (specs.isEmpty()) {
        LOG.error("Unable to find any QOF specification documents (*.dox)");
        throw new TTFilerException("No QOF specification documents found in " + inFolder + " - check folder name and file extension.");
      }

      return specs;
    } catch (IOException e) {
      LOG.error("Error attempting to find any QOF specification documents (*.dox)");
      throw new TTFilerException("Error attempting to find QOF specification documents in " + inFolder + " - check folder name and file extension.");
    }
  }

  @Override
  public void close() throws Exception {

  }
}
