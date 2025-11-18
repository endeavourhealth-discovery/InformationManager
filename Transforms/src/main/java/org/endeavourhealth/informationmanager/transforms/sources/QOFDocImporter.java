package org.endeavourhealth.informationmanager.transforms.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.endeavourhealth.imapi.filer.TTDocumentFiler;
import org.endeavourhealth.imapi.filer.TTFilerException;
import org.endeavourhealth.imapi.filer.TTFilerFactory;
import org.endeavourhealth.imapi.logic.importers.QOFImportEngine;
import org.endeavourhealth.imapi.model.imq.*;
import org.endeavourhealth.imapi.model.qof.QOFDocument;
import org.endeavourhealth.imapi.model.qof.QOFExpressionNode;
import org.endeavourhealth.imapi.model.qof.Rule;
import org.endeavourhealth.imapi.model.tripletree.TTDocument;
import org.endeavourhealth.imapi.model.tripletree.TTEntity;
import org.endeavourhealth.imapi.parser.qofdoc.QOFDocParser;
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
import java.util.stream.Stream;

public class QOFDocImporter implements TTImport {
  private static final Logger LOG = LoggerFactory.getLogger(QOFDocImporter.class);

  private static final String specPattern = ".*\\\\QOF\\\\Specs\\\\.*\\.docx";

  @Override
  public void importData(TTImportConfig config) throws ImportException {
    TTDocument document = new TTDocument();
    try (TTDocumentFiler filer = TTFilerFactory.getDocumentFiler(Graph.IM)) {
      ObjectMapper om = new ObjectMapper();
      List<Path> specs = findSpecs(config.getFolder());

      LOG.info("Found {} QOF specification documents", specs.size());
      for (Path spec : specs) {
        LOG.debug("Importing {}", spec.getFileName());
        QOFDocument qofDoc = QOFImportEngine.processFile(spec.toFile());
        TTDocument imDoc = convertQofDocToIMQ(qofDoc);
        if (imDoc != null) {
          LOG.info("Filing...");
          // filer.fileDocument(document);
        }
      }
//    } catch (QueryException e) {
//      throw new RuntimeException(e);
    } catch (TTFilerException e) {
      LOG.error("Unable to find any QOF specification documents (*.dox)");
      throw new ImportException("Error attempting to find QOF specification documents", e);
    } catch (IOException e) {
      throw new ImportException("Error processing QOF files", e);
    }
  }

  private TTDocument convertQofDocToIMQ(QOFDocument qofDoc) {
    TTDocument doc = new TTDocument();

    qofDoc.getSelections().forEach(s -> doc.addEntity(convertRules(s.getName(), s.getRules())));
    qofDoc.getRegisters().forEach(r -> doc.addEntity(convertRules(r.getName(), r.getRules())));
    qofDoc.getExtractionFields().forEach(f -> getWhereFromLogic(f.getLogic()));
    qofDoc.getIndicators().forEach(i -> doc.addEntity(convertRules(i.getName(), i.getDenominator())));
    qofDoc.getIndicators().forEach(i -> doc.addEntity(convertRules(i.getName(), i.getNumerator())));

    return null;
  }

  private TTEntity convertRules(String name, List<Rule> rules) {
    Query q = new Query();
    q.setName(name);

    rules.forEach(r -> q.addRule(getMatchFromRule(r)));

    TTEntity e = new TTEntity();
    e.setName(name);

    return e;
  }

  private Match getMatchFromRule(Rule r) {
    LOG.debug("Processing rule {}", r.getLogic());
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
    switch (action) {
      case "SELECT": return RuleAction.SELECT;
      case "REJECT": return RuleAction.REJECT;
      case "NEXT RULE": return RuleAction.NEXT;
      default: throw new RuntimeException("Unknown rule action: " + action);
    }
  }

  private Where getWhereFromLogic(QOFExpressionNode logic) {
    LOG.info("Converting logic to Where:\n{}", logic.toFormattedString());
    return null;
  }

  private Where getWhereFromExpression(QOFDocParser.ExpressionContext expression) {
    //if (expression.)
    return null;
  }


  @Override
  public void validateFiles(String inFolder) throws TTFilerException {
    findSpecs(inFolder);
  }

  private List<Path> findSpecs(String inFolder) throws TTFilerException {
    try (Stream<Path> stream = Files.find(Paths.get(inFolder), 16,
      (file, attr) -> file.toString()
        .replace("/", "\\")
        .matches(specPattern))) {
      List<Path> specs = stream.toList();

      if (specs.isEmpty()) {
        LOG.error("Unable to find any QOF specification documents (*.dox)");
        throw new TTFilerException("No QOF specification documents found in " + inFolder + " - check folder name and file extension.  Expected files with pattern: " + specPattern + "  (note the backslashes)  ");
      }

      return specs;
    } catch (IOException e) {
      LOG.error("Error attempting to find any QOF specification documents (*.dox)");
      throw new TTFilerException("Error attempting to find QOF specification documents in " + inFolder + " - check folder name and file extension.  Expected files with pattern: " + specPattern + "  (note the backslashes)  ");
    }
  }

  @Override
  public void close() throws Exception {

  }
}
