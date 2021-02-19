package org.endeavourhealth.informationmanager.common.transform;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.IntervalSet;

import java.util.List;
import java.util.UnknownFormatConversionException;


public class ECLErrorListener extends BaseErrorListener{
   private String partialToken;
   private List<String> errorMessages;
   private String errorOffset;
   private Recognizer recognizer;
   private org.antlr.v4.runtime.CommonToken badToken;
   private IntervalSet expectedTokens;


@Override
public void syntaxError(Recognizer<?,?> recognizer,
                        Object offendingSymbol, int line, int charPositionInLine,
                        String msg, RecognitionException e) {
   this.recognizer = recognizer;
   if (offendingSymbol instanceof org.antlr.v4.runtime.CommonToken)
      badToken = (org.antlr.v4.runtime.CommonToken) offendingSymbol;
   if (recognizer instanceof Lexer) {
      throw new UnknownFormatConversionException(msg + " line " + line + " offset " + charPositionInLine);
   } else {
      Parser parser = (Parser) recognizer;
      ParserRuleContext ctx = parser.getContext();
      expectedTokens = parser.getExpectedTokens();
      String message = "Expecting " + expectedTokens.toString(parser.getVocabulary());
      throw new UnknownFormatConversionException("ECL Syntax error : " + message + " line " + line
          + " offset " + charPositionInLine);
   }
}
}

