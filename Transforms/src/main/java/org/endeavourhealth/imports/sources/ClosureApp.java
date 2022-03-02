package    org.endeavourhealth.imports.sources;

import org.endeavourhealth.imapi.filer.TCGenerator;
import org.endeavourhealth.imapi.filer.TTFilerFactory;

public class ClosureApp {
   public static void main(String[] args) throws Exception {
      if (args.length == 0 || args.length > 2) {
         System.err.println("Incorrect parameters:");
         System.err.println("<output folder> [secure]");
         System.exit(-1);
      }

      boolean secure = (args.length == 2 && "secure".equalsIgnoreCase(args[1]));

      TCGenerator builder = TTFilerFactory.getClosureGenerator();
      builder.generateClosure(args[0], secure);
   }
}
