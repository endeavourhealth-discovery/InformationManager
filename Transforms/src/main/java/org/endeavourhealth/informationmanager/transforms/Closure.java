package org.endeavourhealth.informationmanager.transforms;

import org.endeavourhealth.informationmanager.ClosureGenerator;

public class Closure {
   public static void main(String[] args) throws Exception {
      if (args.length == 0 || args.length > 2) {
         System.err.println("Incorrect parameters:");
         System.err.println("<output folder> [secure]");
         System.exit(-1);
      }

      boolean secure = (args.length == 2 && "secure".equalsIgnoreCase(args[1]));

      ClosureGenerator builder = new ClosureGenerator();
      builder.generateClosure(args[0], secure);
   }
}
