package org.endeavourhealth.informationmanager.scratch.opensearch;

import java.io.IOException;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {
        boolean update=false;
        if (argv.length>0){
            for (String arg:argv)
                if (arg.toLowerCase().equals("update"))
                    update= true;
        }
        new OpenSearchSender().execute(update);
    }
}
