package org.endeavourhealth.informationmanager.utils.opensearch;

import java.io.IOException;

public class Main {
    public static void main(String[] argv) throws IOException, InterruptedException {
        boolean update=false;
        String cache=null;
        if (argv.length>0){
            for (String arg:argv)
                if (arg.toLowerCase().equals("update"))
                    update= true;
                else if (arg.toLowerCase().startsWith("cache=")){
                   cache= arg.split("=")[1];
                }
        }
        new OpenSearchSender().setCache(cache).execute(update);
    }
}
