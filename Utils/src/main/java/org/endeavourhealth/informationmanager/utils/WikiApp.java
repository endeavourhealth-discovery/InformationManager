package org.endeavourhealth.informationmanager.utils;

import org.endeavourhealth.informationmanager.transforms.authored.WikiGenerator;

import java.io.FileWriter;

public class WikiApp {
	public static void main(String[] args) throws Exception {
		WikiGenerator generator= new WikiGenerator();
		String wiki= generator.generateDocs(args[1]);
		try (FileWriter wr = new FileWriter(args[0])){
			wr.write(wiki);
		}
	}
}
