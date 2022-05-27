package org.endeavourhealth.informationmanager.transforms.preload;

import org.endeavourhealth.imapi.filer.rdf4j.TTBulkFiler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class GraphDBRunner implements Runnable {

	private String graphdb;


	public GraphDBRunner (String graphStart){
		this.graphdb= graphStart;
	}
	@Override
	public void run() {
		List<String> cmds = new ArrayList();
		if (System.getProperty("os.name").toLowerCase().contains("windows")) {
			ProcessBuilder processBuilder = new ProcessBuilder();
			processBuilder.command("cmd", "/c", graphdb);
			File dir = new File(TTBulkFiler.getPreload()+"\\");
			processBuilder.directory(dir);
			try {
				Process process=processBuilder.start();
				BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));
				String line;
				while ((line = reader.readLine()) != null) {
					//System.out.println(line);
					Thread.sleep(500);
				}

			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}

			/*
			ProcessBuilder processBuilder = new ProcessBuilder(command);
			//	cmds.add("cmd");
			//cmds.add("/k");

			try {
				Process process = processBuilder.start();
				StringBuilder output = new StringBuilder();
				BufferedReader reader = new BufferedReader(
					new InputStreamReader(process.getInputStream()));
				String line;
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					output.append(line + "\n");
				}

				int exitVal = process.waitFor();
				if (exitVal == 0) {
					System.out.println(output);
					System.exit(0);
				}  //abnormal...

			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}

			 */

		}
		else {
			cmds.add(graphdb);

			try {
				new ProcessBuilder()
					.directory(new File(TTBulkFiler.getPreload()))
					.command(cmds)
					.start();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
}
