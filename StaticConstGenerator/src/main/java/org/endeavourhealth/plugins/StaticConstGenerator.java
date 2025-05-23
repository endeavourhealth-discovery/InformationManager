package org.endeavourhealth.plugins;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.internal.impldep.org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.StringJoiner;

public class StaticConstGenerator implements Plugin<Project> {

  public void apply(Project project) {
    VocabGeneratorExtension extension = project.getExtensions().create("staticConstGenerator", VocabGeneratorExtension.class);
    extension.getInputJson().convention("./vocab.json");
    extension.getJavaOutputFolder().convention("./");
    extension.getTypeScriptOutputFolder().convention("./");
    project.getTasks().register("staticConstGenerator", t -> t
      .doLast(s -> execute(
        project.getProjectDir().getAbsolutePath(),
        extension.getInputJson().get(),
        extension.getJavaOutputFolder().get(),
        extension.getTypeScriptOutputFolder().get())
      )
    );
  }

  public void execute(String baseDir, String jsonIn, String javaOut, String tsOut) {
    if (!baseDir.endsWith("/"))
      baseDir += "/";

    jsonIn = baseDir + jsonIn;
    javaOut = baseDir + javaOut;
    tsOut = baseDir + tsOut;

    try (InputStream in = new FileInputStream(jsonIn)) {

      ObjectMapper mapper = new ObjectMapper();
      List<VocabDef> defs = mapper.readValue(in, new TypeReference<>() {
      });

      for (VocabDef def : defs) {
        generateJava(javaOut, def);
        generateTypeScript(tsOut, def);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void generateJava(String javaOut, VocabDef def) {
    generateCode(
      def,
      javaOut + def.name.toUpperCase() + ".java",
      "    ",
      System.lineSeparator(),
      "public static final {TYPE}",
      "// This file is autogenerated. Any edits made will be lost. To make changes go to imapi/api/vocab.json and re-run gradle task staticConstGenerator",
      "",
      "package org.endeavourhealth.imapi.vocabulary;",
      "",
      "public class {NAME} {"
    );
  }

  private void generateTypeScript(String tsOut, VocabDef def) {
    generateCode(
      def,
      tsOut + def.name.toUpperCase() + ".ts",
      "  ",
      "\n",
      "public static readonly",
      "// This file is autogenerated. Any edits made will be lost. To make changes go to imapi/api/vocab.json and re-run gradle task staticConstGenerator",
      "",
      "export class {NAME} {"
    );
  }

  private void generateCode(VocabDef def, String filename, String indent, String eol, String prefix, String... headers) {
    StringJoiner output = new StringJoiner(eol);

    for (String header : headers)
      output.add(header.replace("{NAME}", def.name.toUpperCase()));

    if (def.entries != null && !def.entries.isEmpty()) {
      for (VocabDef.Entry entry : def.entries) {
        String name = entry.name;
        JsonNode value = entry.value;

        if (name == null || value == null)
          throw new IllegalArgumentException("Entry objects must have both a name and a value");

        if (value.isTextual())
          output.add(indent + prefix.replace("{TYPE}", "String") + " " + name + " = " + value.asText() + ";");
        else if (value.isInt())
          output.add(indent + prefix.replace("{TYPE}", "int") + " " + name + " = " + value.asText() + ";");
        else
          throw new IllegalArgumentException("Unsupported type");
      }
    }

    output.add("}").add("");

    writeIfChangedOrNotExists(filename, output.toString());
  }

  private void writeIfChangedOrNotExists(String filename, String output) {

    File f = new File(filename);

    try {
      String original = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
      if (original.equals(output)) {
        // System.out.println(f.getName() + " unchanged...");
        return;
      }
      System.out.println(f.getName() + " changed, overwriting...");
    } catch (IOException e) {
      System.out.println(f.getName() + " does not exist, creating...");
    }

    try {
      Files.writeString(Paths.get(filename), output);
    } catch (IOException e) {
      System.err.println("Error writing output " + f.getName());
      System.exit(-1);
    }
  }
}
