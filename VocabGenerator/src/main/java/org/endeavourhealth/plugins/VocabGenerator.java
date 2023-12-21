package org.endeavourhealth.plugins;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gradle.api.Plugin;
import org.gradle.api.Project;

import java.io.*;
import java.util.List;

public class VocabGenerator implements Plugin<Project> {

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

    private void generateJava(String javaOut, VocabDef def) throws IOException {
        generateCode(
            def,
            javaOut + def.name.toUpperCase() + ".java",
            "package org.endeavourhealth.imapi.vocabulary;\n\npublic class {NAME} {\n",
            "public static final {TYPE}"
        );
    }

    private void generateTypeScript(String tsOut, VocabDef def) throws IOException {
        generateCode(
            def,
            tsOut + def.name.toUpperCase() + ".ts",
            "export class {NAME} {\n",
            "public static readonly"
        );
    }

    private void generateCode(VocabDef def, String filename, String header, String prefix) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {

            writer.write(header.replace("{NAME}", def.name.toUpperCase()));

            if (def.entries != null && !def.entries.isEmpty()) {
                for (VocabDef.Entry entry : def.entries) {
                    String name = entry.name;
                    JsonNode value = entry.value;

                    if (name == null || value == null)
                        throw new IllegalArgumentException("Entry objects must have both a name and a value");

                    if (value.isTextual())
                        writer.write("\t" + prefix.replace("{TYPE}", "String") + " " + format(name) + " = " + value.asText() + ";\n");
                    else if (value.isInt())
                        writer.write("\t" + prefix.replace("{TYPE}", "int") + " " + format(name) + " = " + value.asText() + ";\n");
                    else
                        throw new IllegalArgumentException("Unsupported type");
                }
            }

            writer.write("}\n");
            writer.flush();
        }
    }

    private String format(String pascalCase) {
        return pascalCase
            .replaceAll("([a-z])([A-Z]+)", "$1_$2")
            .toUpperCase();
    }
}
