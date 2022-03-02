package    org.endeavourhealth.imports.sources;

public class Read2Term {

    private String name;
    private String description;

    public Read2Term() {
    }

    public Read2Term(String name, String description) {
        this.name = name;
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public Read2Term setName(String name) {
        this.name = name;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Read2Term setDescription(String description) {
        this.description = description;
        return this;
    }
}
