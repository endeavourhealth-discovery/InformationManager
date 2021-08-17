public class ExporterApp {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("You need to provide a root path  to send the data to and export type");
			System.exit(-1);
		}
		String exportType=args[1].toLowerCase();
		switch (exportType) {
			case "conceptsets":
				ConceptSetExporter exporter= new ConceptSetExporter();
				exporter.exportAll(args[0]);
				break;
			default:
				throw new Exception("Unrecognised export type");
		}
	}
}
