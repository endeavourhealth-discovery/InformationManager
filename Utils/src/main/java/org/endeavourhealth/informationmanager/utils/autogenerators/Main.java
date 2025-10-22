package org.endeavourhealth.informationmanager.utils.autogenerators;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.sources.IndicatorImporter;

public class Main {
		public static void main(String[] argv) throws Exception {
			new IndicatorImporter().generate("C:\\Users\\david\\GithubRepos\\ImportData\\Smartlife\\",
				"http://smartlifehealth.info/smh#SmartLifeIndicators",
				"http://endhealth.info/im#CarePathways",
				Namespace.SMARTLIFE);
		}
}

