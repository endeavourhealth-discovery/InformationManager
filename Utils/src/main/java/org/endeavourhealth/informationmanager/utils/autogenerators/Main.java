package org.endeavourhealth.informationmanager.utils.autogenerators;
import org.endeavourhealth.imapi.vocabulary.Namespace;
import org.endeavourhealth.informationmanager.transforms.sources.ImportUtils;
import org.endeavourhealth.informationmanager.transforms.sources.IndicatorGenerator;

public class Main {
		public static void main(String[] argv) throws Exception {
			new IndicatorGenerator().generate("C:\\Users\\david\\GithubRepos\\ImportData\\Smartlife\\",
				"http://smartlifehealth.info/smh#SmartLifeIndicators",
				"http://endhealth.info/im#CarePathways",
				Namespace.SMARTLIFE);

		}
}

