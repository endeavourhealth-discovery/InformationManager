sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Feeds")
    }
}

dependencies {
  api project (":common")
  implementation(libs.imapi)

    implementation(libs.commonsIO)
    implementation(libs.jacksonAnnotations)
    implementation(libs.jacksonCore)
    implementation(libs.jacksonJax)
    implementation(libs.jerseyClient)
    implementation(libs.json)
    implementation(libs.owlApiApiBinding)
    implementation(libs.owlApiDistribution)
    implementation(libs.rdf4jQuery)
    implementation(libs.rdf4jRepoApi)
    implementation(libs.rdf4jRepoHttp)
    implementation(libs.rdf4jRepoSail)
    implementation(libs.slf4j)
    implementation(libs.wsrs)


  testImplementation(libs.junit)
}

description = "Scratch"