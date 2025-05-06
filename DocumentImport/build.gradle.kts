sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::DocumentImport")
    }
}

dependencies {
    api(project(":common"))
    implementation(libs.imapi)
    implementation(libs.jacksonAnnotations)
    implementation(libs.jacksonCore)
    implementation(libs.jacksonJax)
    implementation(libs.json)
    implementation(libs.juneau)
    implementation(libs.logbackCore)
    implementation(libs.logbackClassic)
    implementation(libs.rdf4jQuery)
    implementation(libs.rdf4jRepoApi)
    implementation(libs.rdf4jRepoHttp)
    implementation(libs.rdf4jRepoSail)
    implementation(libs.slf4j)
}

description = "DocumentImport"