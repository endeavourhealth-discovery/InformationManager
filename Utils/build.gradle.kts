sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Utils")
  }
}

dependencies {
  implementation(project(":Transforms"))
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
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}

tasks.test {
  useJUnitPlatform()
}

description = "Utils"