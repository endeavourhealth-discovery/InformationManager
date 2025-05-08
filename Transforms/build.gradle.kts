sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Transforms")
  }
}
dependencies {
  implementation(libs.imapi)

  implementation(libs.apache)
  implementation(libs.antlr)
  implementation(libs.jacksonDatabind)
  implementation(libs.jaxbApi)
  implementation(libs.jaxbRuntime)
  implementation(libs.jsonSimple)
  implementation(libs.openCsv)
  implementation(libs.owlApiApiBinding)
  implementation(libs.owlApiDistribution)
  implementation(libs.rdf4jRepoApi)
  implementation(libs.slf4j)
  implementation(libs.wsrs)
  implementation(libs.zip4j)

  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}

tasks.test {
  useJUnitPlatform()
}

description = "Transforms"

tasks.jar {
  manifest {
    attributes("Main-Class" to "org.endeavourhealth.informationmanager.transforms.preload.Preload")
  }
  from(configurations.compileClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}