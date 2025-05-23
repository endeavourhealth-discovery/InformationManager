description = "Transforms"
version = "1.0-SNAPSHOT"

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
  testImplementation(libs.junitSuite)
  testImplementation(libs.cucumber)
  testImplementation(libs.cucumber.junit)
  testImplementation(libs.mockito)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}


tasks.jar {
  manifest {
    attributes("Main-Class" to "org.endeavourhealth.informationmanager.transforms.preload.Preload")
  }
  from(configurations.compileClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}

sonar {
  properties {
    property(
      "sonar.exclusions",
      "**/authored/**, **/models/**, **/online/**, **/preload/**"
    )
  }
}