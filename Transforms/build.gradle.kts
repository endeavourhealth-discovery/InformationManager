sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Transforms")
  }
}
dependencies {
  implementation(libs.imapi)
  implementation(libs.activation)
  implementation(libs.antlr)
  implementation(libs.commonsIO)
  implementation(libs.commonsCompress)
  implementation(libs.factPlusPlus)
  implementation(libs.jaxbApi)
  implementation(libs.jaxbRuntime)
  implementation(libs.jerseyClient)
  implementation(libs.jerseyInject)
  implementation(libs.jsonSimple)
  implementation(libs.logbackCore)
  implementation(libs.logbackClassic)
  implementation(libs.openCsv)
  implementation(libs.openLlet)
  implementation(libs.owlApiApiBinding)
  implementation(libs.owlApiDistribution)
  implementation(libs.rdf4jQuery)
  implementation(libs.rdf4jCommon)
  implementation(libs.rdf4jIterator)
  implementation(libs.rdf4jRepoApi)
  implementation(libs.rdf4jRepoHttp)
  implementation(libs.rdf4jRepoSail)
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