sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Feeds")
  }
}

dependencies {
  implementation(libs.imapi)

  implementation(libs.jacksonDatabind)
  implementation(libs.slf4j)
  implementation(libs.wsrs)
}

description = "Feeds"

tasks.jar {
  manifest {
    attributes("Main-Class" to "org.endeavourhealth.informationmanager.trud.TrudUpdater")
  }
  from(configurations.compileClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}