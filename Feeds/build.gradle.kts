description = "Feeds"
version = "1.0-SNAPSHOT"

dependencies {
  implementation(libs.imapi)

  implementation(libs.jacksonDatabind)
  implementation(libs.slf4j)
  implementation(libs.wsrs)
}

tasks.jar {
  manifest {
    attributes("Main-Class" to "org.endeavourhealth.informationmanager.trud.TrudUpdater")
  }
  isZip64=true
  from(configurations.runtimeClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}