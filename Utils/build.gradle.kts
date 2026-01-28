plugins {
  id("com.gradleup.shadow") version "9.2.2"
}
description = "IM Utilities"
version = "1.0-SNAPSHOT"

dependencies {
  implementation(project(":Transforms"))

  implementation(libs.jacksonDatabind)
  implementation(libs.jacksonDatabind)
  implementation(libs.jakartaActivation)
  implementation(libs.jerseyClient)
  implementation(libs.jerseyHk2)
  implementation(libs.slf4j)
  implementation(libs.rdf4jRepoApi)
  implementation(libs.rdf4jSail)
  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}

tasks.shadowJar {
  isZip64 = true
  duplicatesStrategy = DuplicatesStrategy.INCLUDE
  mergeServiceFiles()
}

tasks.jar {
  from(configurations.compileClasspath.get().map { if (it.isDirectory) it else zipTree(it) })
  duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
