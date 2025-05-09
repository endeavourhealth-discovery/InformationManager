sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::Utils")
  }
}

dependencies {
  implementation(project(":Transforms"))
  implementation(libs.imapi)

  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}

tasks.test {
  useJUnitPlatform()
}

description = "Utils"