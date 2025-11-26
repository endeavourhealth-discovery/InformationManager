description = "IM Utilities"

dependencies {
  implementation(project(":Transforms"))

  implementation(libs.imapi)
  implementation(libs.jacksonDatabind)
  implementation(libs.wsrs)
  implementation(libs.logback)
  implementation(libs.slf4j)
  implementation(libs.rdf4jRepoApi)

  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}
