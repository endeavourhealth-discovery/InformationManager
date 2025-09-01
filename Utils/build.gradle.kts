description = "IM Utilities"

dependencies {
  implementation(project(":Transforms"))

  implementation(libs.jacksonDatabind)
  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}
