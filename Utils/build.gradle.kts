description = "IM Utilities"

dependencies {
  implementation(project(":Transforms"))
  implementation(libs.imapi)

  testImplementation(libs.junit)
  testImplementation(libs.junitEngine)
  testImplementation(libs.junitRunner)
}
