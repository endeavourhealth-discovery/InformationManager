plugins {
  id("java")
}

group = "org.endeavourhealth.qofextractor"
version = "unspecified"

repositories {
  mavenCentral()
}

dependencies {
  implementation("org.apache.poi:poi-ooxml:5.4.1")
  implementation("com.fasterxml.jackson.core:jackson-databind:2.19.1")

  testImplementation(platform("org.junit:junit-bom:5.10.0"))
  testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
  useJUnitPlatform()
}