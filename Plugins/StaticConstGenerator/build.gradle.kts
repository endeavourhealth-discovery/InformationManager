plugins {
  id("java-gradle-plugin")
  id("maven-publish")
}

group = "org.endeavourhealth.plugins"
version = "1.4-SNAPSHOT"
description = "Static Const Generator"

gradlePlugin {
  plugins {
    create("StaticConstGenerator") {
      id = "org.endeavourhealth.staticConstGenerator.StaticConstGenerator"
      implementationClass = "org.endeavourhealth.staticConstGenerator.StaticConstGenerator"

    }
  }
}

dependencies {
  implementation(libs.jacksonDatabind)
  implementation(libs.gradleApi)

}

publishing {
  repositories {
    maven {
      url = uri("https://artifactory.endhealth.co.uk/repository/maven-snapshots")
      credentials {
        username = System.getenv("MAVEN_USERNAME")
        password = System.getenv("MAVEN_PASSWORD")
      }
    }
  }
}
