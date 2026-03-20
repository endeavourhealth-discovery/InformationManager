plugins {
  `kotlin-dsl`
  id("java-gradle-plugin")
  id("maven-publish")
}

group = "org.endeavourhealth.plugins"
version = "0.3-SNAPSHOT"
description = "Typescript const enum to enum"

gradlePlugin {
  plugins {
    create("TypescriptConstEnumToEnum") {
      id = "org.endeavourhealth.plugins.typescript-const-enum-to-enum"
      implementationClass = "org.endeavourhealth.typescriptConstEnumToEnum.TypescriptConstEnumToEnum"
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
