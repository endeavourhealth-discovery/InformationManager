plugins {
  `kotlin-dsl`
  id("java-gradle-plugin")
  id("maven-publish")
}

group = "org.endeavourhealth.plugins"
version = "0.3-SNAPSHOT"
description = "Extract enums from autoGen"

gradlePlugin {
  plugins {
    create("ExtractEnumsFromAutoGen") {
      id = "org.endeavourhealth.plugins.extract-enums-from-auto-gen"
      implementationClass = "org.endeavourhealth.extractEnumsFromAutoGen.ExtractEnumsFromAutoGen"
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
