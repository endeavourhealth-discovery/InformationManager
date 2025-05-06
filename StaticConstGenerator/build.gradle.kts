plugins {
  id("java-gradle-plugin")
}

group = "org.endeavourhealth.plugins"
version = "1.2-SNAPSHOT"

gradlePlugin {
  plugins {
    VocabGenerator {
      id = "org.endeavourhealth.plugins.StaticConstGenerator"
      implementationClass = "org.endeavourhealth.plugins.StaticConstGenerator"
    }
  }
}

repositories {
  mavenLocal()
  mavenCentral()
  maven {
    url = uri("https://artifactory.endhealth.co.uk/repository/maven-releases")
  }
  maven {
    url = uri("https://artifactory.endhealth.co.uk/repository/maven-snapshots")
  }
}

dependencies {
  implementation(libs.jacksonDatabind)
  implementation(libs.gradleApi)
}

configure<PublishingExtension> {
  repositories {
    maven {
      url = uri("https://artifactory.endhealth.co.uk/repository/maven-snapshots")
      credentials {
        username = System.getProperty("MAVEN_USERNAME")
        password = System.getProperty("MAVEN_PASSWORD")
      }
    }
  }
}

tasks.test {
  useJUnitPlatform()
}