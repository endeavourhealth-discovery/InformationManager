plugins {
  id("java-gradle-plugin")
  id("maven-publish")
}

group = "org.endeavourhealth.plugins"
version = "1.2-SNAPSHOT"

gradlePlugin {
  plugins {
    create("StaticConstGenerator") {
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

tasks.test {
  useJUnitPlatform()
}