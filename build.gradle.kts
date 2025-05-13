plugins {
  jacoco
  id("groovy-gradle-plugin")
  alias(libs.plugins.sonar)
}

repositories {
  gradlePluginPortal()
}

sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager")
    property("sonar.organization", "endeavourhealth-discovery")
    property("sonar.token", System.getenv("SONAR_LOGIN"))
    property("sonar.host.url", "https://sonarcloud.io")
    property("sonar.junit.reportPaths", "build/test-results/test/binary")
  }
}

if (System.getenv("ENV") == "prod") {
  tasks.build {
    finalizedBy("sonar")
  }
}

configurations.all {
  resolutionStrategy {
    cacheDynamicVersionsFor(30, "minutes")
  }
}

allprojects {
  apply(plugin = "java")
  apply(plugin = "jacoco")

  group = "org.endeavourhealth.informationManager"
  version = "1.0-SNAPSHOT"

  java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
  }

  tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
  }

  tasks.jacocoTestReport {
    reports {
      xml.required.set(true)
    }
  }

  tasks.test {
    finalizedBy(tasks.jacocoTestReport)
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
}