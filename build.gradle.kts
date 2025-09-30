plugins {
  id("java")
  id("jacoco")
  id("groovy-gradle-plugin")
  alias(libs.plugins.sonar)
}

repositories {
  gradlePluginPortal()
}

java.sourceCompatibility = JavaVersion.VERSION_21
java.targetCompatibility = JavaVersion.VERSION_21

val ENV = System.getenv("ENV") ?: "dev"
println("Build environment = [$ENV]")
if (ENV == "prod") {
  tasks.build { finalizedBy("safeSonar") }
}

tasks.register("safeSonar") {
  //Action block
  doLast {
    try {
      sonar
    } catch (e: Error) {
      throw StopActionException(e.toString())
    }
  }
}

sonar {
  properties {
    property("sonar.token", System.getenv("SONAR_LOGIN"))
    property("sonar.host.url", "https://sonarcloud.io")
    property("sonar.organization", "endeavourhealth-discovery")
    property("sonar.projectKey", "InformationManager")
    property("sonar.projectName", "Information Manager Tools")
    property("sonar.sources", "src/main/java")
    property("sonar.tests", "src/test/java")
    property("sonar.junit.reportPaths", "build/test-results/test")
  }
}

subprojects {
  apply(plugin = "java")
  apply(plugin = "jacoco")

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

  tasks.test {
    jvmArgs("-XX:+EnableDynamicAgentLoading")
    useJUnitPlatform()
    finalizedBy("jacocoTestReport")
  }

  tasks.jacocoTestReport {
    reports {
      xml.required.set(true)
    }
  }
}