sonar {
  properties {
    property("sonar.projectKey", "endeavourhealth-discovery_informationManager::common")
    }
}

dependencies {
    implementation(libs.imapi)
    implementation(libs.collections4)
    implementation(libs.zip4j)
    implementation(libs.owlApiApiBinding)
  implementation(libs.owlApiDistribution)
  implementation(libs.commonsIO)
  implementation(libs.guava)
  implementation(libs.snomed)
  implementation(libs.antlr)
}

configure<PublishingExtension>  {
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

description = "common"
