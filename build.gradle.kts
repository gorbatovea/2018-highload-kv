// See https://gradle.org and https://github.com/gradle/kotlin-dsl

// Apply the java plugin to add support for Java
plugins {
    java
    application
}

repositories {
    jcenter()
}

dependencies {
    //SLF4J-api
    compile(group = "org.slf4j", name = "slf4j-api", version = "1.7.25")

    //Web-server "one-nio"
    compile(group = "ru.odnoklassniki", name = "one-nio", version = "1.0.2")

    // Annotations for better code documentation
    compile("com.intellij:annotations:12.0")

    // JUnit test framework
    testCompile("junit:junit:4.12")

    // HTTP client for unit tests
    testCompile("org.apache.httpcomponents:fluent-hc:4.5.3")
}

tasks {
    "test"(Test::class) {
        maxHeapSize = "128m"
    }
}

application {
    // Define the main class for the application
    mainClassName = "ru.mail.polis.Server"

    // And limit Xmx
    applicationDefaultJvmArgs = listOf("-Xmx128m")
}
