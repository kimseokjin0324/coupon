// API 관문(Spring Cloud Gateway). 본 앱과 같은 Boot 4 계열을 사용한다. 빌드: ./gradlew jibDockerBuild
// 버전은 루트 build.gradle.kts 가 이미 classpath 에 올려 두었으므로 여기서는 생략한다.
// (버전을 다시 적으면 루트와 다를 때 "already on the classpath with a different version" 에러)
plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("io.spring.dependency-management")
    id("com.google.cloud.tools.jib")
}

group = "com.seokjin"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
}

dependencyManagement {
    imports {
        // BOM 의 Kotlin 버전을 플러그인 버전에 맞춘다. 안 맞추면 증분 컴파일이 깨진다.
        mavenBom("org.springframework.boot:spring-boot-dependencies:4.0.7") {
            bomProperty("kotlin.version", "2.3.21")
        }
        mavenBom("org.springframework.cloud:spring-cloud-dependencies:2025.1.2")
    }
}

dependencies {
    implementation("org.springframework.cloud:spring-cloud-starter-gateway-server-webflux")
    // RedisRateLimiter 는 reactive Redis 필요. 저장소는 같은 Redis 재사용.
    implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    testImplementation("org.springframework.boot:spring-boot-starter-webflux-test")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

kotlin {
    compilerOptions {
        freeCompilerArgs.add("-Xjsr305=strict")
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

jib {
    from {
        // 본 앱과 같은 base 이미지 사용.
        image = "eclipse-temurin:25-jre"
        platforms {
            platform {
                architecture = "amd64"
                os = "linux"
            }
        }
    }
    to {
        image = "coupon-gateway"
        tags = setOf("latest")
    }
    container {
        mainClass = "com.seokjin.gateway.GatewayApplicationKt"
        ports = listOf("8080")
        creationTime.set("USE_CURRENT_TIMESTAMP")
    }
}