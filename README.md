# Billing Settlement Engine 

<a href = "https://sonarqube-devops.worldpay.local/dashboard?id=com.worldpay.pms%3Abilling-settlement-engine">
<img alt="Sonar status" src="https://sonarqube-devops.worldpay.local/api/project_badges/measure?project=com.worldpay.pms%3Abilling-settlement-engine&metric=alert_status">
</a>
<a href = "https://sonarqube-devops.worldpay.local/component_measures?id=com.worldpay.pms%3Abilling-settlement-engine&metric=coverage">
<img alt="Coverage" src="https://sonarqube-devops.worldpay.local/api/project_badges/measure?project=com.worldpay.pms%3Abilling-settlement-engine&metric=coverage">
</a>

## IDE Setup
* IntelliJ Community Edition or better
* Maven 3.3+
* settings.xml using WP's Nexus as maven repo

## IntelliJ Plugins
* Lombok plugin installed
* SonarLint plugin
  * Configure it against the WP sonarqube server to get the rules applied locally.
* Code Style Formatter: https://github.com/google/styleguide/blob/gh-pages/intellij-java-google-style.xml

## Database setup
You need an oracle database in order to build the project please check the 
[wiki page](https://github.devops.worldpay.local/NAP/pricing-calculation-engine/wiki/Database-setup).

## Building
You can skip tests locally passing `-Dskip.unit.tests=true` to maven.  
You can skip dependency checks passing: `-Denforcer.skip=true`  
e.g. `mvn package -Dskip.unit.tests=true -Denforcer.skip=true` will build a package without running tests or dependency checks

## Diagram
<img src="./docs/billling.svg">
