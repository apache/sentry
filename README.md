What is Sentry?

Apache Sentry is a highly modular system for providing fine grained role based authorization to both data and metadata stored on an Apache Hadoop cluster.

Contact us!

* Mailing lists: https://cwiki.apache.org/confluence/display/SENTRY/Mailing+Lists

Bug and Issues tracker

*  https://issues.apache.org/jira/browse/SENTRY

Building Sentry

Building Sentry requires the following tools:

* Apache Maven 3.0+
* Java JDK 1.6+

To compile Sentry, run:

mvn compile

To run Sentry tests, run:

mvn test -Pdownload-hadoop

To build a distribution, run:

mvn install

The final Sentry distribution artifacts will be in $project/sentry-dist/target/.  
