//https://github.com/GoogleCloudPlatform/java-docs-samples/blob/master/logging/logback/src/main/java/com/example/logging/logback/enhancers/ExampleEnhancer.java
//https://medium.com/@linda0511ny/akka-server-logging-with-slf4j-logback-in-google-cloud-platform-b2a7455f61af
// https://github.com/yanns/test_structured_logging/blob/master/src/main/scala/example/ExampleLoggingEnhancer.scala
package edu.columbia.advbigdata.utils

// [START logging_enhancer]


import com.google.cloud.logging.{LogEntry, LoggingEnhancer}

class GCPLoggingEnhancer extends LoggingEnhancer {
  override def enhanceLogEntry(builder: LogEntry.Builder): Unit = {
    builder.addLabel("nicks-key", "nicks-value")
  }
}
// [END logging_enhancer]
