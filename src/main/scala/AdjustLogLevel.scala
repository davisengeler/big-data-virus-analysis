package org.virus

import org.apache.spark.Logging
import org.apache.log4j.{Level, Logger}

/** Utility functions to adust Log level. */
object AdjustLogLevel extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [ERROR]")
      //Level.WARN

      Logger.getRootLogger.setLevel(Level.ERROR)
    }
  }
}
