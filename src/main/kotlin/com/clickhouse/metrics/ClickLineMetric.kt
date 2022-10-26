package com.clickhouse.metrics

import org.joda.time.DateTime

/**
 * Represents a LineProtocol metric ready for insertion into ClickHouse
 */
data class ClickLineMetric(
    val measurement: String,
    val tags: Map<String, String>,
    val values: Map<String, Double>,
    val timestamp: String,
    val timestampMillis: Long
) {
    fun toLineProtocolString(): String {
        val tagsString = tags.map {
            "${it.key}=${it.value}"
        }.joinToString(",")
        val valuesString = values.map {
            "${it.key}=${it.value}"
        }.joinToString(",")
        val timestampLong = DateTime.parse(timestamp).millis
        return "$measurement,$tagsString $valuesString ${timestampLong * 1000 * 1000}"
    }

    companion object {
        fun fromString(lineProtocolString: String): ClickLineMetric? {
            // Wrap parse attempt in a try/catch and return NULL if the payload is malformed
            try {
                // Grab the measurement name and the rest of the suffix (tags, fields) as a second String
                val (measurement, protocolSuffix) = lineProtocolString.split(",", limit = 2).let {
                    Pair(it[0], it[1].split(" "))
                }
                // Use the timestamp if set, otherwise use the current relative time
                val timestamp = if (protocolSuffix.last().matches("\\d+".toRegex())) {
                    protocolSuffix.last().toLong() / 1000 / 1000
                } else {
                    System.currentTimeMillis()
                }.let {
                    DateTime(it)
                }
                // Find both tags (optional) and fields using a simple filter
                val keyValues = protocolSuffix.filter { it.contains("=") }
                // Parse both the tags (optional) and fields
                val (tags, values) = if (keyValues.size > 1) {
                    // Both tags and fields are set
                    Pair(
                        keyValues[0].split(",").map { it.split("=") }.associate {
                            it[0] to it[1].removeSurrounding("\"")
                        },
                        keyValues[1].split(",").map { it.split("=") }.associate {
                            it[0] to it[1].removeSurrounding("\"").toDouble()
                        }
                    )
                } else {
                    // Only field is set
                    Pair(
                        null,
                        keyValues[0].split(",").map { it.split("=") }.associate {
                            it[0] to it[1].toDouble()
                        },
                    )
                }
                // Return ClickLine Metric
                return ClickLineMetric(
                    measurement = measurement,
                    tags = tags ?: emptyMap(),
                    values = values,
                    timestamp = timestamp.toString("yyyy-MM-dd HH:mm:ss"),
                    timestampMillis = timestamp.millis
                )
            } catch (e: Exception) {
                return null
            }
        }
    }
}