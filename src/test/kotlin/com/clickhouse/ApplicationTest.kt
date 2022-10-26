package com.clickhouse

import com.clickhouse.metrics.ClickLineMetric
import kotlin.test.*

class ApplicationTest {
    private fun String.toClickLineMetric(): ClickLineMetric {
        return ClickLineMetric.fromString(this)!!
    }

    @Test
    fun testLineProtocolWithTagsAndFields() {
        val lineProtocol = "my.measurement,tag1=tag1Value,tag2=tag2Value value=52,otherValue=32 1465839830100400200"
        lineProtocol.toClickLineMetric().also {
            assertEquals(it.measurement, "my.measurement")
            assertEquals(it.tags["tag1"], "tag1Value")
            assertEquals(it.tags["tag2"], "tag2Value")
            assertEquals(it.values["value"], 52.0)
            assertEquals(it.values["otherValue"], 32.0)
        }
    }

    @Test
    fun testLineProtocolWithTagsAndFieldsWithNoTimestamp() {
        val lineProtocol = "measurementName,tag1=tag1Value,tag2=tag2Value value=52,otherValue=32"
        lineProtocol.toClickLineMetric().also {
            assertEquals(it.measurement, "measurementName")
            assertEquals(it.tags["tag1"], "tag1Value")
            assertEquals(it.tags["tag2"], "tag2Value")
            assertEquals(it.values["value"], 52.0)
            assertEquals(it.values["otherValue"], 32.0)
        }
    }

    @Test
    fun testLineProtocolWithNoTagsAndFields() {
        val lineProtocol = "measurementName,value=52,otherValue=32 1465839830100400200"
        lineProtocol.toClickLineMetric().also {
            assertEquals(it.measurement, "measurementName")
            assertTrue(it.tags.isEmpty())
            assertEquals(it.values["value"], 52.0)
            assertEquals(it.values["otherValue"], 32.0)
        }
    }

    @Test
    fun testLineProtocolWithNoTagsAndFieldsWithNoTimestamp() {
        val lineProtocol = "measurementName,value=52,otherValue=32,thirdValue=1.234456e+78"
        lineProtocol.toClickLineMetric().also {
            assertEquals(it.measurement, "measurementName")
            assertTrue(it.tags.isEmpty())
            assertEquals(it.values["value"], 52.0)
            assertEquals(it.values["otherValue"], 32.0)
        }
    }
}