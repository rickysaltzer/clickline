package com.clickhouse.random

import com.clickhouse.metrics.ClickLineMetric
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.utils.io.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.joda.time.DateTime
import java.lang.Double.max
import java.lang.Double.min
import kotlin.random.Random

object GenerateMetricStream {
    fun generateRandomMetrics(
        numHosts: Int,
        numMetrics: Int,
        numEnvironments: Int,
        valueRange: Pair<Double, Double>,
        intervalLength: Long,
        clickLineHost: String,
        clickLinePort: Int,
        maxJitter: Double = 2.5,
        socketConcurrency: Int = 10,
    ) {
        val hostNames = (1..numHosts).map {
            "host-$it"
        }
        val envNames = (1..numEnvironments).map {
            "env-$it"
        }
        val metricNames = (1..numMetrics).map {
            "metric-$it"
        }

        runBlocking(Dispatchers.IO) {
            val serverSelector = SelectorManager(Dispatchers.IO)

            val writerChannel = Channel<List<ClickLineMetric>>()

            (1..socketConcurrency).map { _ ->
                launch(Dispatchers.IO) {
                    val writer = SocketWriter(aSocket(serverSelector).tcp().connect(clickLineHost, clickLinePort))
                    for (metrics in writerChannel) {
                        for (metric in metrics) {
                            writer.writeMetric(metric)
                        }
                        writer.flushWrites()
                    }
                }
            }

            val jobs = envNames.flatMap { env ->
                hostNames.map { host ->
                    launch(Dispatchers.IO) {
                        val lastValues = metricNames.map { _ ->
                            Random.nextDouble(valueRange.first, valueRange.second)
                        }.toMutableList()
                        while (true) {
                            val startTime = System.currentTimeMillis()
                            val timestamp = DateTime(startTime).toString()
                            val metrics = metricNames.map { metric ->
                                val valueOrdinal = metric.split("-")[1].toInt() - 1
                                val lastValue = lastValues[valueOrdinal]
                                val value = Random.nextDouble(
                                    max(valueRange.first, lastValue - maxJitter),
                                    min(valueRange.second, lastValue + maxJitter),
                                )
                                lastValues[valueOrdinal] = value
                                ClickLineMetric(
                                    measurement = metric,
                                    tags = mapOf(
                                        "host" to host,
                                        "env" to env
                                    ),
                                    values = mapOf("value" to value),
                                    timestamp = timestamp,
                                    timestampMillis = startTime
                                )
                            }
                            writerChannel.send(metrics)
                            val elapsed = System.currentTimeMillis() - startTime
                            delay(intervalLength - elapsed)
                        }
                    }
                }
            }
            jobs.forEach { it.join() }
        }
    }
}

private data class SocketWriter(val socket: Socket) {
    private val channel = socket.openWriteChannel(autoFlush = false)

    suspend fun writeMetrics(metrics: List<ClickLineMetric>) {
        channel.writeStringUtf8(metrics.joinToString("\n") { it.toLineProtocolString() })
    }

    suspend fun writeMetric(metric: ClickLineMetric) {
        channel.writeStringUtf8(metric.toLineProtocolString() + "\n")
    }

    fun flushWrites() {
        channel.flush()
    }
}