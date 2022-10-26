package com.clickhouse

import com.clickhouse.database.ClickLineStorageWriter
import com.clickhouse.metrics.ClickLineMetric
import io.ktor.network.selector.*
import io.ktor.network.sockets.*
import io.ktor.server.application.*
import io.ktor.server.config.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.utils.io.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.joda.time.DateTime
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

class ClickLineServer(private val env: ApplicationConfig) {
    @OptIn(ExperimentalTime::class)
    fun runServer() {
        runBlocking {
            val selectorManager = SelectorManager(Dispatchers.IO)
            val tcpPort = env.property("server.tcp.port").getString().toInt()
            val httpPort = env.property("server.http.port").getString().toInt()
            val host = env.property("clickhouse.hostname").getString()
            val username = env.property("clickhouse.username").getString()
            val password = env.property("clickhouse.password").getString()
            val chPort = env.property("clickhouse.port").getString().toInt()
            val useSSL = env.property("clickhouse.ssl").getString().toBoolean()
            val database = env.property("clickhouse.database").getString()
            val table = env.property("clickhouse.table").getString()
            val batchSize = env.property("clickhouse.batch.size").getString().toInt()
            val batchInterval = env.property("clickhouse.batch.intervalMillis").getString().toInt()
            val serverSocket = aSocket(selectorManager).tcp().bind("0.0.0.0", tcpPort)
            val numWriters = env.propertyOrNull("clickhouse.batch.writers")?.getString()?.toInt() ?: 5

            val metricPersistenceChannel = Channel<ClickLineMetric>()

            (1..numWriters).forEach { writerId ->
                launch(Dispatchers.IO) {
                    val clickhouse = ClickLineStorageWriter(host, username, password, chPort, useSSL, database, table)
                    val currentBatch = mutableListOf<ClickLineMetric>()
                    var lastSentTimestamp: DateTime? = null
                    while (true) {
                        metricPersistenceChannel.tryReceive().getOrNull()?.let { m ->
                            currentBatch.add(m)
                        }
                        if (lastSentTimestamp == null && currentBatch.isNotEmpty()) {
                            lastSentTimestamp = DateTime.now()
                        }
                        if (currentBatch.isEmpty()) {
                            continue
                        }
                        if (currentBatch.size >= batchSize || DateTime.now()
                                .minusMillis(batchInterval) >= lastSentTimestamp
                        ) {
                            val elapsed = measureTime {
                                clickhouse.writeBatch(currentBatch)
                            }
                            println("[#$writerId] Elapsed: $elapsed")
                            currentBatch.clear()
                            lastSentTimestamp = DateTime.now()
                        }
                    }
                }
            }

            val tcpServer = launch {
                println("ClickLine TCP server listening on port [$tcpPort]....")
                while (true) {
                    val socket = serverSocket.accept()
                    launch(Dispatchers.IO) {
                        val receiveChannel = socket.openReadChannel()
                        try {
                            while (!receiveChannel.isClosedForRead) {
                                receiveChannel.readUTF8Line()?.toClickLineMetric()?.let {
                                    metricPersistenceChannel.send(it)
                                }
                            }
                        } catch (e: Throwable) {
                            socket.close()
                        }
                    }
                }
            }

            val httpServer = launch {
                println("ClickLine HTTP server listening on port [$httpPort]...")
                embeddedServer(Netty, port = httpPort) {
                    routing {
                        post("/") {
                            val receiveChannel = call.receiveChannel()
                            while (!receiveChannel.isClosedForRead) {
                                receiveChannel.readUTF8Line()?.toClickLineMetric()?.let {
                                    metricPersistenceChannel.send(it)
                                }
                            }
                            call.respondText(":)")
                        }
                    }
                }.start(wait = true)
            }
            listOf(tcpServer, httpServer).forEach { it.join() }
        }

    }
}

private fun String.toClickLineMetric(): ClickLineMetric? {
    return ClickLineMetric.fromString(this)
}