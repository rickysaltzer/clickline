package com.clickhouse

import com.clickhouse.random.GenerateMetricStream
import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.core.subcommands
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.pair
import com.github.ajalt.clikt.parameters.options.required
import com.github.ajalt.clikt.parameters.types.double
import com.github.ajalt.clikt.parameters.types.file
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.server.config.*


class ClickLine() : CliktCommand() {
    override fun run() = Unit
}

class RunServer() : CliktCommand(name = "server", help = "Run ClickLine server") {
    private val configPath by option("--config", help = "Path to sever config")
        .file(mustExist = true, canBeDir = false, mustBeReadable = true)
        .required()

    override fun run() {
        val env = ApplicationConfig(configPath.path)
        ClickLineServer(env).runServer()
    }
}

class GenerateMetrics() : CliktCommand(name = "generate_metrics", help = "Generates a stream of random metrics") {
    private val numEnvironments by option("--numEnvironments", help = "Number of environments (e.g. prod/dev)").int()
        .required()
    private val numHosts by option("--numHosts", help = "Number of hosts per-environment").int().required()
    private val numMetrics by option("--numMetrics", help = "Number of metrics to generate per-host, per-env").int()
        .required()
    private val valueRange by option(
        "--valueRange",
        help = "Range of possible values to generate. These should be doubles (e.g. 0.0 or 100.0)"
    ).pair().required()
    private val valueJitter by option(
        "--valueJitter",
        help = "Max amount of value change between intervals. This helps make the simulation look less chaotic."
    ).double().default(2.5)
    private val intervalLength by option(
        "--intervalLength",
        help = "Duration of time between the metrics (e.g. 5s)"
    ).required()
    private val socketConcurrency by option(
        "--socketConcurrency",
        help = "Number of concurrent sockets to open for writing."
    ).int().default(10)
    private val clickLineHost by option("--clicklineHost", help = "Host of ClickLine server").default("localhost")
    private val clickLinePort by option("--clicklinePort", help = "Port of ClickLine server").int().default(8086)

    override fun run() {
        val duration = kotlin.time.Duration.parse(intervalLength)
        GenerateMetricStream.generateRandomMetrics(
            numEnvironments = numEnvironments,
            numHosts = numHosts,
            numMetrics = numMetrics,
            valueRange = Pair(valueRange.first.toDouble(), valueRange.second.toDouble()),
            intervalLength = duration.inWholeMilliseconds,
            clickLineHost = clickLineHost,
            clickLinePort = clickLinePort,
            maxJitter = valueJitter,
            socketConcurrency = socketConcurrency
        )
    }
}

fun main(args: Array<String>) = ClickLine()
    .subcommands(RunServer(), GenerateMetrics())
    .main(args)