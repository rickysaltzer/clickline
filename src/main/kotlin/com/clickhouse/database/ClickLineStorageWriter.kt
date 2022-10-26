package com.clickhouse.database

import com.clickhouse.client.*
import com.clickhouse.metrics.ClickLineMetric
import org.msgpack.core.MessagePack

class ClickLineStorageWriter(
    hostname: String,
    username: String,
    password: String,
    port: Int,
    ssl: Boolean = true,
    private val database: String,
    private val tableName: String
) {
    private val endpoint = ClickHouseNode.builder()
        .host(hostname)
        .credentials(ClickHouseCredentials.fromUserAndPassword(username, password))
        .addOption("ssl", ssl.toString().lowercase())
        .port(ClickHouseProtocol.HTTP, port)
        .build()
    private val client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)

    fun writeBatch(batch: List<ClickLineMetric>) {
        client.connect(endpoint).format(ClickHouseFormat.MsgPack).write().let {
            it
                .query("INSERT INTO `$database`.`$tableName`")
                .data { writer ->
                    val packer = MessagePack.newDefaultPacker(writer)
                    for (metric in batch) {
                        packer.packString(metric.measurement)
                        packer.packLong(metric.timestampMillis / 1000)
                        packer.packMapHeader(metric.tags.size)
                        metric.tags.forEach { (k, v) ->
                            packer.packString(k)
                            packer.packString(v)
                        }
                        packer.packMapHeader(metric.values.size)
                        metric.values.forEach { (k, v) ->
                            packer.packString(k)
                            packer.packFloat(v.toFloat())
                        }
                    }
                    packer.flush()
                    writer.flush()
                    writer.close()
                }
        }.executeAndWait()
    }
}