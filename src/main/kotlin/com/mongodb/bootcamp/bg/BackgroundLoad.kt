@file:Suppress("UNUSED_LAMBDA_EXPRESSION")

package com.mongodb.bootcamp.bg

import com.mongodb.*
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Filters.*
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates.*
import org.bson.Document
import org.yaml.snakeyaml.Yaml
import java.io.BufferedWriter
import java.io.FileReader
import java.io.FileWriter
import java.time.*
import java.time.format.DateTimeFormatter
import java.util.*
import kotlin.concurrent.thread
import kotlin.concurrent.timer
import kotlin.system.exitProcess


val writeLatencyWriter = BufferedWriter(FileWriter("writeLatency-${ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}.tsv"))
val readLatencyWriter = BufferedWriter(FileWriter("readLatency-${ZonedDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)}.tsv"))

fun main(args: Array<String>) {
    if (args.size < 1) {
        println("Usage: background <conf>")
        exitProcess(0)
    }
    println("Loading configuration from ${args[0]}")

    val conf = Yaml().load(FileReader(args[0]))

    if (conf is Map<*,*>) {
        val description = conf["description"]

        if (description != null) {
            writeLatencyWriter.write("# \"$description\"\n")
            readLatencyWriter.write("# \"$description\"\n")
        }

        val secondary = conf["secondary"] as Boolean
        val readPreference = if (secondary) ReadPreference.secondary() else ReadPreference.primary()

        val writeConcern = conf["writeConcern"] as Map<*,*>
        var wc : WriteConcern
        if (writeConcern["majority"] as Boolean) {
            wc = WriteConcern("majority")
        } else {
            wc = WriteConcern(1)
        }
        if (writeConcern["journal"] as Boolean) {
            wc = wc.withJournal(true)
        }

        val readConcern = if (conf["readConcernMajority"] as Boolean) ReadConcern.MAJORITY else ReadConcern.LOCAL

        val seeds = (conf["seed"] as String).split(",").map { s -> ServerAddress(s) }

        val credentials = if (conf.containsKey("login"))
            listOf(MongoCredential.createScramSha1Credential(conf["login"] as String, conf["authSource"] as String, (conf["password"] as String).toCharArray()))
        else emptyList<MongoCredential>()

        val mongoClient = MongoClient(seeds, credentials, MongoClientOptions.builder().
                readPreference(readPreference).
                writeConcern(wc).
                readConcern(readConcern).
                threadsAllowedToBlockForConnectionMultiplier(20).
                connectionsPerHost(500).
                build())

        val sampleName = if (conf["sampleName"] != null) conf["sampleName"] as String else "sample"
        val hourlyName = if (conf["hourlyName"] != null) conf["hourlyName"] as String else "hourly"
        val dailyName = if (conf["dailyName"]  != null) conf["dailyName"] as String else "daily"

        val db = mongoClient.getDatabase(conf["database"] as String)
        val sample = db.getCollection(sampleName)
        val hourly = db.getCollection(hourlyName)
        var daily = db.getCollection(dailyName)

        if (conf["drop"] as Boolean) {
            sample.drop()
            hourly.drop()
            daily.drop()

            db.createCollection(dailyName)
            daily = db.getCollection(dailyName)

            if (conf["createIndex"] as Boolean) {
                sample.createIndex(and(eq("server", 1), eq("timestamp", 1)))
                hourly.createIndex(and(eq("server", 1), eq("hour", 1)))
                daily.createIndex(and(eq("server", 1), eq("day", 1)))
            }
        }

        val days = conf["days"] as Int
        val servers = conf["servers"] as Int

        // die after x seconds
        timer(initialDelay = (conf["duration"] as Int).toLong() * 1000, period = 10000, action = {
            Thread.sleep(5000)
            writeLatencyWriter.close()
            readLatencyWriter.close()
            exitProcess(0)
        } )
        timer(period = 1000, action = {
            print(".")
        } )

        val random = Random()

        val endOfTime = LocalDate.of(2013, 1, 1)

        sample.find().first()
        hourly.find().first()
        daily.find().first()

        // wait 1 second
        Thread.sleep(1000)

        val sampleConfig = conf["sample"] as Map<*, *>
        runSample(endOfTime, sampleConfig, random, sample, days, servers, secondary)
        runHourly(endOfTime, conf["hourly"] as Map<*,*>, random, hourly, days, servers, secondary)
        runDaily(endOfTime, conf["daily"] as Map<*,*>, random, daily, days, servers, secondary)
    }
}

private fun runSample(endOfTime: LocalDate, config: Map<*,*>, random: Random, collection: MongoCollection<Document>, days: Int, servers: Int, secondary: Boolean) {
    val numReaders = config["read"] as Int
    val numWriters = config["write"] as Int
    val startOfTime = endOfTime.minusDays(days.toLong())
    val from = Date.from(startOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())
    val to = Date.from(endOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())

    val readDelay = if (config["readDelay"] != null) config["readDelay"] as Int else 0
    val writeStop = if (config["writeStop"] != null) config["writeStop"] as Int else -1

    if (!secondary && numWriters > 0) {
        for (i in 1..numWriters) {
            thread {
                println("sample writer $i starting")
                val end = if (writeStop == -1) Instant.ofEpochMilli(Long.MAX_VALUE) else Instant.now().plusSeconds(writeStop.toLong())

                while(Instant.now().isBefore(end)) {
                    val server = random.nextInt(servers) + 1
                    val ts = Date.from(endOfTime.minusDays(random.nextInt(days).toLong()).atStartOfDay(ZoneId.systemDefault()).withHour(random.nextInt(24)).withMinute(random.nextInt(60)).toInstant())
                    val tStart = System.currentTimeMillis()
                    collection.insertOne(Document("server", "server$server")
                            .append("load", random.nextInt(101))
                            .append("timestamp", ts)
                    )
                    val tEnd = System.currentTimeMillis()
                    writeLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
                }

                println("sample writer $i winding down")
            }
        }
    }

    if (numReaders > 0) {
        for (i in 1..numReaders) {
            thread {
                Thread.sleep(readDelay.toLong() * 1000)
                println("sample reader $i starting")

                while (true) {
                    val server = random.nextInt(servers) + 1
                    val tStart = System.currentTimeMillis()
                    val cursor = collection.find(and(eq("server", "server$server"), gte("timestamp", from), lte("timestamp", to))).batchSize(10000).projection(eq("load", 1))
                    var x = 0
                    if (config["exhaustCursor"] == null || config["exhaustCursor"] as Boolean) {
                        cursor.forEach({ x++ })
                    } else {
                        cursor.first()
                    }
                    val tEnd = System.currentTimeMillis()
                    readLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
                }
            }
        }
    }

}

private fun runHourly(endOfTime: LocalDate, config: Map<*,*>, random: Random, collection: MongoCollection<Document>, days: Int, servers: Int, secondary: Boolean) {
    val numReaders = config["read"] as Int
    val numWriters = config["write"] as Int
    val startOfTime = endOfTime.minusDays(days.toLong())
    val from = Date.from(startOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())
    val to = Date.from(endOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())

    val readDelay = if (config["readDelay"] != null) config["readDelay"] as Int else 0
    val writeStop = if (config["writeStop"] != null) config["writeStop"] as Int else -1

    if (numReaders > 0) {
        for (i in 1..numReaders) {
            thread {
                Thread.sleep(readDelay.toLong() * 1000)
                println("hourly reader $i starting")
                while (true) {
                    val server = random.nextInt(servers) + 1
                    val tStart = System.currentTimeMillis()
                    val cursor = collection.find(and(eq("server", "server$server"), gte("hour", from), lte("hour", to))).batchSize(10000)
                    var x = 0
                    if (config["exhaustCursor"] == null || config["exhaustCursor"] as Boolean) {
                        cursor.forEach({ x++ })
                    } else {
                        cursor.first()
                    }
                    val tEnd = System.currentTimeMillis()
                    readLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
                }
            }
        }
    }

    if (!secondary && numWriters > 0) {
        for (i in 1..numWriters) {
            thread {
                println("hourly writer $i starting")

                val end = if (writeStop == -1) Instant.ofEpochMilli(Long.MAX_VALUE) else Instant.now().plusSeconds(writeStop.toLong())
                while(Instant.now().isBefore(end)) {
                    val server = random.nextInt(servers) + 1
                    val ts = Date.from(endOfTime.minusDays(random.nextInt(days).toLong()).atStartOfDay(ZoneId.systemDefault()).withHour(random.nextInt(24)).toInstant())
                    if (config["collision"] as Boolean) {

                        val sync = booleanArrayOf(false, false)
                        thread {
                            doHourlyUpdate(collection, random, server, ts)
                            sync[0] = true
                        }
                        thread {
                            collection.find(and(eq("server", "server$server"), eq("hour", ts))).first()
                            sync[1] = true
                        }

                        while( ! (sync[0] && sync[1])) {
                            Thread.sleep(1)
                        }
                    } else {
                        doHourlyUpdate(collection, random, server, ts)
                    }
                }
                println("hourly writer $i winding down")
            }
        }
    }
}

private fun doHourlyUpdate(collection: MongoCollection<Document>, random: Random, server: Int, ts: Date?) {
    val tStart = System.currentTimeMillis()
    collection.updateOne(
            and(eq("server", "server$server"), eq("hour", ts)),
            and(inc("load_count", 1), inc("load_sum", random.nextInt(101))),
            UpdateOptions().upsert(true)
    )
    val tEnd = System.currentTimeMillis()
    writeLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
}

private fun runDaily(endOfTime: LocalDate, config: Map<*,*>, random: Random, collection: MongoCollection<Document>, days: Int, servers: Int, secondary: Boolean) {
    val numReaders = config["read"] as Int
    val numWriters = config["write"] as Int
    val startOfTime = endOfTime.minusDays(days.toLong())
    val from = Date.from(startOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())
    val to = Date.from(endOfTime.atStartOfDay(ZoneId.systemDefault()).toInstant())

    val readDelay = if (config["readDelay"] != null) config["readDelay"] as Int else 0
    val writeStop = if (config["writeStop"] != null) config["writeStop"] as Int else -1


    if (numReaders > 0) {
        for (i in 1..numReaders) {
            thread {
                Thread.sleep(readDelay.toLong() * 1000)
                println("daily reader $i starting")
                while (true) {
                    val server = random.nextInt(servers) + 1
                    val tStart = System.currentTimeMillis()
                    val cursor = collection.find(and(eq("server", "server$server"), gte("hour", from), lte("hour", to))).batchSize(10000)
                    var x = 0
                    if (config["exhaustCursor"] == null || config["exhaustCursor"] as Boolean) {
                        cursor.forEach({ x++ })
                    } else {
                        cursor.first()
                    }
                    val tEnd = System.currentTimeMillis()
                    readLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
                }
            }
        }
    }

    if (!secondary && numWriters > 0) {
        for (i in 1..numWriters) {
            thread {
                println("daily writer $i starting")
                val end = if (writeStop == -1) Instant.ofEpochMilli(Long.MAX_VALUE) else Instant.now().plusSeconds(writeStop.toLong())

                while(Instant.now().isBefore(end)) {
                    val server = random.nextInt(servers) + 1
                    val ts = Date.from(endOfTime.minusDays(random.nextInt(days).toLong()).atStartOfDay(ZoneId.systemDefault()).toInstant())
                    val hour = random.nextInt(24)

                    if (config["collision"] as Boolean) {
                        val sync = booleanArrayOf(false, false)
                        thread {
                            doDailyUpdate(collection, hour, random, server, ts)
                            sync[0] = true
                        }
                        thread {
                            val tStart = System.currentTimeMillis()
                            collection.find(and(eq("server", "server$server"), eq("day", ts))).first()
                            val tEnd = System.currentTimeMillis()
                            readLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
                            sync[1] = true
                        }

                        while(! (sync[0] && sync[1]) ) {
                            Thread.sleep(1)
                        }
                    } else {
                        doDailyUpdate(collection, hour, random, server, ts)
                    }
                }
                println("daily writer $i winding down")
            }
        }
    }
}

private fun doDailyUpdate(collection: MongoCollection<Document>, hour: Int, random: Random, server: Int, ts: Date?) {
    val tStart = System.currentTimeMillis()
    collection.updateOne(
            and(eq("server", "server$server"), eq("day", ts)),
            and(inc("hours.$hour.load_count", 1), inc("hours.$hour.load_sum", random.nextInt(101))),
            UpdateOptions().upsert(true)
    )
    val tEnd = System.currentTimeMillis()
    writeLatencyWriter.write("$tEnd\t${tEnd - tStart}\n")
}