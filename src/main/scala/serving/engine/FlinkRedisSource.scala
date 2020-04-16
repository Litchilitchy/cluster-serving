package serving.engine

import java.util.AbstractMap.SimpleEntry

import serving.utils.FileUtils
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import redis.clients.jedis.{Jedis, JedisPool, StreamEntryID}
import serving.utils.SerParams
import serving.pipeline.{RedisIO, RedisUtils, RedisPool}

import scala.collection.JavaConversions._

class FlinkRedisSource(params: SerParams) extends RichSourceFunction[List[(String, String)]] {
  @volatile var isRunning = true

  var redisPool: JedisPool = null
  var db: Jedis = null


  override def open(parameters: Configuration): Unit = {
    redisPool = new JedisPool()
    db = RedisIO.getRedisClient(redisPool)
    try {
      db.xgroupCreate("image_stream", "serving",
        new StreamEntryID(0, 0), true)
    } catch {
      case e: Exception =>
        println(s"$e exist group")
    }

  }

  override def run(sourceContext: SourceFunction.SourceContext[List[(String,String)]]): Unit = while (isRunning){
    val response = db.xreadGroup(
      "serving",
      "cli",
      512,
      50,
      false,
      new SimpleEntry("image_stream", StreamEntryID.UNRECEIVED_ENTRY))
    if (response != null) {
      for (streamMessages <- response) {
        val key = streamMessages.getKey
        val entries = streamMessages.getValue
        val it = entries.map(e => {
          (e.getFields.get("uri"), e.getFields.get("image"))
        }).toList
        sourceContext.collect(it)
      }
      RedisUtils.checkMemory(db, 0.6, 0.5)
      Thread.sleep(10)
    }
    if (FileUtils.checkStop()) {
      isRunning = false
    }
  }

  override def cancel(): Unit = {
    redisPool.close()
  }

}
