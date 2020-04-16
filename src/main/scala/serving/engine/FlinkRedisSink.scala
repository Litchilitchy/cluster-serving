package serving.engine

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.apache.log4j.Logger
import redis.clients.jedis.{Jedis, JedisPool}
import serving.pipeline.{RedisIO, RedisPool, RedisUtils}
import serving.utils.SerParams

class FlinkRedisSink(params: SerParams) extends RichSinkFunction[List[(String, String)]] {
  var redisPool: JedisPool = null
  var db: Jedis = null
  var logger: Logger = null
  override def open(parameters: Configuration): Unit = {
    redisPool = new JedisPool()
    logger = Logger.getLogger(getClass)
  }

  override def close(): Unit = {
    redisPool.close()
  }

  override def invoke(value: List[(String, String)]): Unit = {
    logger.info(s"Preparing to write result to redis")
    db = RedisIO.getRedisClient(redisPool)
    val ppl = db.pipelined()
    value.foreach(v => RedisIO.writeHashMap(ppl, v._1, v._2))
    ppl.sync()
    db.close()
    logger.info(s"${value.size} records written to redis")
  }

}
