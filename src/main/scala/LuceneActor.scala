package eu.pandem.storage;

import akka.pattern.{ask, pipe}
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.actor.{ActorSystem, Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.stream.scaladsl.{Source}
import akka.actor.ActorRef
import akka.Done
import akka.util.ByteString
import akka.util.{Timeout}
import java.time.LocalDateTime
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ArrayBuffer}
import java.time.temporal.{IsoFields, ChronoUnit}
import java.time.{Instant, ZoneOffset, LocalDateTime}
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.{SparkSession, Column, Dataset, Row, DataFrame}
import org.apache.spark.sql.functions.{col, udf, input_file_name, explode, coalesce, when, lit, concat, struct, expr, lower}
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.lucene.search.{Query, TermQuery, BooleanQuery, PrefixQuery, TermRangeQuery}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.index.Term
import org.apache.lucene.search.spell.LuceneDictionary
import scala.concurrent.ExecutionContext
import scala.util.{Try, Success, Failure}
import java.nio.charset.StandardCharsets
import spray.json.JsonParser
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean}

class LuceneActor(conf:Settings) extends Actor with ActorLogging {
  implicit val executionContext = context.system.dispatchers.lookup("lucene-dispatcher")
  implicit val cnf = conf
  def receive = {
    case ts:LuceneActor.CommitRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future{
        LuceneActor.commit()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.CloseRequest =>
      implicit val holder = LuceneActor.writeHolder
      Future {
        LuceneActor.commit(closeDirectory = true)
        LuceneActor.closeSparkSession()
      }
      .map{c =>
        LuceneActor.Success(s"$c Commit done processed")
      }
      .pipeTo(sender())
    case LuceneActor.AggregatedRequest(collection, topic, from, to, filters, jsonnl, caller) => 
      implicit val timeout: Timeout = conf.fsBatchTimeout.seconds //For ask property
      implicit val holder = LuceneActor.readHolder
      val indexes = LuceneActor.getReadIndexes(collection, from, to).toSeq
      Future {
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("[", ByteString.UTF_8), Duration.Inf)
        }
        indexes
          .iterator
          .flatMap{case i => 
            val qb = new BooleanQuery.Builder()
            qb.add(new TermQuery(new Term("topic", topic)), Occur.MUST) 
            qb.add(TermRangeQuery.newStringRange("created_date", from.toString.take(10), to.toString.take(10), true, true), Occur.MUST) 
            filters.foreach{case(field, value) => qb.add(new TermQuery(new Term(field, value)), Occur.MUST)}
            i.searchAll(qb.build)
          }
          .map(doc => Serialisation.luceneDocFormat.write(doc))
          .foreach{line => 
            Await.result(caller ? ByteString(
              s"${line.toString}\n", 
              ByteString.UTF_8
            ), Duration.Inf)
          }
        if(!jsonnl) { 
          Await.result(caller ?  ByteString("]", ByteString.UTF_8), Duration.Inf)
        }
        caller ! Done
      }.recover {
        case e: Exception => 
          caller ! ByteString(s"[Stream--error]: ${e.getMessage}: ${e.getStackTrace.mkString("\n")}", ByteString.UTF_8)
      }.onComplete { case  _ =>
      }
    case LuceneActor.PeriodRequest(collection) =>
      implicit val holder = LuceneActor.readHolder
      Future{
        val sPath = Paths.get(conf.fsRoot, collection)
        if(!Files.exists(sPath))
          LuceneActor.PeriodResponse(None, None)
        else {
          val keys = Files.list(sPath).iterator.asScala.toSeq.map(p => p.getFileName.toString)
          val dates = Seq(keys.min, keys.max)
            .distinct
            .map(key => LuceneActor.getIndex(collection, key))
            .flatMap{index =>
              val aDates = ArrayBuffer[String]()
              val searcher = index.useSearcher()
              val dateIter =  new LuceneDictionary(searcher.getIndexReader, "created_date").getEntryIterator()
              var date = dateIter.next
              while(date != null) {
                aDates += date.utf8ToString 
                date = dateIter.next
              }
              index.releaseSearcher(searcher)
              aDates
            }
          if(dates.size> 0)
            LuceneActor.PeriodResponse(Some(dates.min), Some(dates.max))
          else
            LuceneActor.PeriodResponse(None, None)
        }
      }
      .pipeTo(sender())
    case b => 
      Future(LuceneActor.Failure(s"Cannot understund $b of type ${b.getClass.getName} as message")).pipeTo(sender())
  }
}

case class IndexHolder(
  var spark:Option[SparkSession] = None,
  val dirs:HashMap[String, String] = HashMap[String, String](),
  val indexes:HashMap[String, Driver] = HashMap[String, Driver](),
  val writeEnabled:Boolean = false,
  var toAggregate:ArrayBuffer[JsObject] = ArrayBuffer[JsObject](),
  var aggregating:Boolean = false 
)
 
object LuceneActor {
  lazy val readHolder = LuceneActor.getHolder(writeEnabled = false)
  lazy val writeHolder = LuceneActor.getHolder(writeEnabled = true)
  case class Success(msg:String)
  case class DatesProcessed(msg:String, dates:Seq[String] = Seq[String]())
  case class Failure(msg:String)
  case class CommitRequest()
  case class CloseRequest()
  case class SearchRequest(query:Option[String], topic:String, from:Instant, to:Instant, max:Int, jsonnl:Boolean, caller:ActorRef)
  case class AggregateRequest(items:JsObject)
  case class AggregatedRequest(collection:String, topic:String, from:Instant, to:Instant, filters:Seq[(String, String)], jsonnl:Boolean, caller:ActorRef)
  case class AggregationRequest(
    query:Option[String], 
    from:Instant, 
    to:Instant, 
    columns:Seq[String], 
    groupBy:Seq[String], 
    filterBy:Seq[String], 
    sortBy:Seq[String], 
    sourceExpressions:Seq[String], 
    jsonnl:Boolean, 
    caller:ActorRef 
  )
  case class PeriodRequest(collection:String)
  case class PeriodResponse(first:Option[String], last:Option[String])
  def getHolder(writeEnabled:Boolean) = IndexHolder(writeEnabled = writeEnabled)
  def commit()(implicit holder:IndexHolder)  {
    commit(closeDirectory = true)
  }
  def commit(closeDirectory:Boolean= true)(implicit holder:IndexHolder)  {
    holder.dirs.synchronized {
      var now:Option[Long] = None 
      holder.dirs.foreach{ case (key, path) =>
          now = now.orElse(Some(System.nanoTime))
          val i = holder.indexes(key)
          if(i.writeEnabled && i.writer.get.isOpen) {
            i.writer.get.commit()
            i.writer.get.close()
          }
          i.reader.close()
          if(closeDirectory) {
            i.index.close()
          }
        case _ =>
      }
      holder.indexes.clear
      holder.dirs.clear
      now.map(n => println(s"commit done on ${(System.nanoTime - n) / 1000000000} secs"))
    }
  }
  def closeSparkSession()(implicit holder:IndexHolder) = {
    holder.spark match{
      case Some(spark) =>
        spark.stop()
      case _ =>
    }
  }

  def getSparkSession()(implicit conf:Settings, holder:IndexHolder) = {
    holder.dirs.synchronized {
      conf.load()
      if(holder.spark.isEmpty) {
        holder.spark =  Some(JavaBridge.getSparkSession(conf.sparkCores.getOrElse(0))) 
      }
    }
    holder.spark.get
  }
  def getIndex(collection:String, forInstant:Instant)(implicit conf:Settings, holder:IndexHolder):Driver = {
    getIndex(collection, getIndexKey(collection, forInstant))
  }
  def getIndex(collection:String, key:String)(implicit conf:Settings, holder:IndexHolder):Driver = {
    val path = s"$collection.$key"
    holder.dirs.synchronized {
      if(!holder.dirs.contains(path)) {
        conf.load()
        holder.dirs(path) = Paths.get(conf.home, "fs", collection,  key).toString()
      }
    }
    holder.dirs(path).synchronized {
      if(holder.indexes.get(path).isEmpty || !holder.indexes(path).isOpen) {
        holder.indexes(path) = Driver(holder.dirs(path), holder.writeEnabled)
      }
    }
    holder.indexes(path)
  }

  def getIndexKey(collection:String, forInstant:Instant) = {
    val timing = collection match {
      case "tweets" => "week"
      case "country_counts" => "week"
      case "topwords" => "week"
      case "geolocated" => "week"
      case _ => throw new Exception(s"unknown collection $collection")
    }
    timing match {
      case "week" =>
        Some(LocalDateTime.ofInstant(forInstant, ZoneOffset.UTC))
          .map(utc => s"${utc.get(IsoFields.WEEK_BASED_YEAR)*100 + utc.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)}")
          .map(s => s"${s.take(4)}.${s.substring(4)}")
          .get
      case _ => throw new Exception(s"Unknon grouping for collection $collection")
    }
  }    

  def getReadKeys(collection:String, from:Instant, to:Instant)(implicit conf:Settings, holder:IndexHolder) = 
     Range(0, ChronoUnit.DAYS.between(from, to.plus(1, ChronoUnit.DAYS)).toInt)
       .map(i => from.plus(i,  ChronoUnit.DAYS))
       .map(i => getIndexKey(collection, i))
       .distinct
       .filter(key => Paths.get(conf.home, "fs", collection ,key).toFile.exists())
  
  def getReadIndexes(collection:String, from:Instant, to:Instant)(implicit conf:Settings, holder:IndexHolder) = 
    getReadKeys(collection, from, to).map(key => getIndex(collection, key))

  /*def add2Aggregate(tweets:ArrayBuffer[JsObject])(implicit conf:Settings, holder:IndexHolder, ec: ExecutionContext) = {
    val toAggr = holder.toAggregate.synchronized { 
      holder.toAggregate ++= tweets
      if(!holder.aggregating) {
        println(s"Go Aggr!!!!!! ${holder.toAggregate.size}")
        holder.aggregating = true
        val r =  holder.toAggregate.clone
        holder.toAggregate.clear
        r
      } else {
        ArrayBuffer[JsObject]()
      }
    }
    
    Future{
      if(toAggr.size > 0) {
        implicit val spark = LuceneActor.getSparkSession()
        Files.list(Paths.get(conf.collectionPath)).iterator.asScala.foreach{p =>
           val content = Files.lines(p, StandardCharsets.UTF_8).iterator.asScala.mkString("\n")
           Some(EpiSerialisation.collectionFormat.read(JsonParser(content)))
            .map{
             case collection =>
               LuceneActor.aggregate(toAggr, collection)
           }
        } 
        holder.aggregating = false
      }
    }.onComplete{
       case scala.util.Success(_) => 
       case scala.util.Failure(t) =>
         holder.toAggregate.synchronized { 
           println(s"Error during aggregating: Retrying on next request ${t.getMessage} ${t.getStackTrace.mkString("\n")} ")
           holder.toAggregate ++= toAggr
           holder.aggregating = false
       }
    }
  }*/

  /*def aggregate(objects:ArrayBuffer[JsObject], collection:Collection)(implicit spark:SparkSession, conf:Settings) {
    import spark.implicits._
    val startTime = System.nanoTime
    
    implicit val holder = LuceneActor.readHolder
    val sc = spark.sparkContext
    val sorted = objects
      .flatMap{case objs => objs.map(o => (collection.getPK(o), LuceneActor.getIndexKey(collection.name, collection.getDate(o))))}
      .sortWith(_._3 < _._3)
    val home = conf.home
    val aggr = collection.aggregation
    val columns = aggr.columns.map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val groupBy = aggr.groupBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val filterBy = aggr.filterBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val sortBy = aggr.sortBy.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val sourceExp = aggr.sourceExpressions.getOrElse(Seq[String]()).map(v => aggr.params.map(qPars => qPars.foldLeft(v)((curr, iter) => curr.replace(s"@${iter._1}", iter._2))).getOrElse(v))
    val pks = collection.pks
    val collName = collection.name
    val dateCol = collection.dateCol
    Some(sc.parallelize(sorted))
      .map(rdd => rdd.repartition(conf.sparkCores.get))
      .map{rdd => rdd.mapPartitions{iter => 
        implicit val holder = LuceneActor.writeHolder
        implicit val conf = Settings(home)
        conf.load()
        var lastKey = ""
        var index:Driver=null 
        (for((pk, key) <- iter) yield {  
          if(lastKey != key) {
            index = LuceneActor.getIndex(collName, key)
            index.refreshReader
          }
          index.search(pk) match {
            case Some(s) => Some(s)
            case _ => 
              println(s"Cannot object to aggregatee $key")
              None
          }
        }).flatMap(ot => ot)
      }}
      .map(rdd => spark.read.schema(collection.schema))
      .map{
        case df if(sourceExp.size > 0) => 
          df.select(sourceExp.map(s => expr(s) ):_*)
        case df => df
      }
      .map{
        case df if(filterBy.size > 0) => 
          df.where(filterBy.map(w => expr(w)).reduce(_ && _))
        case df => df
      }
      .map{
        case df if(groupBy.size == 0) => 
          df.select(columns.map(c => expr(c)):_*) 
        case df => 
          df.groupBy(groupBy.map(gb => expr(gb)):_*)
            .agg(expr(columns.head), columns.drop(1).map(c => expr(c)):_*)
      }
      .map{
        case df if(sortBy.size == 0) => 
          df 
        case df =>
          df.orderBy(sortBy.map(ob => expr(ob)):_*)
      }
      .map{df =>
        //println(df.collect.size)
        df.mapPartitions{iter => 
          implicit val holder = LuceneActor.writeHolder
          implicit val conf = Settings(epiHome)
          conf.load()
          var lastKey = ""
          var index:Driver=null 
          (for(row <- iter) yield {
            val key =  Instant.parse(s"${row.getAs[String](dateCol)}T00:00:00.000Z")
            if(lastKey != key)
              index = LuceneActor.getIndex(collName, key)
            index.indexSparkRow(row = row, pk = pks, aggr = collection.aggr)
            1
          })
        }.as[Int].reduce(_ + _)
      }.map{numAggr =>
        val endTime = System.nanoTime
        println(s"${(endTime - startTime)/1e9d} secs for aggregating ${numAggr} tweets in ${collection.name}")
      }
  }*/
}

