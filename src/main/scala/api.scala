package eu.pandem.storage;

import akka.actor.{ActorSystem, Actor, Props}
import akka.stream.ActorMaterializer
import akka.pattern.{ask, pipe}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{HttpEntity, ContentType, ContentTypes, HttpResponse}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.model.{StatusCodes, StatusCode}
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.util.{Timeout}
import spray.json.{JsValue}
import scala.concurrent.duration._
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._
import akka.actor.ActorRef
import akka.Done
import akka.actor.ActorRef
import akka.stream.OverflowStrategy
import akka.stream.CompletionStrategy
import akka.stream.scaladsl._
import akka.http.scaladsl.model.HttpEntity.{Chunked, Strict}
import akka.util.ByteString
import java.time.Instant
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import spray.json.JsonParser

object API {
  var actorSystemPointer:Option[ActorSystem] = None
  var oLuceneRunner:Option[ActorRef] = None
  var oConf:Option[Settings] = None
  def main(args: Array[String]) {
    if(args.size != 2)
      println("pandem file systme API receives a sigle paramater which is the endpoint to get its configuration")
    else {
      val pandemHome = args(1)
      run(pandemHome)
    } 
      
  }
  def run(pandemHome:String) {
    import Serialisation._
    implicit val actorSystem = ActorSystem("pandem-storage")
    actorSystemPointer = Some(actorSystem)
    implicit val executionContext = actorSystem.dispatcher
    implicit val conf = Settings(pandemHome)
    conf.load
    oConf = Some(conf)
    val luceneRunner = actorSystem.actorOf(Props(classOf[LuceneActor], conf))
    oLuceneRunner = Some(luceneRunner)
    removeLockFiles()
    val route =
      extractUri { uri =>
        concat(
          pathEndOrSingleSlash {
            complete("Hello! this is the pandem storage endpoint!")
          }
        )
      }

    Http().newServerAt("localhost", conf.fsPort).bind(route)
  }
  def completeTry(tryed:Try[ToResponseMarshallable], uri:String) = {
    tryed match {
      case Success(entity) =>
        complete(entity)
      case Failure(e) =>
        complete(defaultException(e, uri))
    }
  }
  def defaultException(e:Throwable, uri:String) = {
    val message = s"Request to $uri could not be handled normally" + "\n" + e.toString() + "\n" + e.getStackTrace.mkString("\n")
    println(message)
    HttpResponse(StatusCodes.InternalServerError, entity = message)
  }
  def stop() {
    oLuceneRunner.map{lr =>
      implicit val timeout: Timeout =oConf.get.fsQueryTimeout.seconds //For ask property
      implicit val executionContext = actorSystemPointer.get.dispatcher
      (lr ? LuceneActor.CloseRequest)
        .onComplete{_ => 
          actorSystemPointer.map(as => as.terminate)
          actorSystemPointer = None
          oLuceneRunner = None
      }
    }
  }
  def removeLockFiles()(implicit conf:Settings) { 
    if(Files.exists(Paths.get(conf.fsRoot)))
      Files.walk(Paths.get(conf.fsRoot))
        .iterator.asScala.foreach{p =>
          if(p.endsWith("write.lock"))
            Files.delete(p)
        }
  }
} 
