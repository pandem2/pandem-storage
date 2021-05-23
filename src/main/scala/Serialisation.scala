package eu.pandem.storage;

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import spray.json.{JsString, JsNull, JsValue, JsNumber, DefaultJsonProtocol, JsonFormat, RootJsonFormat, JsObject, JsArray, JsBoolean}
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.Locale 
import scala.util.Try
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField}
import org.apache.lucene.index.IndexableField
import scala.collection.JavaConverters._
import java.net.URLDecoder
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{StructField, StringType, IntegerType, FloatType, BooleanType, LongType, DoubleType}
import scala.collection.mutable.HashMap


case class Collection(
  name:String,
  dateCol:String,
  pks:Seq[String],
  aggr:Map[String, String],
  aggregation:Aggregation 
) {
  def getPK(obj:JsObject) = pks.map{pk =>
    obj.fields.get(pk).map{
      case JsNull => null
      case f => f.toString() 
    }.getOrElse(null)
  }.mkString("__")

  def getDate(obj:JsObject) =obj.fields.get(this.dateCol) match {
    case Some(JsString(v)) => Instant.parse(s"${v}T00:00:00.000Z")
    case _ => throw new Exception(s"getting date needs a non null instant string field, got ${obj.fields.get(this.dateCol).getOrElse("empty")} instead" )
  }
}

case class Aggregation(
  columns:Seq[String], 
  groupBy:Option[Seq[String]], 
  filterBy:Option[Seq[String]], 
  sortBy:Option[Seq[String]], 
  sourceExpressions:Option[Seq[String]], 
  params:Option[Map[String, String]],
)

case class Datasource(name:String, endpoint:String)
object Serialisation
  extends SprayJsonSupport
    with DefaultJsonProtocol {
    implicit val datasourceFormat = jsonFormat2(Datasource.apply)
    implicit object luceneDocFormat extends RootJsonFormat[Document] {
      def write(doc: Document) = writeField(fields = doc.getFields.asScala.toSeq)
      def writeField(fields:Seq[IndexableField], path:Option[String] = None):JsValue = {
        val (baseFields, childrenNames) = path match {
          case Some(p) => (
            fields.filter(f => f.name.startsWith(p) && !f.name.substring(p.size).contains(".")), 
            fields
              .map(_.name)
              .filter(n => n.startsWith(p) && n.substring(p.size).contains("."))
              .map(n => n.substring(p.size).split("\\.")(0))
              .distinct
          ) 
          case None => (
            fields.filter(f => !f.name.contains(".")), 
            fields
              .map(_.name)
              .filter(n => n.contains("."))
              .map(n => n.split("\\.")(0))
              .distinct

          )
        }
        JsObject(Map(
          (baseFields.map{f =>
            val fname = f.name.substring(path.getOrElse("").size)
            if(f.numericValue != null)
              (fname, f.numericValue match {
                case v:java.lang.Integer => JsNumber(v)
                case v:java.lang.Float => JsNumber(v)
                case v:java.lang.Double => JsNumber(v)
                case v:java.lang.Long => JsNumber(v)
                case v:java.math.BigDecimal => JsNumber(BigDecimal(v.toString))
                case _ => throw new NotImplementedError("I do not know how convert ${v.getClass.getName} into JsNumber")
              })
            else if(f.stringValue != null)
              (fname, JsString(f.stringValue))
            else if(f.binaryValue != null && f.binaryValue.length == 1)
              (fname, JsBoolean(f.binaryValue.bytes(0) == 1.toByte))
            else 
              throw new NotImplementedError(f"Serializing lucene field is only supported for boolean, number and string so far and got $f")
          } ++
            childrenNames.map(c => (c, writeField(fields = fields, path = Some(f"${path.map(p => p+".").getOrElse("")}$c."))))
          ):_*
        ))
      }
      def read(value: JsValue) = {
        throw new NotImplementedError(f"Deserializing lucene documebts is not supported") 
      }
    }
  def sparkRowDoc(row:Row, pk:Option[Seq[String]]=None, textFields:Set[String]=Set[String](), oldDoc:Option[Document], aggr:Map[String, String]) = {
    val doc = new Document()
    val pkVal = HashMap[String, String]()
    row.schema.fields.zipWithIndex.foreach{case (StructField(name, dataType, nullable, metadata), i) =>
      pk.map{pkFields =>
        if(pkFields.contains(name)) {
          if(!row.isNullAt(i))
            pkVal += ((name, row.get(i).toString))
        }
      }
      if(!row.isNullAt(i)) {
        dataType match {
          case StringType if !textFields.contains(name) =>  doc.add(new StringField(name, row.getAs[String](i), Field.Store.YES))
          case StringType if textFields.contains(name) =>  doc.add(new TextField(name, row.getAs[String](i), Field.Store.YES))
          case IntegerType => 
            doc.add(new IntPoint(name, applyIntAggregation(row.getAs[Int](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyIntAggregation(row.getAs[Int](i), name, aggr, oldDoc)))
          case BooleanType => 
            doc.add(new StringField(name, if(row.getAs[Boolean](i)) "true" else "false", Field.Store.NO)) 
            doc.add(new StoredField(name, Array(if(row.getAs[Boolean](i)) 1.toByte else 0.toByte )))  
          case LongType =>   
            doc.add(new LongPoint(name, applyLongAggregation(row.getAs[Long](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyLongAggregation(row.getAs[Long](i), name, aggr, oldDoc)))
          case DoubleType => 
            doc.add(new DoublePoint(name, applyDoubleAggregation(row.getAs[Double](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyDoubleAggregation(row.getAs[Double](i), name, aggr, oldDoc)))
          case FloatType =>
            doc.add(new FloatPoint(name, applyFloatAggregation(row.getAs[Float](i), name, aggr, oldDoc)))
            doc.add(new StoredField(name, applyFloatAggregation(row.getAs[Float](i), name, aggr, oldDoc)))
          case _ =>
            throw new NotImplementedError(f"Indexing automatic index of datatype $dataType is not supported")
           
        }
      }
    }
    pk.map{pkFields =>
      doc.add(new StringField(
        pkFields.mkString("_"), 
        pkFields.map(k => pkVal.get(k).getOrElse(null)).mkString("_"), 
        Field.Store.YES
      ))
    }
    doc
  }
  def applyIntAggregation(value:Int, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.intValue()
    else if(aggr.get(fieldName) == Some("avg"))  
      (value + oldDoc.get.getField(fieldName).numericValue.intValue())/2
    else value
  }
  def applyLongAggregation(value:Long, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.longValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.longValue())/2
    else value
  }
  def applyDoubleAggregation(value:Double, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(!oldDoc.isEmpty && oldDoc.get.getField(fieldName) == null)
      println(s"INFO: $fieldName is not in >>>>>>>>>>>>>>>>>>>>>>${oldDoc}")

    if(oldDoc.isEmpty || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.doubleValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.doubleValue())/2
    else value
  }
  def applyFloatAggregation(value:Float, fieldName:String, aggr:Map[String, String], oldDoc:Option[Document]) = {
    if(oldDoc.isEmpty  || oldDoc.get.getField(fieldName) == null || oldDoc.get.getField(fieldName).numericValue() == null)
      value
    else if(aggr.get(fieldName) == Some("sum"))
      value + oldDoc.get.getField(fieldName).numericValue.floatValue()
    else if(aggr.get(fieldName) == Some("avg")) 
      (value + oldDoc.get.getField(fieldName).numericValue.floatValue())/2
    else value
  }
}

