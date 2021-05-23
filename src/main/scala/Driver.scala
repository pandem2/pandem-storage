package eu.pandem.storage;

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.search.{IndexSearcher, Query, TermQuery}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.ScoreDoc
import java.nio.file.{Files, Paths, Path}
import org.apache.lucene.store.{ FSDirectory, MMapDirectory}
import org.apache.lucene.index.{IndexWriter, IndexReader, DirectoryReader,IndexWriterConfig}
import org.apache.lucene.analysis.Analyzer
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField, DoubleDocValuesField, BinaryPoint}
import org.apache.lucene.index.Term
import org.apache.lucene.search.{TopDocs, BooleanQuery}
import org.apache.lucene.search.BooleanClause.Occur
import scala.util.{Try,Success,Failure}
import scala.collection.JavaConverters._
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{StructField, StringType, IntegerType, FloatType, BooleanType, LongType, DoubleType}

object Driver {
  def apply(indexPath:String, writeEnabled:Boolean=false):Driver = {
    val analyzer = new StandardAnalyzer()
    val indexDir = Files.createDirectories(Paths.get(s"${indexPath}"))
    val index = new MMapDirectory(indexDir, org.apache.lucene.store.SimpleFSLockFactory.INSTANCE)
    val config = new IndexWriterConfig(analyzer);
    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
    val writer = 
      if(writeEnabled)
        Some(new IndexWriter(index, config))
      else
        None
    //make the index near real time
    val reader = 
      if(writeEnabled)
        DirectoryReader.open(writer.get,true, true)
      else {
        DirectoryReader.open(index)
      }
    val searcher = new IndexSearcher(reader)
    Driver(reader = reader, writer = writer, searcher = searcher, index = index, writeEnabled)
  }
}


case class Driver(var reader:IndexReader, writer:Option[IndexWriter], var searcher:IndexSearcher, index:FSDirectory, writeEnabled:Boolean){
  def isOpen = {
    if(writeEnabled)
      this.writer.get.isOpen
    else
      true
  }
  def refreshReader()  = {
    this.synchronized {
      val oldReader = this.reader
      try{
        val newReader = 
          if(writeEnabled){
            DirectoryReader.open(this.writer.get,true, true)
          } else {
            DirectoryReader.open(this.index)
          }
        this.reader = newReader
        this.searcher = new IndexSearcher(this.reader)
      } 
      finally {
        oldReader.decRef()
      }
    }
    this
  }


  def indexSparkRow(row:Row, pk:Seq[String], textFields:Set[String]=Set[String](), aggr:Map[String, String] =Map[String, String]() ) = {
    val oldDoc = searchRow(row, pk)
    val doc = Serialisation.sparkRowDoc(row = row, pk = Some(pk), textFields = textFields, oldDoc = oldDoc, aggr = aggr)
    if(!writeEnabled)
       throw new Exception("Cannot index on a read only index")
    if(pk.isEmpty)
      throw new Exception(s"Indexing spark rows without primary key definition is not supported")
    
    val pkTerm = new Term(pk.mkString("_"), pk.map(k => if(doc.getField(k) == null) null else doc.getField(k).stringValue).mkString("_"))
    this.writer.get.updateDocument(pkTerm, doc)
  }


  def searchRow(row:Row, pk:Seq[String]) = {
    assert(pk.size > 0)
    implicit val searcher = this.useSearcher()
    Try{
      val qb = new BooleanQuery.Builder()
      row.schema.fields.zipWithIndex
        .filter{case (StructField(name, dataType, nullable, metadata), i) => pk.contains(name)}
        .foreach{case (StructField(name, dataType, nullable, metadata), i) =>
        if(!row.isNullAt(i)) {//throw new Exception("cannot search a row on a null PK value")
        dataType match {
          case StringType => qb.add(new TermQuery(new Term(name, row.getAs[String](i))), Occur.MUST)
          case IntegerType => qb.add(IntPoint.newExactQuery(name, row.getAs[Int](i)), Occur.MUST)
          case BooleanType => qb.add(BinaryPoint.newExactQuery(name, Array(row.getAs[Boolean](i) match {case true => 1.toByte case _ => 0.toByte})), Occur.MUST)
          case LongType => qb.add(LongPoint.newExactQuery(name, row.getAs[Long](i)), Occur.MUST)
          case FloatType => qb.add(FloatPoint.newExactQuery(name, row.getAs[Float](i)), Occur.MUST)
          case DoubleType => qb.add(DoublePoint.newExactQuery(name, row.getAs[Double](i)), Occur.MUST)
          case dt => throw new Exception(s"Spark type {$dt.typeName} cannot be used as a filter since it has not been indexed")
         }
        }
      }

      val res = search(qb.build, maxHits = 1)
      if(res.scoreDocs.size == 0) {
          None
      } else {
         Some(searcher.getIndexReader.document(res.scoreDocs(0).doc))
      }
    } match {
      case Success(r) =>
        this.releaseSearcher(searcher)
        r
      case Failure(e) =>
        this.releaseSearcher(searcher)
        throw e
    }
  }

  def parseQuery(query:String) = {
    val analyzer = new StandardAnalyzer()
    val parser = new QueryParser("text", analyzer)
    parser.parse(query)
  }
  def search(query:String)(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, 100, None) 
  }
  def search(query:String, maxHits:Int)(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, maxHits, None) 
  }
  def search(query:String, after:Option[ScoreDoc])(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, 100, after) 
  }
  def search(query:String, maxHits:Int, after:Option[ScoreDoc])(implicit searcher:IndexSearcher):TopDocs  = {
    search(parseQuery(query), maxHits, after) 
  }
  def search(query:Query)(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, 100, None) 
  }
  def search(query:Query, maxHits:Int)(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, maxHits, None) 
  }
  def search(query:Query, after:Option[ScoreDoc])(implicit searcher:IndexSearcher):TopDocs  = {
    search(query, 100, after) 
  }
  def search(query:Query, maxHits:Int, after:Option[ScoreDoc])(implicit searcher:IndexSearcher):TopDocs  = {
    val docs = after match {
      case Some(last) => searcher.searchAfter(last, query, maxHits)
      case None => searcher.search(query, maxHits)
    }
    docs
  } 
  def searchAll(query:String):Iterator[Document]  = {
    searchAll(parseQuery(query))
  }
  def searchAll(query:Query):Iterator[Document]  = {
    var after:Option[ScoreDoc] = None
    var first = true
    var searcher:Option[IndexSearcher]=None
    Iterator.continually{
      if(first) {
        searcher = Some(this.useSearcher()) 
        first = false
      }
      val res = search(query = query, after = after)(searcher.get)
      val ret = res.scoreDocs.map(doc => searcher.get.getIndexReader.document(doc.doc))
      if(res.scoreDocs.size > 0)
        after = Some(res.scoreDocs.last)
      else {
        this.releaseSearcher(searcher.get)
      }
      ret
    }.takeWhile(_.size > 0)
      .flatMap(docs => docs)
  }

  def useSearcher() = {
    this.synchronized {
      val searcher = this.searcher
      val reader = searcher.getIndexReader
      reader.incRef
      searcher
    }
  }

  def releaseSearcher(searcher:IndexSearcher) {
    this.synchronized {
      searcher.getIndexReader.decRef()
    }
  }
}
