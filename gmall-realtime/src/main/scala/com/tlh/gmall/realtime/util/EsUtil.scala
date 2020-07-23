package com.tlh.gmall.realtime.util

import java.text.SimpleDateFormat
import java.util.Date

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/22 14:48
 * @Version 1.0
 */
object EsUtil {

    private var jestClientFactory : JestClientFactory = null;

    def getJestClient:JestClient = {
        if(jestClientFactory==null) build()
        jestClientFactory.getObject
    }

    def  build(): Unit ={
        jestClientFactory = new JestClientFactory
        jestClientFactory.setHttpClientConfig(new HttpClientConfig.Builder("http://hadoop102:9200" )
          .multiThreaded(true)
          .maxTotalConnection(20)
          .connTimeout(10000)
          .readTimeout(1000).build())
    }

    def putIndex(args: Array[String]): Unit = {

        val jest: JestClient = getJestClient
        val actorList=new java.util.ArrayList[String]()
        actorList.add("tom")
        actorList.add("Jack")
        val movieTest = MovieTest("102","中途岛之战",actorList)
        val datestring: String = new SimpleDateFormat("yyyyMMdd").format(new Date)
        val index: Index = new Index.Builder(movieTest).index("movie_index"+datestring).`type`("_doc").id(movieTest.id).build()

        jest.execute(index)
        jest.close()

    }

    def getIndex(args: Array[String]): Unit = {
        val jest: JestClient = getJestClient
        val searchBuilder = new SearchSourceBuilder
        val boolbuilder = new BoolQueryBuilder()
        boolbuilder.should(new MatchQueryBuilder("name","red sea"))
          .filter(new RangeQueryBuilder("doubanScore").gte(3))
        searchBuilder.query(boolbuilder);
        searchBuilder.sort("doubanScore",SortOrder.ASC)
        searchBuilder.from(0)
        searchBuilder.size(20)
        searchBuilder.highlight(new HighlightBuilder().field("name"))
        val query2: String = searchBuilder.toString
        println(query2)
        val search: Search = new Search.Builder(query2).
          addIndex("movie_index").
          addType("movie").build()
        val result: SearchResult = jest.execute(search)
        val hitList: java.util.List[SearchResult#Hit[java.util.Map[String, Any], Void]] =
            result.getHits(classOf[java.util.Map[String ,Any]])
        var resultList =new  ListBuffer[java.util.Map[String, Any]].asJava;
        for ( hit <- hitList.asScala ) {
            val source: java.util.Map[String, Any] = hit.source
            resultList.add(source)
        }
        println(resultList.asScala.mkString("\n"))
        jest.close()

    }

    def main(args: Array[String]): Unit = {
        getIndex(args)
    }


    case class MovieTest(id:String ,movie_name:String, actionNameList: java.util.List[String] ){

    }

}
