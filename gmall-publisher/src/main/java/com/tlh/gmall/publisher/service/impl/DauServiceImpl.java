package com.tlh.gmall.publisher.service.impl;

import com.tlh.gmall.publisher.service.DauService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * @Comment
 * @Author: tlh
 * @Date 2020/7/24 10:39
 * @Version 1.0
 */
@Service
@Slf4j
public class DauServiceImpl implements DauService {

    @Autowired
    private JestClient jestClient;

    private static String indexPrefix = "gmall_dau_info_";

    private static String indexSuffix = "_query";

    @Override
    public Long getDauTotal(String date) {
        String indexName = indexPrefix + date.replace("-", "") + indexSuffix;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        Search search = new Search.Builder(searchSourceBuilder.toString()).
                addIndex(indexName).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            return searchResult.getTotal();
        }catch (Exception e){
           log.info("es 查询异常{}",e);
           throw new RuntimeException("es 查询异常");
        }
    }

    @Override
    public Map getDauHourCount(String date) {
        String indexName = indexPrefix + date.replace("-", "") + indexSuffix;
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsBuilder termsBuilder = AggregationBuilders.
                terms("groupby_hour").field("hr").size(24);
        log.info(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).
                addIndex(indexName).addType("_doc").build();
            try {
                SearchResult searchResult = jestClient.execute(search);
                Map resultMap = new HashMap();
                TermsAggregation termsAggregation = searchResult.getAggregations().getTermsAggregation("groupby_hour");
                if (termsAggregation!=null){
                    List<TermsAggregation.Entry> buckets = termsAggregation.getBuckets();
                    for (TermsAggregation.Entry bucket : buckets) {
                      resultMap.put(bucket.getKey(), bucket.getCount());
                    }
                }
                return resultMap;
            } catch (IOException e) {
                log.info("es 查询异常{}",e);
                throw new RuntimeException("es 查询异常");
            }
    }
}
