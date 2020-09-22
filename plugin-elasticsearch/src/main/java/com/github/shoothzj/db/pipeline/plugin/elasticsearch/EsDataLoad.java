package com.github.shoothzj.db.pipeline.plugin.elasticsearch;

import com.github.shoothzj.db.pipeline.api.exchange.AbstractExchange;
import com.github.shoothzj.db.pipeline.api.exchange.MapExchange;
import com.github.shoothzj.db.pipeline.api.module.db.EsInfoDto;
import com.github.shoothzj.db.pipeline.core.AbstractLoad;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class EsDataLoad extends AbstractLoad<EsInfoDto> {

    private RestHighLevelClient client;

    private String indexName;

    @Override
    public void init(EsInfoDto esInfoDto) {
        final HttpHost httpHost = new HttpHost(esInfoDto.getEsHost(), esInfoDto.getEsPort(), esInfoDto.getSchema());
        client = new RestHighLevelClient(RestClient.builder(httpHost));
        indexName = esInfoDto.getIndexName();
    }

    @Override
    public boolean load(MapExchange mapExchange) {
        //todo 嵌套需要测试解决
        try {
            final XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
            mapExchange.traverse((name, abstractExchange) -> putXContent(xContentBuilder, name, abstractExchange));
            xContentBuilder.endObject();

            final IndexRequest indexRequest = new IndexRequest("test");
            indexRequest.source(xContentBuilder);
            final IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            return DocWriteResponse.Result.CREATED.equals(indexResponse.getResult());
        } catch (Exception e) {
            log.error("load data to es exception ", e);
            return false;
        }
    }

    private void putXContent(XContentBuilder xContentBuilder, String name, AbstractExchange abstractExchange) {
        //todo put XContent
    }

    @Override
    public boolean load(List<MapExchange> mapExchanges) {
        return false;
    }
}
