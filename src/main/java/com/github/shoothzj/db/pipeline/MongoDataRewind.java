package com.github.shoothzj.db.pipeline;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.github.shoothzj.db.pipeline.module.DbInfoDto;
import com.github.shoothzj.db.pipeline.module.MongoInfoDto;
import com.github.shoothzj.db.pipeline.module.RewindTaskDto;
import com.github.shoothzj.db.pipeline.module.TransformDto;
import com.github.shoothzj.db.pipeline.util.MongoUtil;
import com.github.shoothzj.javatool.util.LogUtil;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.mongodb.BasicDBObject;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.gt;

/**
 * @author hezhangjian
 */
@Slf4j
public class MongoDataRewind {

    public static void main(String[] args) throws Exception {
        LogUtil.configureLog();
        final URL resourceUrl = Resources.getResource("rewind_sample.yaml");
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final List<RewindTaskDto> rewindTaskDtos = mapper.readValue(resourceUrl, new TypeReference<List<RewindTaskDto>>() {
        });
        log.info("rewind config dto is [{}]", rewindTaskDtos);
        //开始处理rewind task
        for (RewindTaskDto rewindTaskDto : rewindTaskDtos) {
            processRewindTask(rewindTaskDto);
        }
    }

    private static void processRewindTask(RewindTaskDto rewindTaskDto) {
        log.info("rewind task dto is [{}]", rewindTaskDto);
        final DbInfoDto dbInfo = rewindTaskDto.getDbInfo();
        if (dbInfo.getDbType().equals("mongodb")) {
            processRewindMongo(rewindTaskDto.getDbInfo().getMongoInfo(), rewindTaskDto.getTransform());
        } else {
            throw new IllegalArgumentException("Not supported db type yet");
        }
    }

    private static void processRewindMongo(MongoInfoDto mongoInfoDto, TransformDto transformDto) {
        final ConnectionString connectionString = new ConnectionString(mongoInfoDto.getConnectionStr());
        final MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString);
        builder.applyToConnectionPoolSettings(connectionPoolBuilder -> connectionPoolBuilder.maxSize(200));
        final MongoClientSettings mongoClientSettings = builder.build();

        final MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        final MongoDatabase database = mongoClient.getDatabase(mongoInfoDto.getDbName());
        final MongoCollection<Document> collection = database.getCollection(mongoInfoDto.getCollectionName());

        boolean first = true;
        Object cursor = null;
        long count = 0;

        final long startTime = System.currentTimeMillis();
        FindIterable<Document> documents;
        while (true) {
            if (first) {
                documents = collection.find().sort(new BasicDBObject("_id", 1)).limit(10);
                first = false;
            } else {
                documents = collection.find(gt("_id", cursor)).sort(new BasicDBObject("_id", 1)).limit(10);
            }
            final ArrayList<Document> arrayList = Lists.newArrayList(documents);
            if (arrayList.size() == 0) {
                break;
            }
            count += arrayList.size();
            for (Document document : arrayList) {
                MongoUtil.rewindDocument(collection, document, transformDto);
            }
            final Document document = arrayList.get(arrayList.size() - 1);
            cursor = document.get("_id");
            log.info("cursor is [{}]", cursor);
        }
        log.info("read count is [{}], cost is [{}]", count, System.currentTimeMillis() - startTime);
    }

}
