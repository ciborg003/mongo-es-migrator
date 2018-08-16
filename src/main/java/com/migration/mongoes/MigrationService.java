package com.migration.mongoes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.migration.mongoes.domain.DocumentMongo;
import com.migration.mongoes.helper.ElasticsearchTemplate;
import com.migration.mongoes.util.FileService;
import com.migration.mongoes.util.UtilService;
import com.mongodb.client.MongoCollection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


@Service
public class MigrationService {

    private static final Logger LOGGER = LogManager.getLogger(MigrationService.class);

    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @Value("${mongo.collection}")
    private String mongoCollection;
    @Value("${mongo.batch_size}")
    private int batchSize;
    @Value("${util.file_for_db}")
    private String fileForDB;
    @Value("${org.elasticsearch.index}")
    private String index;
    @Value("${org.elasticsearch.type}")
    private String type;

    private ObjectId lastObjectId = null;

    public void start() {
        MongoCollection collection = mongoTemplate.getCollection(mongoCollection);
        int skip = 0;
        LOGGER.info("Migration Started");


        while (true) {
            try {
                List<DocumentMongo> res = mongoTemplate.find(getLastPushedObjectId(), DocumentMongo.class);

                List<ElasticsearchTemplate.IndexQuery> indexQueries = new ArrayList<>(batchSize);

                res.forEach(document -> {
                    indexQueries.add(buildIndexQuery(document));
                });

                if (res.size() == 0) {
                    break;
                }
                elasticsearchTemplate.bulkIndex(indexQueries);

                skip += res.size();
                lastObjectId =  new ObjectId("" + res.get(res.size()- 1).get("mongo_id"));
                LOGGER.info("Pushed " + skip + " records");
                FileService.rewriteToFileValue(fileForDB, lastObjectId.toHexString());
            } catch (Exception e) {
                LOGGER.warn("Exception in migration, last pushed value with _id: " + lastObjectId, e);
            }
        }

    }

    /**
     * methods return _id of last pushed object
     * @return
     */
    private Query getLastPushedObjectId() {
        if (lastObjectId != null) {
            return new Query()
                    .addCriteria(Criteria.where("_id").gt(lastObjectId))
                    .with(Sort.by(Sort.Direction.ASC, "_id"))
                    .limit(batchSize);
        } else {
            String id = FileService.getValueFromFile(fileForDB);

            if (id != null && ObjectId.isValid(id)) {
                return new Query()
                        .addCriteria(Criteria.where("_id").gt(new ObjectId(id)))
                        .with(Sort.by(Sort.Direction.ASC, "_id"))
                        .limit(batchSize);
            }
        }

        return new Query()
                .with(Sort.by(Sort.Direction.ASC, "_id"))
                .limit(batchSize);
    }

    private ElasticsearchTemplate.IndexQuery buildIndexQuery(DocumentMongo document) {

        validateDocument(document);


        ObjectId obj = null;
        if (document.get("_id") instanceof String) {
            obj = new ObjectId("" + document.remove("_id"));
        } else {
            obj = (ObjectId) document.remove("_id");
        }

        lastObjectId = obj;
        if (obj != null) {
            document.put("mongo_id", obj.toHexString());
        }

        return new ElasticsearchTemplate.IndexQueryBuilder()
                .withIndexName(index)
                .withObject(document)
                .withType(type)
                .build();
    }

    private void validateDocument(DocumentMongo document) {
        Map<String,Object> value = (Map<String, Object>) document.get("value");
        if (value != null) {
            Map<String,Object> metadata = (Map<String, Object>) value.get("records_metadata");
            if (metadata != null) {
                Date newDate = validateDate(
                        metadata.get("created_at"));

                if (newDate != null) {
                    metadata.put("created_at", newDate);
                }
            }
        }
    }

    private Date validateDate(Object date) {
        if (date instanceof String ) {
            SimpleDateFormat format = null;
            if (((String) date).matches("([A-Z][a-z]{2}) ([0-9]|[1-2]\\d|3[0-1]), (\\d{4}) ([0-9]|1[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9] (PM|AM)")) {
                format = new SimpleDateFormat("MMM dd, yyyy, hh:mm:ss a");
            } else if (((String) date).matches("(\\d{4})-(0\\d|1[0-2])-([0-2]\\d|3[0-1]) ([0-1]\\d|2[0-3]):([0-5]\\d):([0-5]\\d) [A-Z]{3}")) {
                format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss ");
            } else if (((String) date).matches("([0]\\d|1[0-2])\\/([0-2]\\d|3[0-1])\\/\\d{4} (0\\d|1[0-2]):([0-5]\\d|1[0-2]):([0-5]\\d|1[0-2]) (AM|PM)")) {
                format = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
            }

            if (format != null) {
                try {
                    return format.parse((String) date);
                } catch (ParseException e) {
                    LOGGER.warn("Date parse exception, date: " + date);
                }
            }
        }

        return null;
    }
}
