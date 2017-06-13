import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;


/**
 * Created by Said on 5/29/2017.
 */
class BulkRequest {

    private static Logger logger = LogManager.getLogger(ElasticSearchDataAccess.class.getName());

    /* Hold a reference to the TransportClient. This class should only
     * be instantiated by ElasticSearchDataAccess Class  */
    private BulkRequestBuilder _bulkRequest;
    private TransportClient _client;
    //private BulkProcessor _bulkProcessor;

    BulkRequest(TransportClient client)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        _client = client;
        _bulkRequest = _client.prepareBulk();
        //_bulkProcessor = BulkProcessor.builder(_client, new )
    }

    void AddUpsertRequest(String indexName, String typeName, String id, String json)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        IndexRequest indexRequest = new IndexRequest(indexName, typeName, id)
                .source(json);

        _bulkRequest.add(
                _client.prepareUpdate(indexName, typeName, id)
                        .setDoc(json)
                        .setUpsert(indexRequest)
        );
    }

    void AddDeleteRequest(String indexName, String typeName, String id)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        _bulkRequest.add(_client.prepareDelete(indexName, typeName, id));
    }

    BulkResponse ExecuteBulkRequest()
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        BulkResponse bulkResponse = _bulkRequest.get();

        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse) {
                logger.error("Error executing bulk request at id: " + item.getId());
            }
        }

        return bulkResponse;
    }

}
