import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkIndexByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * Created by Said on 1/22/2017.
 * Default access is limited to package
 */
// ToDo: Rename to ElasticConnection
class ElasticSearchDataAccess {

    private static Logger logger = LogManager.getLogger(ElasticSearchDataAccess.class.getName());

    // ToDo: Refactor out Search / Insert / Update / Delete.. Return client as global?
    // ToDo: Get your braces in order.
    private String _clusterName;
    private List<String> _nodeNames = new ArrayList<String>();
    private BulkRequest _bulkRequest = null;
    private int _numOfNodes;

    private TransportClient _client;
    TransportClient GetClient()
    {
        return _client;
    }

    ElasticSearchDataAccess(String clusterName, List<String> nodeNames) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
        _clusterName = clusterName;
        _nodeNames = nodeNames;
        _numOfNodes = nodeNames.size();
    }

    void ConnectToCluster() throws UnknownHostException {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        //ToDo: Install x-pack and authentic users
        Settings settings = Settings.builder()
                .put("cluster.name", _clusterName)
                .put("client.transport.sniff", true)    //dynamically add new hosts and remove old ones
                .put("client.transport.ping_timeout", "10s")
                .build();

        _client = new PreBuiltTransportClient(settings);
        for(String node: _nodeNames) {
            _client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(node), 9300));
            logger.info(String.format("cluster name: %1$20s%nnode: %2$18s", _clusterName, node));
        }

        _numOfNodes = _client.listedNodes().size();
    }

    void CloseConnection() {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
        _client.close();
        logger.info(String.format("closed connection to: %1$s", _clusterName));
    }

    String InsertDocument(String indexName, String typeName, String jsonDoc) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        IndexResponse response = _client.prepareIndex(indexName, typeName)
                .setSource(jsonDoc)
                .get();

        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        RestStatus restStatus = response.status();

        logger.info(String.format("index: %1$13s%ntype: %2$13s%nId: %3$31s%nversion: %4$7s%nrest status: %5$9s",
                index, type, id, version, restStatus));

        return id;
    }

    Map<String, Object> GetDocument(String indexName, String typeName, String id) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        GetResponse response = _client.prepareGet(indexName, typeName, id).get();
        logger.info(String.format("source json: %1$70s", response.getSource().toString()));

        return response.getSource();
    }

    RestStatus DeleteDocument(String indexName, String typeName, String id) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        DeleteResponse response = _client.prepareDelete(indexName, typeName, id).get();
        logger.info(String.format("Deleted document: %1$20s", response.getId()));
        logger.info(response.status());

        return response.status();
    }

    long DeleteByQuery (String indexName, String field, String matchText) {
        /* It is possible to make this operation asynchronous */
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        BulkIndexByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(_client)
                        .filter(QueryBuilders.matchQuery(field, matchText))
                        .source(indexName)
                        .get();
        logger.info(String.format("No. Deleted documents: %1$20s", response.getDeleted()));

        return response.getDeleted();
    }

    void UpdateDocumentById(String indexName, String typeName, String id, String updateJson) throws ExecutionException, InterruptedException {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                .doc(updateJson);

        _client.update(updateRequest).get();
    }

    void UpsertDocument(String indexName, String typeName, String id, String updateJson) throws ExecutionException, InterruptedException {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        /* If the document does not exist, the content of the upsert element
         * will be used to index the fresh document. */

        IndexRequest indexRequest = new IndexRequest(indexName, typeName, id)
                .source(updateJson);

        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, id)
                .doc(updateJson)
                .upsert(indexRequest);

        _client.update(updateRequest).get();
    }

    String[] GetMultipleDocuments(String indexName, String typeName, String idList) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        String[] ids = idList.split(",|;");
        String[] documents = new String[ids.length];
        int documentsIndex = 0;

        MultiGetResponse multiGetItemResponses = _client.prepareMultiGet()
                .add(indexName, typeName, ids)
                .get();

        for (MultiGetItemResponse itemResponse : multiGetItemResponses) {
            GetResponse response = itemResponse.getResponse();
            if (response.isExists()) {
                documents[documentsIndex] = response.getSourceAsString();
                documentsIndex++;
            }
        }

        return documents;
    }

    public void PrepareBulkUpsertRequest(String indexName, String typeName, String id, String json) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        if (_bulkRequest == null) {
            _bulkRequest = new BulkRequest(_client);
        }

        _bulkRequest.AddUpsertRequest(indexName, typeName, id, json);
    }

    public void PrepareBulkDeleteRequest(String indexName, String typeName, String id) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        if (_bulkRequest == null) {
            _bulkRequest = new BulkRequest(_client);
        }

        _bulkRequest.AddDeleteRequest(indexName, typeName, id);
    }

    public String ExecuteBulkRequest() {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        BulkResponse response = null;

        if (_bulkRequest != null) {
            response = _bulkRequest.ExecuteBulkRequest();
            _bulkRequest = null;
        }

        return response.toString();
    }
}
