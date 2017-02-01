import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Said on 1/22/2017.
 */
public class ElasticSearchDataAccess {

    private static Logger logger = LogManager.getLogger(ElasticSearchDataAccess.class.getName());

    private String _clusterName;
    private List<String> _nodeNames = new ArrayList<String>();
    private TransportClient _client;

    public ElasticSearchDataAccess(String clusterName, List<String> nodeNames)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
        _clusterName = clusterName;
        _nodeNames = nodeNames;
    }

    public void ConnectToCluster() throws UnknownHostException {
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
    }

    public void CloseConnection()
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());
        _client.close();
        logger.info(String.format("closed connection to: %1$s", _clusterName));
    }

    public RestStatus InsertDocument(String indexName, String typeName, String jsonDoc)
    {
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

        return restStatus;
    }

    public Map<String, Object> GetDocument(String indexName, String typeName, String id)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        GetResponse response = _client.prepareGet(indexName, typeName, id).get();
        logger.info(String.format("source json: %1$70s", response.getSource().toString()));

        return response.getSource();
    }
}
