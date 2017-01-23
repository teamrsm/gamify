import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

    public RestStatus InsertDocument(String indexName, String jsonDoc)
    {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        IndexResponse response = _client.prepareIndex(indexName, "test")
                .setSource(jsonDoc)
                .get();

        String index = response.getIndex();
        String type = response.getType();
        String id = response.getId();
        long version = response.getVersion();
        RestStatus restStatus = response.status();

        logger.info(String.format("index: %1$20s%ntype: %2$20s%nId: %3$15s%nversion: %4$15s%nrest status: %5$20s",
                index, type, id, version, restStatus));

        return restStatus;
    }
}
