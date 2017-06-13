import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;

/**
 * Created by Said on 5/30/2017.
 */
class SearchRequest {
    private static Logger logger = LogManager.getLogger(ElasticSearchDataAccess.class.getName());

    private TransportClient _client;

    private int _timeout;

    SearchRequest(TransportClient client) {
        logger.trace(Thread.currentThread().getStackTrace()[1].getMethodName());

        _client = client;
        _timeout = 60000;
    }


}
