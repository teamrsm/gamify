import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.transport.TransportClient;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Said on 6/12/2017.
 */
public class ElasticFacade {
    // static members
    private static Logger logger = LogManager.getLogger(ElasticSearchDataAccess.class.getName());

    // private members
    private JsonJumbalay _json;
    private TransportClient _client;

    private ElasticSearchDataAccess esda;
    private BulkRequest _bulkRequest;
    private SearchRequest _searchRequest;

    // public members
    private String _clusterName;
    public String GetClusterName() { return this._clusterName; }
    public void SetClusterName(String clusterName) { this._clusterName = clusterName; }

    private List<String> _nodeNames;
    public void SetNodeNames (List<String> nodeNames)
    {
        if (this._nodeNames == null)
        {
            this._nodeNames = new ArrayList<String>();
        }

        this._nodeNames = nodeNames;
    }

    public ElasticFacade()
    {
        // instantiate members and properties
        _json = new JsonJumbalay();
        esda = new ElasticSearchDataAccess(this._clusterName, _nodeNames);

        try
        {
            /* Connecting to the cluster populates the transport client
            *  so long as there is not a failure */
            esda.ConnectToCluster();
            _client = esda.GetClient();
            _bulkRequest = new BulkRequest(_client);
            _searchRequest = new SearchRequest(_client);
            esda.CloseConnection();
        }
        catch (UnknownHostException uhe)
        {
            logger.log(Level.ERROR, "Invalid connection attribute: " + uhe.getMessage());
        }
    }


}
