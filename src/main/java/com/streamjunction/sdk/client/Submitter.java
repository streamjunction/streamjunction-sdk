package com.streamjunction.sdk.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.ByteArrayBody;
import org.apache.http.entity.mime.content.InputStreamBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONValue;

import backtype.storm.Config;
import backtype.storm.generated.StormTopology;
import backtype.storm.generated.SubmitOptions;
import backtype.storm.utils.Utils;

public class Submitter {
	
	/**
	 * <code>streamjunction.topology.version</code> storm configuration property
	 * is used to prevent multiple clients from resubmiting the same topology
	 * version during startup. This field may be e.g. a Unix epoch timestamp. Topology
	 * versions with the greater version will override the newer ones.
	 */
	public static String SJ_TOPOLOGY_VERSION = "streamjunction.topology.version";
	
	private static String SJ_TOPOLOGY_NAME = "streamjunction.topology.name";
	
	/**
     * Submits a topology to run on the cluster. A topology runs forever or until 
     * explicitly killed.
     *
     * @param sjGateway the SJ gateway endpoint URL
     * @param name the name of the topology used to identify your topology within the project; do not confuse with the auto-generated storm-id
     * @param version the serial version of the topology 
     * @param stormConf the topology-specific configuration. See {@link Config}
     * @param inputMap the mapping of topologies and input streams
     * @param topology the processing to execute
     * @throws CannotPrepareRequestException if cannot prepare a request
     * @throws CannotExecuteRequestException if request execution has failed
     * @throws CannotSubmitTopologyException if gateway returned error
     */
    public static void submitTopology(
    		URI sjGateway,
    		String name,
    		long version,
    		Map stormConf,
    		Properties inputMap,
    		StormTopology topology) throws CannotPrepareRequestException, CannotExecuteRequestException, CannotSubmitTopologyException {
    	if (System.getProperty("storm.jar") != null) {
    		submitTopology(
    				null, 
    				sjGateway, 
    				name, 
    				version,
    				stormConf, 
    				inputMap,
    				topology, 
    				new File(System.getProperty("storm.jar")),
    				null);
    	}
    }
    
    /**
     * Submits a topology to run on the cluster. A topology runs forever or until 
     * explicitly killed.
     *
     * @param client Apache HTTP utils client, if <code>null</code> then the default one will be created
     * @param sjGateway the SJ gateway endpoint URL
     * @param name the name of the topology used to identify your topology within the project; do not confuse with the auto-generated storm-id
     * @param version the serial version of the topology
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param inputMap the mapping of topologies and input streams 
     * @param topology the processing to execute
     * @param topologyJar the topology jar file
     * @param opts options to manipulate the starting of the topology
     * @throws CannotPrepareRequestException if cannot prepare a request
     * @throws CannotExecuteRequestException if request execution has failed
     * @throws CannotSubmitTopologyException if gateway returned error
     */
    public static void submitTopology(
    		HttpClient client,
    		URI sjGateway, 
    		String name, 
    		long version,
    		Map stormConf,
    		Properties inputMap,
    		StormTopology topology, 
    		File topologyJar,
    		SubmitOptions opts) throws CannotPrepareRequestException, CannotExecuteRequestException, CannotSubmitTopologyException {
        if(!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }
        
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        stormConf.put(SJ_TOPOLOGY_NAME, name);
        stormConf.put(SJ_TOPOLOGY_VERSION, version);
        
        String serConf = JSONValue.toJSONString(stormConf);
        
        byte[] topologySer;
        
        try {
	        ByteArrayOutputStream topologyOut = new ByteArrayOutputStream();        
	        ObjectOutputStream oos = new ObjectOutputStream(topologyOut);
	        oos.writeObject(topology);
	        topologyOut.flush();
	        
	        topologySer = topologyOut.toByteArray();
        } catch (IOException e) {
        	throw new CannotPrepareRequestException("Cannot serialize topology", e);
        }
        
        // Not clear if we should allow submit opts...
        
        byte[] submitOptsSer;
        
        if (opts != null) {
	        try {
		        ByteArrayOutputStream submitOptsOut = new ByteArrayOutputStream();        
		        ObjectOutputStream oos = new ObjectOutputStream(submitOptsOut);
		        oos.writeObject(opts);
		        submitOptsOut.flush();
		        
		        submitOptsSer = submitOptsOut.toByteArray();
	        } catch (IOException e) {
	        	throw new CannotPrepareRequestException("Cannot serialize submit options", e);
	        }
        } else {
        	submitOptsSer = null;
        }
        
        byte[] inputMapSer;
        
        try {
        	ByteArrayOutputStream inputMapOut = new ByteArrayOutputStream();
        	inputMap.store(inputMapOut, "");
        	
        	inputMapSer = inputMapOut.toByteArray();
        } catch (IOException e) {
        	throw new CannotPrepareRequestException("Cannot serialize input mapping", e);
        }
        
        InputStream topologyIn;
		try {
			topologyIn = new FileInputStream(topologyJar);
		} catch (FileNotFoundException e) {
			throw new CannotPrepareRequestException("Cannot open topology jar", e);
		}
        
        boolean defaultClient = false;
        
        if (client == null) {
        	client = new DefaultHttpClient();
        	defaultClient = true;
        }
        
        try {
        	submitTopology(client, sjGateway, serConf.getBytes(), topologySer, submitOptsSer, inputMapSer, topologyIn);
        } catch (ClientProtocolException e) {
			throw new CannotExecuteRequestException("Cannot execute request", e);
		} catch (IOException e) {
			throw new CannotExecuteRequestException("Cannot execute request", e);
		} finally {
        	try {
				topologyIn.close();
			} catch (IOException e) {				
			}
        	
        	if (defaultClient) {
				client.getConnectionManager().shutdown();
        	}
        }
                
    }
    
    private static void submitTopology(
    		HttpClient httpClient,
    		URI sjGateway, 
    		byte[] conf, 
    		byte[] topology, 
    		byte[] opts, 
    		byte[] inputMap,
    		InputStream jar) throws CannotSubmitTopologyException, ClientProtocolException, IOException {
    	
    	HttpPost req = new HttpPost(sjGateway);
    	
    	MultipartEntity body = new MultipartEntity();    	
    	body.addPart("stormConfig.json", new ByteArrayBody(conf, "application/json", "stormConfig.json"));
    	body.addPart("topology.ser", new ByteArrayBody(topology, "application/octet-stream", "topology.ser"));
    	    	
    	if (opts != null) {
    		body.addPart("submitOpts.ser", new ByteArrayBody(opts, 
    				"application/octet-stream", "submitOpts.ser"));
    	}
    	body.addPart("input-map.properties", new ByteArrayBody(inputMap, "text/plain", "input-map.properties"));
    	body.addPart("topology.jar", new InputStreamBody(jar, "application/java-archive", "topology.jar"));
    	
    	String userInfo = sjGateway.getUserInfo();
    	if (userInfo != null) {
    		req.addHeader("authorization", "Basic "+new String(Base64.encodeBase64(userInfo.getBytes())));    		
    	} 
    	req.setEntity(body);    	
    	    			
    	HttpResponse res = httpClient.execute(req);
    	
    	StatusLine status = res.getStatusLine();
    	int statusCode = status.getStatusCode();
    	if (statusCode == 200 || statusCode == 204) {
    		// the topology has been created, updated or has already been 
    		// submitted before
    	} else {
    		throw new CannotSubmitTopologyException("Cannot submit topology", 
    				statusCode, status.getReasonPhrase(), res.getEntity());
    	}
    	
    }

    public static long getVersionFromResource(String resource) {
    	InputStream in = Submitter.class.getClassLoader().getResourceAsStream(resource);
    	if (in == null) {
    		throw new IllegalArgumentException("Cannot find resource: "+resource);
    	}
    	try {
    		BufferedReader r = new BufferedReader(new InputStreamReader(in));
	    	String l;
	    	while ((l=r.readLine()) != null) {
	    		return Long.parseLong(l);
	    	}
	    	throw new IllegalArgumentException("Cannot find version");
    	} catch (NumberFormatException e) {
			throw new IllegalArgumentException(e);
		} catch (IOException e) {
			throw new IllegalArgumentException(e);
		} finally {
    		try {
				in.close();
			} catch (IOException e) {				
			}
    	}
    }
}
