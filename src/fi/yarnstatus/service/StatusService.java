package fi.yarnstatus.service;

import java.net.UnknownHostException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Path("/status")
public class StatusService {
	

	 @SuppressWarnings("unchecked")
	 @GET
	 @Produces("application/json")
	 public Response getStatus() throws ParseException, UnknownHostException {

		 Client client = Client.create();

		 WebResource webResource = client
				 //.resource("http://localhost:8080/YarnStatus/rest/status/dummy");
		 		 .resource("http://localhost:8088/ws/v1/cluster/metrics");

		 ClientResponse response = webResource.accept("application/json")
				 .get(ClientResponse.class);

		 if (response.getStatus() != 200) {
			 throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		
		 	}

		 String output = response.getEntity(String.class);
		 JSONObject output_obj = (JSONObject)new JSONParser().parse(output);
		 
		 JSONObject clusterMetricsObj = (JSONObject)output_obj.get("clusterMetrics");
		 
		 JSONObject obj = new JSONObject();
		 
		 obj.put("Total Applications currently running: ", clusterMetricsObj.get("appsRunning"));
		 obj.put("Total Applications currently pending: ", clusterMetricsObj.get("appsPending"));
		 double avail_mb = ((Long)clusterMetricsObj.get("availableMB"));
		 double alloc_mb = ((Long)clusterMetricsObj.get("allocatedMB"));
		 
		 
		 double percent_resource_double = (alloc_mb / (alloc_mb + avail_mb))*100;
		 long percent_resource = (long)percent_resource_double;
		 
		 if(percent_resource > 89)
			 percent_resource = 100;
		 
		 obj.put("Resources Used (in percent)", percent_resource);
		 obj.put("Resources Available (in percent)", 100 - percent_resource);
		 
		 
		 String result = obj.toString();
		 return Response.status(200).entity(result).build();
	  }
	 
	 
	 @SuppressWarnings("unchecked")
	 @GET
	 @Path("/dummy")
	 @Produces("application/json")
	 public Response dummyService() {

		JSONObject clusterMetrics = new JSONObject();
		JSONObject jsonObject = new JSONObject();
		
		jsonObject.put("availableMB", 50);
		jsonObject.put("allocatedMB", 90);
		jsonObject.put("appsRunning", 5);
		jsonObject.put("appsPending", 1);
	
		clusterMetrics.put("clusterMetrics", jsonObject);
		String result = clusterMetrics.toString();
		return Response.status(200).entity(result).build();
	  }
	 

	 
	}
	
