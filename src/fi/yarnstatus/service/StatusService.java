package fi.yarnstatus.service;

import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import org.json.JSONException;
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
	 public Response getStatus() throws JSONException, ParseException, UnknownHostException {

		 Client client = Client.create();

		 WebResource webResource = client
				 .resource("http://localhost:8080/YarnStatus/rest/status/dummy");

		 ClientResponse response = webResource.accept("application/json")
				 .get(ClientResponse.class);

		 if (response.getStatus() != 200) {
			 throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		
		 	}

		 String output = response.getEntity(String.class);
		 JSONObject output_obj = (JSONObject)new JSONParser().parse(output);
		 
		 JSONObject clusterMetricsObj = (JSONObject)output_obj.get("clusterMetrics");
		 
		 JSONObject obj = new JSONObject();
		 obj.put("The available memory is ", clusterMetricsObj.get("availableMB"));
		 obj.put("Total Applications running currently are: ", clusterMetricsObj.get("appsRunning"));
		 obj.put("Total Applications running pending are: ", clusterMetricsObj.get("appsPending"));

			
			System.out.println(InetAddress.getLocalHost().getHostName());
		String result = obj.toString();
		return Response.status(200).entity(result).build();
	  }
	 
	 
	 @SuppressWarnings("unchecked")
	@GET
	 @Path("/dummy")
	 @Produces("application/json")
	 public Response dummyService() throws JSONException {

		JSONObject clusterMetrics = new JSONObject();
		JSONObject jsonObject = new JSONObject();
		
		jsonObject.put("availableMB", 10);
		jsonObject.put("allocatedMB", 10);
		jsonObject.put("Vcores", 1);
		jsonObject.put("appsRunning", 5);
		jsonObject.put("appsPending", 1);
	
		clusterMetrics.put("clusterMetrics", jsonObject);
		String result = clusterMetrics.toString();
		return Response.status(200).entity(result).build();
	  }
	 

	 
	}
	
