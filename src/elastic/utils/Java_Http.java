package elastic.utils;

import java.util.HashMap;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import elastic.encode.Base64Coder;


@SuppressWarnings("deprecation")
public class Java_Http {
	
	private StringBuffer sb = null;
	private String url = null;
	private Boolean isSendACK = false;
	
	public Java_Http() {
		// TODO Auto-generated constructor stub
	}
	
	
	public Java_Http(Boolean isSendACK, StringBuffer sb, String url) {
		// TODO Auto-generated constructor stub
		this.isSendACK = isSendACK;
		this.sb = sb;
		this.url = url;
	}
	
	
	
	@SuppressWarnings("deprecation")
	public void sendHttpRequest() {
	
		HttpPost _Httppost = null;
		HttpClient _Httpclient = null;
		String result = null;
		HttpEntity entity = null;
		HttpResponse response = null;
		
		String _buffer = this.sb.toString();
		String url = this.url.toString();
	
		System.out.println("url : " + url);
		System.out.println("_buffer : -> ");
		System.out.println(_buffer);
		// System.exit(1);

		HashMap<String, String> map = new HashMap<String, String>();

		try {

			_Httpclient = new DefaultHttpClient();
			_Httppost = new HttpPost(url);

			entity = new ByteArrayEntity(_buffer.getBytes("UTF-8"));
			_Httppost.setEntity(entity);
			_Httppost.setHeader("Authorization", "Basic " + Base64Coder.encodeString("elastic:gsaadmin"));
			_Httppost.setHeader("Content-Type", "application/x-ndjson");
//			_Httppost.setHeader("Content-Type", "application/json");

			response = _Httpclient.execute(_Httppost);
			result = EntityUtils.toString(response.getEntity());

			System.out.println("\n");
			System.out.println("#####################################################");
			System.out.println("ElasticSearch Send : " + this.url);
			System.out.println("#####################################################");
			System.out.println("\n");
			
			System.out.println("response.getStatusLine().getStatusCode() : " + response.getStatusLine().getStatusCode());

			if (response.getStatusLine().getStatusCode() != 200) {
				System.out.println("Failed : HTTP error code : " + response.getStatusLine().getStatusCode() + "\t" + response.getStatusLine());
				System.out.println("Http ElasticSearch Error >>\n");
				System.out.println(_buffer.toString());

				result = "";

			} else {

				// map.put("ajaxData", result);
				System.out.println("result -> ");

				// JSON Pretty
				Java_Output.Pretty_Json(result);
				
				if (!this.isSendACK) {
						JSONObject jsonObject = new JSONObject(result);
						
						System.out.println("\n");
						System.out.print("# Search Results -> The Number of Results : ");
						System.out.println(jsonObject.getJSONObject("hits").getJSONObject("total").get("value").toString());
						return;
				}
								
				new Java_Elastic().HttpElasticResponseACK(result);

			}

		} catch (Exception ex) {
			ex.printStackTrace();
			System.out.println(ex.toString());
			System.out.println("[Main Statics] : " + ex.getMessage());
		} finally {
			_Httpclient.getConnectionManager().shutdown();
		}

	}


}
