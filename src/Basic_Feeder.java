import elastic.utils.Java_Elastic;
import elastic.utils.Java_Http;

public class Basic_Feeder {

		public static void main(String[] args) {
		
		// TODO Auto-generated method stub
		/*
		 *https://www.json-to-ndjson.app/
		 * 
		curl -XPOST "http://127.0.0.1:9201/_bulk" -H 'Content-Type: application/json' --data-binary @/ES/Basic_Feeder/INPUT/posts.json
		curl -XPOST -u elastic:1 "http://127.0.0.1:9201/_bulk" -H 'Content-Type: application/json' --data-binary @/ES/Basic_Feeder/INPUT/posts.json
		 */
			 
		String Elastic_IP = "127.0.0.1:9200";
		String URL = null;
		String INDICES_NAME = "sample";
		Integer MAX_ROWS = 30;
		
		float Max_Buffer_Size = 500;
			
		StringBuffer sb = new StringBuffer();	
		
		Java_Elastic getLengthObj = new Java_Elastic();
		
		try
		{
			/*
			String Index_Header = "{ \"index\" : { \"_index\" : \"" + INDICES_NAME + "\", \"_type\" : \"_doc\"}}";
			
			sb.append(Index_Header + "\n");
			sb.append("{ \"brand\" : \"Mercedes\", \"doors\" : 5 }\n");
			sb.append(Index_Header + "\n");
			sb.append("{ \"brand\" : \"1\", \"doors\" : 15 }\n");
			*/
			
			URL = "http://" + Elastic_IP + "/_bulk";
//			URL = "http://" + Elastic_IP + "/_bulk?refresh=wait_for";
			
			for (int i = 0; i < MAX_ROWS; i++) 
			{
				sb.append("{ \"index\" : { \"_index\" : \"" + INDICES_NAME + "\", \"_type\" : \"_doc\", \"_id\" : \"" + "new_id_" + String.valueOf(i) + "\"}}" + "\n");
				sb.append("{\"TITLE\" : \"Java Feeder 샘플 데이터 색인 과제\"}" + "\n");
				//sb.append("{ \"delete\" : { \"_index\" : \"" + INDICES_NAME + "\", \"_type\" : \"_doc\", \"_id\" : \"" + "new_id_" + String.valueOf(i) + "\"}}" + "\n");
				sb.append("{ \"update\" : { \"_index\" : \"" + INDICES_NAME + "\", \"_type\" : \"_doc\", \"_id\" : \"" + "new_id_" + String.valueOf(i) + "\"}}" + "\n");
				// sb.append("{\"doc\" : {\"TITLE\" : \"Feeder 샘플 데이터 색인 과제 Change\"}, \"doc_as_upsert\" : true}" + "\n");
				sb.append("{\"doc\" : {\"TITLE\" : \"Feeder 샘플 데이터 색인 과제 Change\"}}" + "\n");

				/*
				 * Buffer Check
				 if (getLengthObj.getBufferLength(sb.toString(), Max_Buffer_Size) < Max_Buffer_Size)
					continue;
				*/
				
				System.out.println("\nBuffer -> ");
				/*
				 * Send Buffer
				 */
				new Java_Http(true, sb, URL).sendHttpRequest();

				sb.setLength(0);

			}
			
			/*
			 * Remain Send Buffer
			 */
			/*
			if (sb.length() > 0) {
				System.out.println("\nRemain Buffer -> ");
				new Java_Http(true, sb, URL).sendHttpRequest();
				sb.setLength(0);
			}
			*/
							
		}
		catch(Exception ex) {
			System.out.println(ex);
		}
		finally {
			
			System.out.println("\n\n");
			
			URL = "http://" + Elastic_IP + "/" + INDICES_NAME + "/_search";
			
			sb.append("{");
			sb.append("\"track_total_hits\": true,");
			sb.append("   \"query\": {");
			sb.append("   		\"match_all\": {}");
			sb.append("   }");
			sb.append("}");
			
			new Java_Http(false, sb, URL).sendHttpRequest();
		}

	}

}
