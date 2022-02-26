package elastic.utils;

import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


/*
 *  Elastic Response ACK
 */
public class Java_Elastic {

	
	public Integer getBufferLength(String str, float MEMORY_MAX_SIZE) {
//		print('\n' + Util.bcolors().BOLD + Util.bcolors().YELLOW + 'StringBuffer [Add Meta] ' + str(max_len) + 'Bytes /' + str(MEMORY_MAX_SIZE) + 'Bytes (Total Meta Buffer Ratio : ' + str(round((float)(max_len / MEMORY_MAX_SIZE), 2) * 100) + '%)' + Util.bcolors().ENDC)
		System.out.println("\n");
		System.out.println("StringBuffer [Add Meta]"  + str.length() + "Bytes / " +
				String.valueOf((MEMORY_MAX_SIZE)) + "Bytes (Total Meta Buffer Ratio : "
				+ String.valueOf(Math.round(((float)(str.length()) / MEMORY_MAX_SIZE) * 100.0)) + "%)");
		return str.length();
	}
	
	
	public StringBuffer HttpElasticResponseACK(String result) {
		JSONParser parser = new JSONParser();
		StringBuffer _response = new StringBuffer();
		String ACKObject = null;

		try {

			JSONObject jsonObject = (JSONObject) parser.parse(result);

			if (jsonObject.get("items") != null) {
				JSONArray items = (JSONArray) jsonObject.get("items");

				@SuppressWarnings("unchecked")
				Iterator<String> iterator = items.iterator();

				while (iterator.hasNext()) {
					Object obj1 = parser.parse(String.valueOf(iterator.next()));
					JSONObject jsonArrayObject = (JSONObject) obj1;

					// ACK insert/update >> index
					if (jsonArrayObject.get("index") != null)
						ACKObject = String.valueOf(jsonArrayObject.get("index"));

					if (jsonArrayObject.get("update") != null)
						ACKObject = String.valueOf(jsonArrayObject.get("update"));

					// ACK delete >> delete
					if (jsonArrayObject.get("delete") != null)
						ACKObject = String.valueOf(jsonArrayObject.get("delete"));

					Object jsonArrayObjectFinal = parser.parse(ACKObject);
					JSONObject jsonFinal = (JSONObject) jsonArrayObjectFinal;

					String _id = String.valueOf(jsonFinal.get("_id"));
					String status = String.valueOf(jsonFinal.get("status"));

					if (jsonArrayObject.get("index") != null) {
						if (status.startsWith("2"))
							_response.append("[CS]" + _id + ",");
						//
						if (status.startsWith("4"))
							_response.append("[CF]" + _id + ":error >> " + result + ",");
					}

					if (jsonArrayObject.get("update") != null) {
						if (status.startsWith("2"))
							_response.append("[US]" + _id + ",");

						if (status.startsWith("4"))
							_response.append("[UF]" + _id + ":error >> " + result + ",");
					}

					if (jsonArrayObject.get("delete") != null) {
						if (status.startsWith("2"))
							_response.append("[DS]" + _id + ",");
						//
						if (status.startsWith("4"))
							_response.append("[DF]" + _id + ":error >> " + result + ",");
					}
				}
			}

			// Bulk Deleted
			if (jsonObject.get("_indices") != null) {
				JSONObject jsonSubObject = (JSONObject) jsonObject.get("_indices");
				jsonSubObject = (JSONObject) jsonSubObject.get("_all");
				String TotlaDeletedCount = String.valueOf(jsonSubObject.get("deleted"));

				System.out.println("TotalDeletedCounts >> " + TotlaDeletedCount);
			}

			System.out.println("\n # ACK~~~ : The Number of Success : " + _response.toString().split(",").length);
			System.out.println(_response.substring(0, _response.length() - 1));
			
		} catch (ParseException e) {
			_response.append("\n" + result);
			e.printStackTrace();
		}

		return _response;
	}
}
