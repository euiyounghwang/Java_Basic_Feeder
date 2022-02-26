package elastic.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class Java_Output {
	
	
	public static void Pretty_Json(String str) {
		//Trying to pretify JSON String:
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		JsonParser parser = new JsonParser();
		JsonElement je = parser.parse(str);
		System.out.println(gson.toJson(je));

	}

}
