

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map.Entry;

import elastic.utils.Java_Elastic;
import elastic.utils.Java_Http;

import java.util.Set;


public class DB_Feeder_Sample {
	
	private Connection con;
	private PreparedStatement pstmt;
	private ResultSet rs;
	private Statement stmt;
	
	private String URL = null;
	private String INDICES_NAME = "sample";
	private String Elastic_IP = "127.0.0.1:9201";

		
	private StringBuffer sb = null;
	
	/*
	 * 1) DB Connection (TNS/ID/PW)
	 * 2) SQL 
	 * 3) RecordSet loop -> JSON 
	 * 4) SEND (Same, Buffer Transaction)
	 */
			
	
	public DB_Feeder_Sample() {
		
		try 
		{
			
			String url = "jdbc:oracle:thin:@127.0.0.1:1234:DB";
			String user = "account";
			String passwd = "1";
			
			con = DriverManager.getConnection(url, user, passwd);
			System.out.println("\n");
			System.out.println("---");
			System.out.println("DB Connection Success..");
			System.out.println("---");
			
			pstmt = con.prepareStatement("SELECT * FROM TB WHERE ROWNUM< 12");
			/*
			pstmt.setString(1, COMPANY_CODE);
			*/
			
			rs = pstmt.executeQuery();
			
			ResultSetMetaData rsmd = rs.getMetaData();
			
			int numberOfColumns=rsmd.getColumnCount();
			
			/*
			int number = 1;
			System.out.println("\n");
			for(int i=1;i<=numberOfColumns;i++){
				System.out.println(String.format("Columns %s -> %s", String.valueOf(number), rsmd.getColumnName(i)));
				number +=1;
			}
			*/
			
			System.out.println("\n");
			
			URL = "http://" + Elastic_IP + "/_bulk";
//		URL = "http://" + Elastic_IP + "/_bulk?refresh=wait_for";
			
			
			HashMap<String, String> each_columns = new HashMap<String, String>();
			
			sb = new StringBuffer();
			
			int loop=0;
			while (rs.next()) {
//				System.out.println(rs.getString(1));
				for(int i=1;i<=numberOfColumns;i++){
//					System.out.println(String.format("%s -> %s", rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i))));
					each_columns.put(rsmd.getColumnName(i), rs.getString(rsmd.getColumnName(i)));
				}
				
//				System.out.println(each_columns);
				
				/*
				 *  Add Buffer (DB Fields)
				 */
				
				StringBuffer c_sb = new StringBuffer();
				/*
				// for loop (entrySet())
				for (Entry<String, String> entrySet : each_columns.entrySet()) {
					// System.out.println(entrySet.getKey() + " : " + entrySet.getValue());
					c_sb.append(String.format("\"%s\" : \"%s\",", entrySet.getKey(), entrySet.getValue()));
				}*/

				Set<String> keySet = each_columns.keySet();
				// for loop (keySet())
				for (String key : keySet) {
//					System.out.println(key + " : " + each_columns.get(key));
					c_sb.append(String.format("\"%s\" : \"%s\",", key, each_columns.get(key)));
				}

				/*
				 *  **************************************
				 *  **************************************
				 *  CONTENT : ECM_DOC_ID Mapping -> Add Fields
				 *  Socket(Attached ECM_DOC_ID) -> DRM -> DOCUMENT_EXTRACT Library -> Text EXTRACT
				 *  **************************************
				 *  
				 */
				String CONTENT = "Add ECM_DOC_ID";
				c_sb.append(String.format("\"%s\" : \"%s\",", "CONTENT", CONTENT));
				
				/*
				 * Make INDEX BUFFER				
				 */
				sb.append("{\"index\" : {\"_index\" : \"" + INDICES_NAME + "\", \"_type\" : \"_doc\", \"_id\" : \"" + "new_id_" + String.valueOf(loop++) + "\"}}" + "\n");
				sb.append("{" + c_sb.toString().substring(0, c_sb.toString().length()-1) + "}\n");
				
				// System.out.println(sb.toString());
				
			
				System.out.println("\nBuffer -> ");
				
				/*
				 * Send Buffer
			    */
				
				
			}
			
			
			rs.close();
			pstmt.close();
			con.close();
			
		} catch (SQLException e) {
			System.out.println("DB Connection Error~");
			System.out.println("ex : " + e.getMessage());
		}
		
		finally {
			System.out.println("\n");
			System.out.println("---");
			System.out.println("DB DisConnection Suuccess..");
			System.out.println("---");
			
			System.out.println("\n\n");
			
			/*
			URL = "http://" + Elastic_IP + "/" + INDICES_NAME + "/_search";
			
			sb.append("{");
			sb.append("\"track_total_hits\": true,");
			sb.append("   \"query\": {");
			sb.append("   		\"match_all\": {}");
			sb.append("   }");
			sb.append("}");
			
			new Java_Http(false, sb, URL).sendHttpRequest();
			*/
		}
	}
	
	public static void main(String[] args) {
		new DB_Feeder_Sample();
	}

}
