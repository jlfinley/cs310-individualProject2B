package edu.jsu.mcis;

import java.sql.*;
import java.util.*;
import org.json.simple.*;

public class IndivProject2B {

    public static JSONArray getJSONData()
    {
        Connection conn = null;
        PreparedStatement pstSelect = null, pstUpdate = null;
        ResultSet resultset = null;
        ResultSetMetaData metadata = null;
        
        String query, key, value;
        
        boolean hasresults;
        int resultCount, columnCount, updateCount = 0;
        JSONArray records = new JSONArray();
        
        try {
            
            /* Identify the Server */
            
            String server = ("jdbc:mysql://localhost/p2_test");
            String username = "root";
            String password = "CS488";
            System.out.println("Connecting to " + server + "...");
            
            /* Load the MySQL JDBC Driver */
            
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            
            /* Open Connection */

            conn = DriverManager.getConnection(server, username, password);

            /* Test Connection */
            
            if (conn.isValid(0)) {
                
                /* Connection Open! */
                
                System.out.println("Connected Successfully!"); 
                
                /* Prepare Select Query */
                
                query = "SELECT * FROM people";
                pstSelect = conn.prepareStatement(query);
                
                /* Execute Select Query */
                
                System.out.println("Submitting Query ...");
                
                hasresults = pstSelect.execute();                
                
                /* Get Results */
                
                System.out.println("Getting Results ...\n\n");
                
                while ( hasresults || pstSelect.getUpdateCount() != -1 ) {

                    if ( hasresults ) {
                        
                        /* Get ResultSet Metadata */
                        
                        resultset = pstSelect.getResultSet();
                        metadata = resultset.getMetaData();
                        columnCount = metadata.getColumnCount();
                        
                        LinkedHashMap<String, String> jsonObject = new LinkedHashMap<>();
                        String jsonString = "";
                        
                        /* Get Column Names and Add to List */
                        
                        StringBuilder keyString = new StringBuilder();
                        
                        for (int i = 2; i <= columnCount; i++) {

                            key = metadata.getColumnLabel(i);
                            keyString.append(key).append(",");

                        }
                        
                        List<String> keys = new ArrayList<>(Arrays.asList(keyString.toString().split(",")));
                        String[] keyArray = keys.toArray(new String[0]);
                        
                        /* Get Data and Add to List */
                        
                        while(resultset.next()) {

                            StringBuilder valueString = new StringBuilder();
                            
                            for (int i = 2; i <= columnCount; i++) {

                                value = resultset.getString(i);

                                if (resultset.wasNull())
                                {
                                    System.out.format("%20s", "NULL");
                                }

                                else
                                {
                                    valueString.append(value).append(",");
                                }
                            
                            }
                            
                            List<String> values = new ArrayList<>(Arrays.asList(valueString.toString().split(",")));
                            String[] valueArray = values.toArray(new String[0]);
                            jsonObject = new LinkedHashMap<>();
                            
                            for (int j = 0; j < keyArray.length; ++j)
                            {
                                jsonObject.put(keyArray[j], valueArray[j]);
                            }
                            
                            records.add(jsonObject);
                        }
                        
                        jsonString = JSONValue.toJSONString(records);
                        System.out.println(jsonString.trim());
                    
                    }

                    else {

                        resultCount = pstSelect.getUpdateCount();  

                        if ( resultCount == -1 ) {
                            break;
                        }

                    }
                    
                    /* Check for More Data */

                    hasresults = pstSelect.getMoreResults();

                }
                
            }
            
            System.out.println();
            
            /* Close Database Connection */
            
            conn.close();
            
        }
        
        catch (Exception e) {
            System.err.println(e.toString());
        }
        
        /* Close Other Database Objects */
        
        finally {
            
            if (resultset != null) { try { resultset.close(); resultset = null; } catch (Exception e) {} }
            
            if (pstSelect != null) { try { pstSelect.close(); pstSelect = null; } catch (Exception e) {} }
            
            if (pstUpdate != null) { try { pstUpdate.close(); pstUpdate = null; } catch (Exception e) {} }
            
        }
    
        return records;
        
    }

}
