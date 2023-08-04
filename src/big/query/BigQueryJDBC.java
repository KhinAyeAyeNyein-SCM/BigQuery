package big.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class BigQueryJDBC {
    public Connection getConnection() {
        Properties prop = new Properties();
        try {
            Class.forName("cdata.jdbc.googlebigquery.GoogleBigQueryDriver");

            
            prop.setProperty("InitiateOAuth", "REFRESH");
            prop.setProperty("OAuthClientId",
                    "666879781482-a3ihmaj2b8ngkjqudot8mm0phqjiq2fr.apps.googleusercontent.com");
            prop.setProperty("OAuthClientSecret", "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");
            prop.setProperty("OAuthAccessToken",
                    "ya29.a0AbVbY6PWbWnbgspoM6neHS72aTFbmfJ4acggxLdYF5UQZZ3jVb9uG_W3QPMx4_iztaQPoEWlncWAJS2v1eEYirWBE63XJ9TSCNDqLo19MGIKVWU5qxI2EgcAQvjU1sojF_Ci7Mw9fc8HJM6PTNNeyH28h2P8aCgYKARESARMSFQFWKvPl6OlYxwyGTr17yTzW9YAsCA0163");
            prop.setProperty("OAuthRefreshToken",
                    "1//04E7uXbvCziUpCgYIARAAGAQSNwF-L9IrEZ-aZa_zCf81sFBm4oyQ8TLhR2V6eSJxS8GiURUO0798PsjxaPBPYJLvoGnsG0Ai5_4");
            prop.setProperty("OAuthSettingsLocation", "US");
            prop.setProperty("Projectid", "windy-skyline-394706");
            
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            return DriverManager.getConnection("jdbc:googlebigquery:", prop);
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }
    
    public List<List<String[]>> buildSql(Map<String, List<String>> dataList) throws SQLException {
        List<List<String[]>> rowList = new ArrayList<List<String[]>>();
        Connection conn = getConnection();
        Statement statement = conn.createStatement();
        System.out.println("Field List Generate Start!");
        dataList.forEach((key, value) -> {
            String field = String.join(",", value);
            StringBuilder query = new StringBuilder();
            query.append("SELECT ").append(field).append(" FROM Testing.").append(key);
            ResultSet result;
            List<String[]> resultRows = new ArrayList<String[]>();
            try {
                result = statement.executeQuery(query.toString());
                ResultSetMetaData metaData = result.getMetaData();
                while(result.next()) {
                    List<String> row = new ArrayList<String>();
                    for (int i = 0; i < metaData.getColumnCount(); i++) {
                        row.add(result.getString(i + 1));
                    }
                    resultRows.add(row.toArray(new String[0]));
                }
                rowList.add(resultRows);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } );
        System.out.println("Field List Generate Complete!------");
        return rowList;
    }
}
