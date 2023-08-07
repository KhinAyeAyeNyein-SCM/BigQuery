package big.query;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

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
            prop.setProperty("ProjectId", "windy-skyline-394706");
            return DriverManager.getConnection("jdbc:googlebigquery:", prop);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @SuppressWarnings("resource")
    public Map<String, String> buildSql(Map<String, List<String>> dataList) throws SQLException {
        System.out.print("Do you want to import sample data only? (Y | N) : ");
        Scanner sc = new Scanner(System.in);
        String str = sc.next();
        Map<String, String> tableQuery = new HashMap<String, String>();
        System.out.println("Query Generate Start!");
        dataList.forEach((key, value) -> {
            String field = String.join(",", value);
            StringBuilder query = new StringBuilder();

            query.append("SELECT ").append(field).append(" FROM Testing.").append(key);
            if (value.contains("_PARTITIONTIME")) {
                query.append(" WHERE TIMESTAMP_TRUNC(_PARTITIONTIME, DAY) <= TIMESTAMP('2023-08-07')");
            }
            if (str.equalsIgnoreCase("Y")) {
                query.append(" LIMIT 5");
            }
            System.out.println(query.toString());
            tableQuery.put(key, query.toString());
        });
        System.out.println("Query Generate Complete!------");
        System.out.println("_______________________________________");
        return tableQuery;
    }
}
