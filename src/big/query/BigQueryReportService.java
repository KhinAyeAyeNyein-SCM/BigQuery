package big.query;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVWriter;

public class BigQueryReportService {
    public static void outputReport(Connection conn, Map<String, List<String>> fieldList, Map<String, String> query) {
        System.out.println("Generate CSV Files Start!");
        List<String> keyList = new ArrayList<String>(fieldList.keySet());
        for (int i = 0; i < fieldList.size(); i++) {
            CSVWriter writer;
            try {
                writer = new CSVWriter(new FileWriter(keyList.get(i) + ".csv"));
                String[] headers = fieldList.get(keyList.get(i)).toArray(new String[0]);
                writer.writeNext(headers);
                ResultSet result;
                List<String[]> resultRows = new ArrayList<String[]>();
                try {
                    Statement statement = conn.createStatement();
                    result = statement.executeQuery(query.get(keyList.get(i)));
                    ResultSetMetaData metaData = result.getMetaData();
                    while (result.next()) {
                        List<String> row = new ArrayList<String>();
                        for (int j = 0; j < metaData.getColumnCount(); j++) {
                            row.add(result.getString(j + 1));
                        }
                        resultRows.add(row.toArray(new String[0]));
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                writer.writeAll(resultRows);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Generate CSV Files Complete!");
    }
}
