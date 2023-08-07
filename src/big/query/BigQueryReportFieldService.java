package big.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryReportFieldService {
    public static Map<String, List<String>> getReportFields(Connection conn, List<String> tableList)
            throws SQLException {
        Map<String, List<String>> reportFieldList = new HashMap<String, List<String>>();
        Statement statement = conn.createStatement();
        System.out.println("Field Name Generate Start!");
        for (String tableName : tableList) {
            StringBuilder query = new StringBuilder();
            List<String> fieldList = new ArrayList<String>();
            query.append("SELECT ");
            query.append("ColumnName, DataTypeName, IsRequired ");
            query.append("FROM sys_tablecolumns ");
            query.append("Where TableName = 'windy-skyline-394706.Testing.");
            query.append("").append(tableName).append("'");
            System.out.println(query.toString());
            ResultSet result = statement.executeQuery(query.toString());
            while (result.next()) {
                if (!result.getString("ColumnName").equals("_PARTITIONDATE")) {
                    fieldList.add(result.getString("ColumnName"));
                }
            }
            reportFieldList.put(tableName, fieldList);
            System.out.println("CSV import for " + tableName + "is Completed! ");
        }
        System.out.println("Field Name Generate Completed!----");
        System.out.println("_____________________________________");
        return reportFieldList;
    }
}
