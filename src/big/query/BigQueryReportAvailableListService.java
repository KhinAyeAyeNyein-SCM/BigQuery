package big.query;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BigQueryReportAvailableListService {
    public static List<String> getAvailableReports(Connection conn) {
        List<String> availableReportList = new ArrayList<String>();
        DatabaseMetaData dmd;
        try {
            dmd = conn.getMetaData();
            ResultSet rsGetTables = dmd.getTables(null, null, "%", null);
            while (rsGetTables.next()) {
                availableReportList.add(rsGetTables.getString("TABLE_NAME"));
                System.out.println(rsGetTables.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Table Generate Complete!");
        System.out.println("____________________________________");
        return availableReportList;
    }
}
