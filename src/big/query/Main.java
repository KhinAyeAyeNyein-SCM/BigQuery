package big.query;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws SQLException {
        BigQueryJDBC jdbcConnection = new BigQueryJDBC();
        Connection conn =  jdbcConnection.getConnection();
        List<String> tableList = BigQueryReportAvailableListService.getAvailableReports(conn);
        Map<String, List<String>> fieldList = BigQueryReportFieldService.getReportFields(conn, tableList);
        Map<String, String> query = jdbcConnection.buildSql(fieldList);
        BigQueryReportService.outputReport(conn, fieldList, query);
    }
}
