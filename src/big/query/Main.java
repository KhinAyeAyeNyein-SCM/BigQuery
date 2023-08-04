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
        List<List<String[]>> rowList = jdbcConnection.buildSql(fieldList);
        BigQueryReportService.outputReport(fieldList, rowList);
    }
}
