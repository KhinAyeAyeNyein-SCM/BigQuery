package big.query;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVWriter;

public class BigQueryReportService {
    public static void outputReport(Map<String, List<String>> fieldList, List<List<String[]>> rowList) {
        System.out.println("Generate CSV Files Start!");
        List<String> keyList = new ArrayList<String>(fieldList.keySet());
        for (int i = 0; i < fieldList.size(); i++) {
            CSVWriter writer;
            try {
                writer = new CSVWriter(new FileWriter(keyList.get(i) + ".csv"));
                String[] headers = fieldList.get(keyList.get(i)).toArray(new String[0]);
                writer.writeNext(headers);
                List<String[]> rows = rowList.get(i);
                writer.writeAll(rows);
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("Generate CSV Files Complete!");
    }
}
