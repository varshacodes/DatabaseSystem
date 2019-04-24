package dubstep;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class DataFileExpressionComparator implements Comparator<DataRow>
{
    List<OrderByField> orderByFields;
    HashMap<String, Integer> fieldMapping;

    public DataFileExpressionComparator(List<OrderByField> orderByFields, HashMap<String, Integer> fieldMapping)
    {
        this.orderByFields = orderByFields;
        this.fieldMapping = fieldMapping;
    }

    @Override
    public int compare(DataRow rowA, DataRow rowB)
    {
        DataExpressionComparator dataExpressionComparator = new DataExpressionComparator(orderByFields,fieldMapping);
        return  dataExpressionComparator.compare(rowA.getDataRow(),rowB.getDataRow());
    }
}
