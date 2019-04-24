package dubstep;

import java.util.Comparator;
import java.util.List;

public class DataFileRowComparator implements Comparator<DataRow>
{
    List<Field> sortFields;

    public DataFileRowComparator(List<Field> sortFields)
    {
        this.sortFields = sortFields;
    }

    @Override
    public int compare(DataRow A, DataRow B)
    {
        DataRowComparator comparator = new DataRowComparator(sortFields);

        return comparator.compare(A.getDataRow(),B.getDataRow());
    }
}
