package dubstep;


import net.sf.jsqlparser.expression.PrimitiveValue;

public class DataRow
{
    int FileNo;
    PrimitiveValue[] dataRow;

    public DataRow(int fileNo, PrimitiveValue[] dataRow)
    {
        FileNo = fileNo;
        this.dataRow = dataRow;
    }

    public int getFileNo()
    {
        return FileNo;
    }

    public PrimitiveValue[] getDataRow()
    {
        return dataRow;
    }


}
