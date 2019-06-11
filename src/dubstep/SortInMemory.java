package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Iterator;

public class SortInMemory implements RowTraverser
{
    RowTraverser rowIterator;
    HashMap<String, Integer> fieldMapping;
    Iterator<PrimitiveValue[]> resultIterator;
    ArrayList<PrimitiveValue[]> dataRows;
    List<OrderByField> orderByFields;
    List<Field> sortFields;
    boolean isOrderBy;
    boolean isInitialized;

    public SortInMemory(RowTraverser rowIterator, List<Field> sortFieldList) throws SQLException, IOException,ClassNotFoundException
    {
        this.rowIterator = rowIterator;
        this.fieldMapping = rowIterator.getFieldPositionMapping();
        this.isOrderBy = false;
        this.sortFields = sortFieldList;
        this.isInitialized = false;
        resultIterator = null;
    }

    public SortInMemory(RowTraverser rowIterator,List<OrderByField> orderByFields,boolean isOrderBy)
    {
        this.rowIterator = rowIterator;
        this.fieldMapping = rowIterator.getFieldPositionMapping();
        this.isOrderBy = isOrderBy;
        this.orderByFields =orderByFields;
        this.isInitialized = false;
        resultIterator = null;

    }
    private void initialize()throws SQLException, IOException,ClassNotFoundException
    {
        if(isOrderBy)
        {
            sort(orderByFields,isOrderBy);

        }
        else
        {
            sort(sortFields);
        }
    }

    private void sort(List<OrderByField> orderByFields, boolean isMixture)throws IOException,SQLException,ClassNotFoundException
    {
        dataRows = new ArrayList<PrimitiveValue[]>();

        PrimitiveValue[] dataRow = rowIterator!=null? rowIterator.next():null;

        if(dataRow!=null)
        {
            while (dataRow != null)
            {
                dataRows.add(dataRow);
                dataRow = rowIterator.next();

            }
            Collections.sort(dataRows, new DataExpressionComparator(orderByFields, fieldMapping));
            resultIterator = dataRows.iterator();
        }
        rowIterator.close();

    }


    private void sort(List<Field> sortFieldList)throws IOException,SQLException, ClassNotFoundException
    {
        dataRows = new ArrayList<PrimitiveValue[]>();

        PrimitiveValue[] dataRow = rowIterator!=null? rowIterator.next():null;

        if(dataRow != null)
        {
            while (dataRow != null)
            {
                dataRows.add(dataRow);
                dataRow = rowIterator.next();

            }
            Collections.sort(dataRows, new DataRowComparator(sortFieldList));
            resultIterator = dataRows.iterator();
        }
        rowIterator.close();
    }


    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(!isInitialized)
        {
            initialize();
            isInitialized = true;
        }
        if(resultIterator!=null &&resultIterator.hasNext())
        {
            return resultIterator.next();
        }

        return null;
    }


    @Override
    public void reset()
    {
        resultIterator = dataRows.iterator();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return null;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
      rowIterator.close();
    }

    @Override
    public int getNoOfFields()
    {
        return rowIterator.getNoOfFields();
    }

    public RowTraverser getChild()
    {
        return rowIterator;
    }

    public Iterator<PrimitiveValue[]> getResultIterator()
    {
        return resultIterator;
    }

    public ArrayList<PrimitiveValue[]> getDataRows()
    {
        return dataRows;
    }


}