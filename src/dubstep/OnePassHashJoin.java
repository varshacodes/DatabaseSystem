package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;


public class OnePassHashJoin implements RowTraverser
{

    RowTraverser leftIterator;
    RowTraverser rightIterator;
    HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>> hashTable;
    int leftIndex;
    int rightIndex;
    PrimitiveValue[] rightDataRow;
    HashMap<String, Integer> FieldPositionMapping;
    Iterator<PrimitiveValue[]> resultSet;


    public OnePassHashJoin(RowTraverser leftIterator, RowTraverser rightIterator, int leftIndex, int rightIndex) throws IOException, SQLException,ClassNotFoundException
    {
        this.leftIterator = leftIterator;
        this.rightIterator = rightIterator;
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
        setFieldPositionMapping();
        hashLeftTable();

    }

    @Override
    public int getNoOfFields()
    {
        return leftIterator.getNoOfFields() + rightIterator.getNoOfFields();
    }
    private void setFieldPositionMapping()
    {
        FieldPositionMapping = new HashMap<String, Integer>();
        HashMap<String, Integer> leftFieldMapping = leftIterator.getFieldPositionMapping();
        HashMap<String, Integer> rightFieldMapping = rightIterator.getFieldPositionMapping();
        int sizeOfLeft = Utility.getMaxPosition(leftFieldMapping)+1;
        Iterator<String> FieldKeys = leftFieldMapping.keySet().iterator();

        while (FieldKeys.hasNext())
        {
            String FieldName = FieldKeys.next();
            int position = leftFieldMapping.get(FieldName);
            FieldPositionMapping.put(FieldName,position);
        }

        FieldKeys = rightFieldMapping.keySet().iterator();

        while (FieldKeys.hasNext())
        {
            String FieldName = FieldKeys.next();
            int position = sizeOfLeft+ rightFieldMapping.get(FieldName);
            FieldPositionMapping.put(FieldName,position);

        }

    }
    private void hashLeftTable() throws IOException,SQLException, ClassNotFoundException
    {

       PrimitiveValue dataRow[] = leftIterator !=null ? leftIterator.next(): null;
       hashTable = new HashMap<PrimitiveValue, ArrayList<PrimitiveValue[]>>();

       while (dataRow!=null)
       {
           ArrayList<PrimitiveValue[]> dataRows = hashTable.getOrDefault(dataRow[leftIndex], new ArrayList<PrimitiveValue[]>());
           dataRows.add(dataRow);
           hashTable.put(dataRow[leftIndex],dataRows);
           dataRow = leftIterator.next();
       }
    }


    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(resultSet != null && resultSet.hasNext())
        {
            return mergeValues(resultSet.next(),rightDataRow);
        }
        else
        {
            resultSet = getNextResultSet();
            if(resultSet!= null && resultSet.hasNext())
            {
                return  mergeValues(resultSet.next(),rightDataRow);
            }
        }

        return null;
    }

    private  Iterator<PrimitiveValue[]> getNextResultSet() throws SQLException, IOException, ClassNotFoundException
    {
        rightDataRow = rightIterator!=null ? rightIterator.next():null;

        if(rightDataRow == null)
        {
            return  null;
        }
        else
        {
            ArrayList<PrimitiveValue[]> leftRows = null;
            while (leftRows== null && rightDataRow!=null)
            {
                leftRows = hashTable.get(rightDataRow[rightIndex]);
                if(leftRows!=null)
                {
                    return leftRows.iterator();

                }
                else
                {
                    rightDataRow = rightIterator.next();
                }

            }
        }

        return null;
    }

    private PrimitiveValue[] mergeValues(PrimitiveValue[] leftData, PrimitiveValue[] rightData)
    {
        PrimitiveValue[] mergeData = new PrimitiveValue[leftData.length +rightData.length];

        for (int i=0; i < leftData.length; i++)
        {
            mergeData[i] = leftData[i];
        }

        int sizeOfLeft = leftData.length;

        for (int i=0; i < rightData.length; i++)
        {
            mergeData[sizeOfLeft+i] = rightData[i];
        }
        return mergeData;

    }


    @Override
    public void reset() throws IOException, SQLException,ClassNotFoundException
    {
        rightIterator.reset();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return this.FieldPositionMapping;
    }

    @Override
    public void close() throws IOException , ClassNotFoundException
    {
        this.leftIterator.close();
        this.rightIterator.close();
    }



}
