package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.PrimitiveType;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class SortMergeJoin extends Evaluate implements RowTraverser
{
    RowTraverser leftIterator;
    RowTraverser rightIterator;
    HashMap<String, Integer> FieldPositionMapping;
    PrimitiveValue[] leftDataRow;
    PrimitiveValue[] rightDataRow;
    int leftIndex;
    int rightIndex;
    Iterator<PrimitiveValue[]> resultSet;
    boolean isInitialized = false;


    public SortMergeJoin(RowTraverser leftIterator, RowTraverser rightIterator, List<Field> left, List<Field> right) throws  IOException , SQLException, ClassNotFoundException
    {
        this.leftIterator = new SortOnDisk(leftIterator,left,"Sort_Left");
        this.rightIterator = new SortOnDisk(rightIterator,right,"Sort_Right");
        this.leftIndex = left.get(0).getPosition();
        this.rightIndex = right.get(0).getPosition();
        this.isInitialized = false;
        setFieldPositionMapping();
    }

    @Override
    public int getNoOfFields() {
        return leftIterator.getNoOfFields() + rightIterator.getNoOfFields();
    }

    private void initialize()throws  IOException , SQLException, ClassNotFoundException
    {
        this.leftDataRow = leftIterator !=null ? this.leftIterator.next(): null;
        this.rightDataRow = rightIterator !=null ? this.rightIterator.next(): null;
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


    private int compare(PrimitiveValue a, PrimitiveValue b) throws SQLException
    {
        if(a.getType() == PrimitiveType.STRING)
        {
            return a.toRawString().compareTo(b.toRawString());
        }
        if(eval(new EqualsTo(a,b)).toBool())
        {
            return 0;
        }
        else if(eval(new GreaterThan(a,b)).toBool())
        {
            return 1;
        }
        else {
            return -1;
        }
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
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
         if(!isInitialized)
         {
             isInitialized= true;
             initialize();
         }
         if(resultSet !=null && resultSet.hasNext())
         {
             return resultSet.next();
         }
         else {

                 resultSet = getNextResultSet();
                 if(resultSet != null && resultSet.hasNext())
                 {
                     return  resultSet.next();
                 }

         }
         return null;
    }

    private Iterator<PrimitiveValue[]> getNextResultSet() throws SQLException, IOException, ClassNotFoundException
    {
        if(leftDataRow != null && rightDataRow!= null)
        {
            PrimitiveValue matchValue = findMatch(leftDataRow,rightDataRow);
            if(matchValue != null)
            {
                ArrayList<PrimitiveValue[]> leftList = getLeftList(matchValue);
                ArrayList<PrimitiveValue[]> rightList = getRightList(matchValue);
                return mergeData(leftList,rightList);
            }
        }

        return null;

    }
    private Iterator<PrimitiveValue[]> mergeData(ArrayList<PrimitiveValue[]> leftList,ArrayList<PrimitiveValue[]> rightList)
    {
        ArrayList<PrimitiveValue[]> joinedData = new ArrayList<PrimitiveValue[]>();

        for(PrimitiveValue[] leftDataRow: leftList)
        {
            for(PrimitiveValue[] rightDataRow: rightList)
            {
                joinedData.add(mergeValues(leftDataRow,rightDataRow));
            }
        }

        return joinedData.iterator();
    }
    private ArrayList<PrimitiveValue[]> getLeftList(PrimitiveValue primitiveValue) throws SQLException,IOException,ClassNotFoundException
    {
        ArrayList<PrimitiveValue[]> dataList = new ArrayList<PrimitiveValue[]>();

        while (leftDataRow != null && compare(primitiveValue,leftDataRow[leftIndex])==0)
        {
            dataList.add(leftDataRow);
            leftDataRow = leftIterator.next();
        }
        return dataList;

    }

    private ArrayList<PrimitiveValue[]> getRightList(PrimitiveValue primitiveValue) throws SQLException,IOException,ClassNotFoundException
    {
        ArrayList<PrimitiveValue[]> dataList = new ArrayList<PrimitiveValue[]>();

        while (rightDataRow != null && compare(primitiveValue,rightDataRow[rightIndex])==0)
        {
            dataList.add(rightDataRow);
            rightDataRow = rightIterator.next();
        }
        return dataList;

    }

    private PrimitiveValue findMatch(PrimitiveValue[] leftDataRow, PrimitiveValue[] rightDataRow)throws SQLException,IOException, ClassNotFoundException
    {
        int compare = Integer.MAX_VALUE;

        while (compare !=0)
        {
            if (leftDataRow!= null && rightDataRow !=null)
            {
                compare = compare(leftDataRow[leftIndex],rightDataRow[rightIndex]);

                if(compare ==0)
                {
                    return leftDataRow[leftIndex];
                }
                else if(compare >0)
                {
                    leftDataRow = leftIterator.next();
                }
                else
                {
                    rightDataRow = rightIterator.next();
                }
            }
            else
            {
                return null;
            }

        }

        return null;
    }


    @Override
    public void reset() throws IOException, SQLException, ClassNotFoundException
    {
        leftIterator.reset();
        rightIterator.reset();
        isInitialized = false;

    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping() {
        return FieldPositionMapping;
    }

    @Override
    public void close() throws IOException, ClassNotFoundException
    {
        leftIterator.close();
        rightIterator.close();
    }



    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        return null;
    }
}
