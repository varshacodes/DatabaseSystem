package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

public class JoinIterator implements RowTraverser
{
    RowTraverser leftIterator;
    RowTraverser rightIterator;
    PrimitiveValue[] leftDataRow;
    PrimitiveValue[] joinData;
    HashMap<String, Integer> FieldPositionMapping;
    PrimitiveValue[] current;
    boolean isInitialized;

    public JoinIterator(RowTraverser leftITerator , RowTraverser rightIterator)
    {
       this.leftIterator = leftITerator;
       this.rightIterator = rightIterator;
       this.isInitialized = false;
       setFieldPositionMapping();

    }

    @Override
    public int getNoOfFields()
    {
        return leftIterator.getNoOfFields() + rightIterator.getNoOfFields();
    }

    private void initialize()throws IOException, SQLException, ClassNotFoundException
    {
        this.leftDataRow = leftIterator.next();

    }

    public void setLeftIterator(RowTraverser leftIterator)
    {
        this.leftIterator = leftIterator;
    }

    public void setRightIterator(RowTraverser rightIterator)
    {
        this.rightIterator = rightIterator;
    }

    private void setFieldPositionMapping()
    {
        FieldPositionMapping = new HashMap<String, Integer>();
        HashMap<String, Integer> leftFieldMapping = leftIterator.getFieldPositionMapping();
        HashMap<String, Integer> rightFieldMapping = rightIterator.getFieldPositionMapping();
        int sizeOfLeft = Utility.getMaxPosition(leftFieldMapping) + 1;
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



    @Override
    public PrimitiveValue[] next() throws SQLException, IOException, ClassNotFoundException
    {
        if(!isInitialized)
        {
            isInitialized = true;
            initialize();
        }
        if(this.leftDataRow != null)
        {
            PrimitiveValue[] rightDataRow = rightIterator!= null? this.rightIterator.next(): null;

             if (rightDataRow != null )
             {
                 joinData = mergeValues(this.leftDataRow, rightDataRow);
                 return joinData;

             }
             else
             {
                this.leftDataRow = leftIterator !=null ? this.leftIterator.next(): null;
                if(leftDataRow!=null)
                {
                    rightIterator.reset();
                    joinData = mergeValues(this.leftDataRow,rightIterator.next());
                    return joinData;
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
    public void reset() throws IOException,SQLException,ClassNotFoundException
    {
      leftIterator.reset();
      rightIterator.reset();
      isInitialized = false;
    }

    public RowTraverser getLeftChild()
    {
        return leftIterator;
    }

    public RowTraverser getRightChild()
    {
        return rightIterator;
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldPositionMapping;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
        leftIterator.close();
        rightIterator.close();

    }
}
