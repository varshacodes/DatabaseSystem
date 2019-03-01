package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

public class JoinIterator implements RowTraverser
{


    RowTraverser leftITerator;
    RowTraverser rightIterator;
    PrimitiveValue[] leftDataRow;
    PrimitiveValue[] joinData;
    HashMap<String, Integer> joinFieldMapping = new HashMap<>();

    public JoinIterator(RowTraverser leftITerator , RowTraverser rightIterator)  throws IOException, SQLException
    {
       this.leftITerator = leftITerator;
       this.rightIterator = rightIterator;
       this.leftDataRow = this.leftITerator.next();
       setJoinFieldTable();

    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return joinFieldMapping;
    }
    private void setJoinFieldTable()
    {

        joinFieldMapping = leftITerator.getFieldPositionMapping();
        HashMap<String, Integer> rightFieldMapping = rightIterator.getFieldPositionMapping();
        int sizeOfLeft = joinFieldMapping.size();

        Iterator<String> FieldKeys = rightFieldMapping.keySet().iterator();

        while (FieldKeys.hasNext())
        {
            String FieldName = FieldKeys.next();
            int position = sizeOfLeft+ rightFieldMapping.get(FieldName);
            joinFieldMapping.put(FieldName,position);
        }



    }



    @Override
    public PrimitiveValue[] next() throws SQLException, IOException {



        if(this.leftDataRow != null)
        {
            if (this.rightIterator.hasNext())
                {
                    joinData = mergeValues(this.leftDataRow, this.rightIterator.next());
                    return joinData;
                }
                else
                {
                    if(this.leftITerator.hasNext())
                    {
                        this.leftDataRow = this.leftITerator.next();
                        rightIterator.reset();
                        joinData = mergeValues(this.leftDataRow,this.rightIterator.next());
                        return joinData;
                    }


                }
            }
        return  null;
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
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public void reset() throws IOException
    {
      leftITerator.reset();
      rightIterator.reset();
    }
}
