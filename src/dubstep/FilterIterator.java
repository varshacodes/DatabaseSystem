package dubstep;


import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

public class FilterIterator implements RowTraverser
{
    RowTraverser dataRowIterator;
    HashMap<String,Integer> FieldPositionMapping;
    Expression whereCondition;

    @Override
    public void reset() throws IOException
    {
      dataRowIterator.reset();
    }

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return FieldPositionMapping;
    }

    public FilterIterator(RowTraverser dataRowIterator,Expression whereCondition)
    {
        this.dataRowIterator = dataRowIterator;
        this.FieldPositionMapping = FieldPositionMapping;
        this.whereCondition = whereCondition;
        this.FieldPositionMapping = dataRowIterator.getFieldPositionMapping();
    }

    public boolean hasNext()throws IOException
    {
        if(dataRowIterator.hasNext())
        {
            return true;
        }
        else {
            return  false;
        }
    }

    public PrimitiveValue[] next() throws SQLException, IOException
    {
       if(dataRowIterator!= null)
       {
           PrimitiveValue[] datarow = dataRowIterator.next();

           if (datarow != null)
           {
               ExpressionEvaluator evaluator = new ExpressionEvaluator(datarow, FieldPositionMapping);
               boolean isConditionTrue = evaluator.eval(whereCondition).toBool();

               if (isConditionTrue)
               {
                   return datarow;

               } else {

                   return next();
               }
           }

       }

       return null;

    }

}
