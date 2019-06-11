package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;


public class AggregateField
{
    Aggregate aggregate;
    Expression expression;
    boolean isAllColums;
    HashMap<String,Integer> fieldMapping;

    public HashMap<String, Integer> getFieldMapping()
    {
        return fieldMapping;
    }

    public AggregateField(Aggregate aggregate, Expression expression,HashMap<String,Integer> rowIteratorMapping)throws SQLException
    {
        this.aggregate = aggregate;
        this.isAllColums = isAllColums;

        if(expression !=null)
        {
            this.expression = expression;
            isAllColums = false;
            this.fieldMapping = rowIteratorMapping;
        }
        else
        {
            isAllColums = true;
        }

    }
//    private void setFieldMapping(HashMap<String, Integer> fieldMapping)throws SQLException
//    {
//        Set<String> Fields = new HashSet<String>();
//        Utility.eval(expression,Fields);
//        this.fieldMapping = new HashMap<>();
//        Iterator<String> fieldsIterator = Fields.iterator();
//
//        while (fieldsIterator.hasNext())
//        {
//            String fieldName = fieldsIterator.next();
//            int position = fieldMapping.get(fieldName);
//            this.fieldMapping.put(fieldName,position);
//
//        }
//    }


    public Aggregate getAggregate()
    {
        return aggregate;
    }

    public Expression getExpression()
    {
        return expression;
    }

    public boolean isAllColums()
    {
        return isAllColums;
    }

    public Accumulator getNewAccumulator(PrimitiveValue[] dataRow)throws SQLException
    {
        switch (this.aggregate)
        {
            case AVG: return new AverageAccumulator(expression,fieldMapping,dataRow);
            case MAX: return new MaxAccumulator(expression,fieldMapping,dataRow);
            case MIN: return new MinAccumulator(expression,fieldMapping,dataRow);
            case COUNT: return new  CountAccumulator(expression,fieldMapping,dataRow);
            case SUM: return new SumAccumulator(expression,fieldMapping,dataRow);
        }
        return null;
    }


}
