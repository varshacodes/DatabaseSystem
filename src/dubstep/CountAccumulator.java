package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;


public class CountAccumulator extends Evaluate implements Accumulator
{
    Long count;
    Expression expression;
    HashMap<String, Integer> fieldMapping;
    PrimitiveValue[] current;

    @Override
    public String toString()
    {
        return " Count: "+ count;
    }

    public  CountAccumulator(Expression expression,HashMap<String, Integer> fieldMapping,PrimitiveValue[] dataRow)throws SQLException
    {
        this.expression = expression;
        this.fieldMapping = fieldMapping;
        init(dataRow);


    }
    public void init(PrimitiveValue[] dataRow)throws SQLException
    {
        this.current = dataRow;
        count = new Long(1);

    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow) throws SQLException
    {
       count = count +1;

    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return new LongValue(count);
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
