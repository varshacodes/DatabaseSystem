package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

import java.sql.SQLException;
import java.util.HashMap;

public class CountAccumulator implements Accumulator
{
    PrimitiveValue count;
    Expression expression;

    @Override
    public String toString()
    {
        return " Count: "+ count;
    }

    public  CountAccumulator(Expression expression)
    {
        count = new LongValue(0);
        this.expression = expression;
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow, HashMap<String, Integer> fieldMapping) throws SQLException
    {
       ExpressionEvaluator evaluator = new ExpressionEvaluator(dataRow,fieldMapping);
        if(expression!=null)
        {
            boolean isNotnull = evaluator.eval(expression) !=null;
            if(isNotnull)
            {
                count = evaluator.eval(new Addition(count,new LongValue(1)));
            }
        }
        else
        {
            count = evaluator.eval(new Addition(count,new LongValue(1)));

        }

    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return count;
    }
}
