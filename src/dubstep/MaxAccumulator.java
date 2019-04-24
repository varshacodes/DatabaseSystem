package dubstep;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;

import java.sql.SQLException;
import java.util.HashMap;

public class MaxAccumulator implements  Accumulator
{
    PrimitiveValue max;
    ExpressionEvaluator evaluator;
    Expression expression;
    boolean isInitialized;

    public MaxAccumulator(Expression expression)
    {
       this.expression = expression;
       this.isInitialized = false;
    }

    @Override
    public String toString()
    {
        return " Max: "+ max;
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow, HashMap<String, Integer> fieldMapping) throws SQLException
    {
        evaluator = new ExpressionEvaluator(dataRow,fieldMapping);
        PrimitiveValue value = evaluator.eval(expression);

        if(!isInitialized)
        {
            max = value;
            isInitialized =true;
        }
        else
        {
            boolean istrue = evaluator.eval(new GreaterThan(max,value)).toBool();
            max = istrue? max: value;
        }




    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return max;
    }
}
