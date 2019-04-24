package dubstep;

import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;

import java.sql.SQLException;
import java.util.HashMap;

public class MinAccumulator implements Accumulator
{
    PrimitiveValue min;
    ExpressionEvaluator evaluator;
    Expression expression;
    boolean isInitialized;


    public MinAccumulator(Expression expression)
    {
        this.isInitialized = false;
        this.expression = expression;
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow, HashMap<String, Integer> fieldMapping) throws SQLException
    {

        evaluator = new ExpressionEvaluator(dataRow,fieldMapping);
        PrimitiveValue value = evaluator.eval(expression);
        if(!isInitialized)
        {
            min = value;
            isInitialized = true;
        }
        else
        {
            boolean istrue = evaluator.eval(new GreaterThan(min,value)).toBool();
            min = istrue? value: min;
        }

    }

    @Override
    public String toString()
    {
        return " min: "+ min;
    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return min;
    }
}
