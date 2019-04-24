package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;

import java.sql.SQLException;
import java.util.HashMap;

public class SumAccumulator implements  Accumulator
{
    PrimitiveValue sum;
    ExpressionEvaluator evaluator;
    Expression expression;
    boolean isInitialized;

    @Override
    public String toString()
    {
       return " sum: "+ sum;
    }

    public SumAccumulator(Expression expression)
    {
       this.expression = expression;
       this.isInitialized = false;
    }

    @Override
    public void Accumulate(PrimitiveValue  []dataRow, HashMap<String,Integer> fieldMapping)throws SQLException
    {
        evaluator = new ExpressionEvaluator(dataRow,fieldMapping);
        if(!isInitialized)
        {
            sum = evaluator.eval(expression);
            isInitialized = true;
        }
        else
        {
            sum = evaluator.eval(new Addition(sum,evaluator.eval(expression)));

        }
    }

    @Override
    public PrimitiveValue Fold()throws SQLException
    {
        return sum;
    }
}
