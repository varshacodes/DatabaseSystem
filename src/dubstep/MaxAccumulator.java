package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;

public class MaxAccumulator extends Evaluate implements Accumulator
{
    GreaterThan greaterThan;
    PrimitiveValue max;
    Expression expression;
    HashMap<String,Integer> fieldMapping;
    PrimitiveValue[] current;

    public MaxAccumulator(Expression expression, HashMap<String,Integer> fieldMapping,PrimitiveValue[] dataRow)throws SQLException
    {
       this.expression = expression;
       this.fieldMapping = fieldMapping;
       init(dataRow);
    }

    @Override
    public String toString()
    {
        return " Max: "+ max;
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow) throws SQLException
    {
            this.current = dataRow;
            PrimitiveValue value = eval(expression);
            greaterThan.setLeftExpression(max);
            greaterThan.setRightExpression(eval(expression));
            boolean istrue = eval(greaterThan).toBool();
            max = istrue? max: value;
    }

    @Override
    public PrimitiveValue Fold() throws SQLException
    {
        return max;
    }

    @Override
    public void init(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        max = eval(expression);
        greaterThan = new GreaterThan(null,null);
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
