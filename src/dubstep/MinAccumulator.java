package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;

public class MinAccumulator extends Evaluate implements Accumulator
{
    PrimitiveValue min;
    GreaterThan greaterThan;
    Expression expression;
    HashMap<String,Integer> fieldMapping;
    PrimitiveValue[] current;

    public MinAccumulator(Expression expression, HashMap<String,Integer> fieldMapping,PrimitiveValue[] dataRow)throws SQLException
    {
        this.expression = expression;
        this.fieldMapping = fieldMapping;
        init(dataRow);
    }

    @Override
    public void Accumulate(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        PrimitiveValue value = eval(expression);
        greaterThan.setLeftExpression(min);
        greaterThan.setRightExpression(value);
        boolean istrue = eval(greaterThan).toBool();
        min = istrue ? value : min;


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

    @Override
    public void init(PrimitiveValue[] dataRow) throws SQLException
    {
        this.current = dataRow;
        min = eval(expression);
        greaterThan = new GreaterThan(null,null);

    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
