package dubstep;

import net.sf.jsqlparser.eval.Eval;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import java.sql.SQLException;
import java.util.HashMap;

public class ExpressionEvaluator extends Eval
{
    PrimitiveValue[] dataRow;
    HashMap<String,Integer> FieldPostionMapping;

    public ExpressionEvaluator(PrimitiveValue[] dataRow, HashMap<String, Integer> FieldPostionMapping)
    {
        this.dataRow = dataRow;
        this.FieldPostionMapping = FieldPostionMapping;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        String columnName = column.getWholeColumnName();
        int position = FieldPostionMapping.get(columnName);
        return  dataRow[position];
    }
}
