package dubstep;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.insert.Insert;
import java.util.HashMap;
import java.util.List;

public class InsertProcessor
{
    Table Table;

    public InsertProcessor(Insert insertStatement)
    {
        this.Table= TableInformation.getTable(insertStatement.getTable().getWholeTableName());
        processInsert(insertStatement.getColumns(),(ExpressionList) insertStatement.getItemsList());
    }

    private void processInsert(List<Column> columns, ExpressionList itemsList)
    {
        HashMap<String, PrimitiveValue> dataValues = getHashMap(columns,itemsList);
        this.Table.insertRow(dataValues);
    }

    private HashMap<String, PrimitiveValue> getHashMap(List<Column> columns,  ExpressionList itemsList)
    {
        HashMap<String, PrimitiveValue> dataValues = new HashMap<String,PrimitiveValue>();
        for(int i=0; i < columns.size(); i++)
        {
            String column = columns.get(i).getColumnName();
            PrimitiveValue value = (PrimitiveValue)itemsList.getExpressions().get(i);
            dataValues.put(column,value);

        }
        return dataValues;
    }


}
