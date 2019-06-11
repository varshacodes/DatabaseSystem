package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.update.Update;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

public class UpdateProcessor extends Evaluate
{
    Table table;
    List<Column>  columns;
    List<Expression> setExpressions;
    Expression whereExpression;
    PrimitiveValue[] current;
    HashMap<String,Integer> fieldMapping;

    public UpdateProcessor(Update updateStatement) throws SQLException, IOException
    {
        this.table = TableInformation.getTable(updateStatement.getTable().getWholeTableName());
        this.fieldMapping = table.getFieldPostionMapping();
        columns = updateStatement.getColumns();
        setExpressions = updateStatement.getExpressions();
        whereExpression = updateStatement.getWhere();
        processQuery();

    }

    private void processQuery() throws SQLException, IOException
    {
        DeleteProcessor deleteProcessor = new DeleteProcessor(table,whereExpression);
        procesUpdateInMemory();
        procesUpdateOnDisk();

    }

    private void procesUpdateOnDisk() throws IOException, SQLException
    {
        TupleTraverser tupleReader = new TupleTraverser(table.getTableName(),false,null);

        current = tupleReader != null? tupleReader.next(): null;

        while (current != null)
        {
            if(eval(whereExpression).toBool())
            {
                updateValues(current);
                table.insertRow(current);
            }

            current = tupleReader.next();
        }


    }

    private void procesUpdateInMemory() throws SQLException
    {
        if(table.hasUpdates())
        {
            UpdatesInfo updatesInfo = table.getUpdatesInfo();

            if(updatesInfo.isHasInserts())
            {
                List<PrimitiveValue[]> rows = updatesInfo.getDataRows();

                for(int i=0; i < rows.size(); i++)
                {
                    current = rows.get(i);

                    if(eval(whereExpression).toBool())
                    {
                       updateValues(current);

                    }
                }

            }
        }


    }

    private void updateValues(PrimitiveValue[] current) throws SQLException
    {
        for(int i=0; i < columns.size();i++)
        {
            int position = fieldMapping.get(columns.get(i).toString());
            current[position] = eval(setExpressions.get(i));
        }
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldMapping.get(column.toString());
        return current[position];
    }
}
