package dubstep;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.delete.Delete;
import java.sql.SQLException;
import java.util.List;
import java.util.HashMap;


public class DeleteProcessor extends Evaluate
{

    Table table;
    Expression whereCondition;
    PrimitiveValue[] current;
    HashMap<String,Integer> fieldPositionMapping;

    public DeleteProcessor(Delete deleteStatement) throws SQLException
    {
        this.table = TableInformation.getTable(deleteStatement.getTable().getWholeTableName());
        this.whereCondition = deleteStatement.getWhere()!=null ? deleteStatement.getWhere(): null;
        this.fieldPositionMapping = table.getFieldPostionMapping();
        processDeleteInMemory();
    }
    public DeleteProcessor(Table table,Expression whereCondition) throws SQLException
    {
        this.table = table;
        this.whereCondition = whereCondition;

    }

    private void processDeleteInMemory() throws SQLException
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

                    if(eval(whereCondition).toBool())
                    {
                        rows.remove(i);
                        i = i-1;
                    }
                }

                if(rows.isEmpty())
                {
                    updatesInfo.setHasInserts(false);
                }

            }
        }

        table.deleteRows(whereCondition);


    }


    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = fieldPositionMapping.get(column.toString());
        return current[position];
    }
}
