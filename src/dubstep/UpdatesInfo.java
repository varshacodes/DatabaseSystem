package dubstep;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class UpdatesInfo
{
    boolean hasInserts;
    boolean hasDeletes;
    boolean hasColumns;
    List<PrimitiveValue[]> dataRows;
    Expression deleteExpression;
    Set<String> columns;

    public UpdatesInfo()
    {
        hasInserts = false;
        hasDeletes = false;
        dataRows = new ArrayList<>();
        columns = null;

    }

    public boolean isHasInserts()
    {
        return hasInserts;
    }

    public boolean isHasDeletes()
    {
        return hasDeletes;
    }

    public void setHasInserts(boolean hasInserts)
    {
        this.hasInserts = hasInserts;
    }

    public List<PrimitiveValue[]> getDataRows()
    {
        return dataRows;
    }

    public void insertRow(PrimitiveValue[] dataRow)
    {
        hasInserts = true;
        dataRows.add(dataRow);
    }

    public Expression getDeleteExpression() {
        return deleteExpression;
    }

    public boolean isHasColumns() {
        return hasColumns;
    }

    public Set<String> getColumns() {
        return columns;
    }

    public void deleteRows(Expression OnCondition)throws SQLException
    {
        hasDeletes = true;

        if(deleteExpression == null)
        {
            deleteExpression = new InverseExpression(OnCondition);
        }
        else
        {
            deleteExpression = new AndExpression(deleteExpression,new InverseExpression(OnCondition));
        }
        addToColumns(OnCondition);

    }

    public void addToColumns(Expression OnCondition)throws SQLException
    {
        hasColumns = true;
        if(columns == null)
        {
            columns = new HashSet<>();
        }

        Utility.eval(OnCondition,columns);
    }







}
