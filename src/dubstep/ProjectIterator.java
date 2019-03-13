package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.List;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;

public class ProjectIterator implements RowTraverser
{
    RowTraverser DataRowIterator;
    HashMap<String, Integer> PositionFieldMapping;
    List<SelectItem> selectItemList;
    HashMap<String,Integer> projectionMapping;

    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return projectionMapping;
    }

    @Override
    public void close() throws IOException
    {
        DataRowIterator.close();

    }

    @Override
    public void reset() throws IOException
    {
         DataRowIterator.reset();
    }


    ProjectIterator(RowTraverser DataRowIterator,List<SelectItem> selectItemList)
    {
        this.DataRowIterator = DataRowIterator;
        this.PositionFieldMapping = DataRowIterator.getFieldPositionMapping();
        this.selectItemList = selectItemList;
        setprojectionMapping();

    }

    private void setprojectionMapping()
    {
        this.projectionMapping = new HashMap<>();

        for(int i=0; i < selectItemList.size(); i++)
        {
            SelectExpressionItem selectItem = (SelectExpressionItem) selectItemList.get(i);
            String Alias= selectItem.getAlias();

            if(Alias == null)
            {
                Alias = selectItem.toString();

            }

            projectionMapping.put(Alias,i);
        }
    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException
    {
        if(DataRowIterator != null)
        {
            PrimitiveValue[] dataRow = DataRowIterator.next();

            if(dataRow != null)
            {
                PrimitiveValue[] ProjectRow = new PrimitiveValue[selectItemList.size()];

                for(int i=0; i< selectItemList.size(); i++)
                {
                    SelectExpressionItem selectItem = (SelectExpressionItem) selectItemList.get(i);
                    ExpressionEvaluator evaluator = new ExpressionEvaluator(dataRow, PositionFieldMapping);
                    PrimitiveValue value = evaluator.eval(selectItem.getExpression());
                    ProjectRow[i] = value;
                }
                return ProjectRow;
            }
        }

        return null;
    }

    @Override
    public boolean hasNext() throws IOException
    {
        if(DataRowIterator.hasNext())
        {
            return true;
        }
        else {

            return false;
        }

    }
}
