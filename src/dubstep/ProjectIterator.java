package dubstep;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.List;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

public class ProjectIterator extends Evaluate implements RowTraverser
{
    RowTraverser DataRowIterator;
    HashMap<String, Integer> PositionFieldMapping;
    List<SelectItem> selectItemList;
    HashMap<String,Integer> projectionMapping;
    PrimitiveValue[] current;
    boolean isAllColumns;



    ProjectIterator(RowTraverser DataRowIterator, List<SelectItem> selectItemList, boolean isAllColumns)
    {
        this.DataRowIterator = DataRowIterator;
        this.PositionFieldMapping = DataRowIterator.getFieldPositionMapping();
        this.isAllColumns = isAllColumns;
        if(!isAllColumns)
        {
            this.selectItemList = selectItemList;
        }

        setprojectionMapping();

    }
    @Override
    public int getNoOfFields()
    {
        return selectItemList.size();
    }
    private void setprojectionMapping()
    {
        if(isAllColumns)
        {
            this.projectionMapping = DataRowIterator.getFieldPositionMapping();
        }
        else {
             this.projectionMapping = new HashMap<>();

            for (int i = 0; i < selectItemList.size(); i++)
            {
                SelectExpressionItem selectItem = (SelectExpressionItem) selectItemList.get(i);
                String Alias = selectItem.getAlias();
                if(Alias == null)
                {
                    Alias = selectItem.toString();

                }

                projectionMapping.put(Alias, i);
            }
        }
    }

    public void updateProjectionMapping(String queryAlias)
    {
        HashMap<String, Integer> projectionMapping = new HashMap<>();
        Iterator<String> fieldNames = this.projectionMapping.keySet().iterator();

        while (fieldNames.hasNext())
        {
            String key = fieldNames.next();
            int position = this.projectionMapping.get(key);
            projectionMapping.put(queryAlias+"."+key,position);
        }
        this.projectionMapping = projectionMapping;
    }

    @Override
    public PrimitiveValue[] next() throws SQLException, IOException,ClassNotFoundException
    {
        if(isAllColumns)
        {
            current = DataRowIterator !=null ? DataRowIterator.next() : null;

            if (current != null)
            {
                return  current;
            }
        }
        else
        {
            current = DataRowIterator !=null ? DataRowIterator.next() : null;

            if(current != null)
            {
                PrimitiveValue[] ProjectRow = new PrimitiveValue[selectItemList.size()];

                for(int i=0; i< selectItemList.size(); i++)
                {
                    SelectExpressionItem selectItem = (SelectExpressionItem) selectItemList.get(i);

                    if(selectItem.getExpression() instanceof Function)
                    {
                        int position = PositionFieldMapping.get(selectItem.toString());
                        ProjectRow[i] =current[position];
                    }
                    else
                    {
                        ProjectRow[i] = eval(selectItem.getExpression());

                    }
                }
                return  ProjectRow;

            }
        }
        return  null;

    }



    public RowTraverser getChild()
    {
        return DataRowIterator;
    }

    public List<SelectItem> getSelectItemList()
    {
        return selectItemList;
    }

    public boolean isAllColumns()
    {
        return isAllColumns;
    }


    @Override
    public HashMap<String, Integer> getFieldPositionMapping()
    {
        return projectionMapping;
    }

    @Override
    public void close() throws IOException,ClassNotFoundException
    {
        DataRowIterator.close();

    }

    @Override
    public void reset() throws IOException,SQLException,ClassNotFoundException
    {
        DataRowIterator.reset();
    }

    public void setRowIterator(RowTraverser dataRowIterator)
    {
        DataRowIterator = dataRowIterator;
    }

    @Override
    public PrimitiveValue eval(Column column) throws SQLException
    {
        int position = PositionFieldMapping.get(column.toString());

        return current[position];
    }
}
