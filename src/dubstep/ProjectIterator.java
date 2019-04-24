package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import java.util.List;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;

public class ProjectIterator implements RowTraverser
{
    RowTraverser DataRowIterator;
    HashMap<String, Integer> PositionFieldMapping;
    List<SelectItem> selectItemList;
    HashMap<String,Integer> projectionMapping;
    PrimitiveValue[] current;
    boolean isAllColumns;

    ProjectIterator(RowTraverser DataRowIterator,List<SelectItem> selectItemList, boolean isAllColumns)
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
    private void setprojectionMapping()
    {
        if(isAllColumns)
        {
            this.projectionMapping = DataRowIterator.getFieldPositionMapping();
        }
        else {
             this.projectionMapping = new HashMap<>();

            for (int i = 0; i < selectItemList.size(); i++) {
                SelectExpressionItem selectItem = (SelectExpressionItem) selectItemList.get(i);
                String Alias = selectItem.getAlias();

                if (Alias == null) {
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
            PrimitiveValue[] dataRow = DataRowIterator !=null ? DataRowIterator.next() : null;

            if (dataRow != null)
            {
                return  dataRow;
            }
        }
        else
        {
            PrimitiveValue[] dataRow = DataRowIterator !=null ? DataRowIterator.next() : null;
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
                return  ProjectRow;

            }
        }
        return  null;

    }

    @Override
    public PrimitiveValue[] getcurrent()
    {
        return current;
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
}
