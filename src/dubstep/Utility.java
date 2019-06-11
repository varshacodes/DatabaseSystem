package dubstep;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Utility
{

    private static long FileStart = 0;


    public static long getFileStart()
    {
        FileStart = FileStart + 1000;
        return FileStart;

    }
    public static String getLine(PrimitiveValue[] dataRow)
    {
        if(dataRow[0]!=null)
        {
            String Line = dataRow[0].toRawString();

            for (int i = 1; i < dataRow.length; i++) {
                Line = Line + "|" + dataRow[i].toRawString();
            }
            return Line + '\n';

        }
        return "";

    }
    public static String getLines(ArrayList<PrimitiveValue[]> dataRows)
    {
        String Line = "";

        for(PrimitiveValue[] dataRow: dataRows)
        {
            Line = Line + getLine(dataRow);
        }

        return Line;

    }


    public static int getMaxPosition(HashMap<String,Integer> positionMapping)
    {
        Iterator<String> Fields = positionMapping.keySet().iterator();
        int max =-1;

        while(Fields.hasNext())
        {
            String key = Fields.next();
            int position = positionMapping.get(key);
            if(position > max)
            {
                max = position;
            }
        }

        return max;
    }

    public static boolean isTableInfoAvailable()
    {
        String file = Table.getDirectory()+"Tables/"+Table.getInfoFileName();
        File TableInfoFile = new File(file);

        if(!TableInfoFile.exists())
        {
            return false;
        }
        else
        {
            return true;

        }
    }

    public static FieldType[] getFieldTypes(PrimitiveValue[] dataRow)
    {
        FieldType[] fieldTypes = new FieldType[dataRow.length];

        for(int i=0; i < dataRow.length; i++)
        {
            fieldTypes[i] = FieldType.getFieldType(dataRow[i]);
        }

        return fieldTypes;
    }

    public  static void eval(Expression expression, Set<String> Columns) throws SQLException
    {
        /* Implemented Using Reference From Evalib Eval
        * */

             if(expression instanceof Column)
             {
                Columns.add(expression.toString());
                return;
             }
             else if(expression instanceof BinaryExpression)
             {
                 eval(((BinaryExpression) expression).getLeftExpression(),Columns);
                 eval(((BinaryExpression) expression).getRightExpression(),Columns);
             }
             else if(expression instanceof Function)
             {
                 if(((Function) expression).isAllColumns())
                 {
                     Columns.add("*");
                 }
                 else {
                         expression = ((Function) expression).getParameters().getExpressions().get(0);
                         eval(expression, Columns);
                 }
             }
             else if(expression instanceof PrimitiveValue)
             {
                 return;
             }
             else if(expression instanceof Between)
             {
                 eval(((Between) expression).getLeftExpression(),Columns);
                 eval(((Between) expression).getBetweenExpressionStart(),Columns);
                 eval(((Between) expression).getBetweenExpressionEnd(),Columns);
             }
            else if(expression instanceof CaseExpression)
            {
                eval((CaseExpression)expression,Columns);
            }
            else if(expression instanceof WhenClause)
            {
                 eval(((WhenClause) expression).getWhenExpression(),Columns);
                 eval(((WhenClause) expression).getThenExpression(),Columns);

            }
            else {

                 throw new SQLException("Expression: "+ expression +" Not Supported");
             }

        }




    public static void eval(CaseExpression caseExpression,Set<String> Columns) throws SQLException
    {
        if(caseExpression.getSwitchExpression() == null)
        {
            for(Object ow : caseExpression.getWhenClauses())
            {
                WhenClause w = ((WhenClause)ow);
                eval(w.getWhenExpression(),Columns);
                eval(w.getThenExpression(),Columns);
            }
        }
        else {
            eval(caseExpression.getSwitchExpression(),Columns);
            for(Object ow : caseExpression.getWhenClauses())
            {
                WhenClause w = ((WhenClause)ow);
                eval(w.getWhenExpression(),Columns);
                eval(w.getThenExpression(),Columns);
            }
            }
        if(caseExpression.getElseExpression() != null)
        {
            eval(caseExpression.getElseExpression(),Columns);
        }
    }


}
