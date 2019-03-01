package dubstep;

import java.io.*;
import java.sql.SQLException;
import net.sf.jsqlparser.expression.PrimitiveValue;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.select.Select;

public class Main
{

    public static Statement statement;

    public static void main(String args[]) throws ParseException, IOException, SQLException
    {
        CCJSqlParser parser = new CCJSqlParser(System.in);
        System.out.println("$> ");

        while ((statement = parser.Statement()) != null)
        {
            if (statement instanceof Select)
            {
                SelectProcessor  selectProcessor = new SelectProcessor(((Select) statement).getSelectBody());
                RowTraverser RowIterator = selectProcessor.processQuery();
                printResult(RowIterator);

            } else if (statement instanceof CreateTable)
            {
                String TableName = ((CreateTable) statement).getTable().getWholeTableName();
                TableInformation.addTableInfo(TableName, (CreateTable) statement);
            }

            System.out.println("$> ");
        }
    }

    public static void printResult(RowTraverser rowIterator)throws SQLException,IOException
    {
        while (rowIterator != null)
        {
            PrimitiveValue[]  dataRow = rowIterator.next();

            if(dataRow != null)
            {
                String Line = dataRow[0].toRawString();

                for(int i =1; i < dataRow.length; i++)
                {
                    Line = Line + "|" + dataRow[i].toRawString();
                }
                System.out.println(Line);

            }
            else
            {
                return;
            }
        }
    }
}