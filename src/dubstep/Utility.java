package dubstep;

import net.sf.jsqlparser.expression.PrimitiveValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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

    public static FieldType[] getListFieldType(List<Field> fieldList)
    {
        FieldType []fieldTypes = new FieldType[fieldList.size()];

        for(int i=0; i < fieldList.size(); i++)
        {
            fieldTypes[i] = fieldList.get(i).fieldType;
        }
        return fieldTypes;
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
}
