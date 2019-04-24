package dubstep;

import java.io.Serializable;

public class Field implements Serializable
{
    String columnName;
    int position;
    FieldType fieldType;

    public Field(String columnName, int position, FieldType fieldType)
    {
        this.columnName = columnName;
        this.position = position;
        this.fieldType = fieldType;
    }


    public String getColumnName()
    {
        return columnName;
    }

    public int getPosition()
    {
        return position;
    }

    public FieldType getFieldType()
    {
        return fieldType;
    }

    @Override
    public String toString()
    {
        return "Field{" +
                "columnName='" + columnName + '\'' +
                ", position=" + position +
                ", fieldType=" + fieldType +
                '}';
    }
}
