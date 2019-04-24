package dubstep;

public enum ConditionType
{
    TOP(101),LEFT(102),RIGHT(103),JOIN(104);

    int conditionType;

    ConditionType(int type)
    {
        conditionType = type;
    }

}
