package dubstep;

public enum Aggregate
{
    SUM(101),COUNT(102),MIN(103),MAX(104),AVG(104),DISTINCT(105);

    int AggregateType;

    Aggregate(int type)
    {
        AggregateType = type;
    }

    public static Aggregate getAggregate(String aggType)
    {
        aggType = aggType.toLowerCase();

        switch(aggType)
        {
            case "sum": return Aggregate.SUM;
            case "count": return Aggregate.COUNT;
            case "min": return Aggregate.MIN;
            case "max": return Aggregate.MAX;
            case "avg": return Aggregate.AVG;
            case "distinct": return Aggregate.DISTINCT;
        }

        return null;
    }
}
