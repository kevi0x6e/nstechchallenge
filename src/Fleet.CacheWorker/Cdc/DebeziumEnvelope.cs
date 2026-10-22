namespace Fleet.CacheWorker.Cdc;

/// <summary>
/// The subset of a Debezium change-event value this worker relies on
/// (JSON converter, <c>schemas.enable=false</c>). Unknown fields are ignored.
/// </summary>
/// <param name="Op">c = create, u = update, d = delete, r = snapshot read.</param>
public sealed record DebeziumEnvelope<TRow>(TRow? Before, TRow? After, DebeziumSource Source, string Op)
    where TRow : class;

/// <param name="Lsn">Postgres WAL position of the change: the log order of the source.</param>
/// <param name="TsMs">Commit time in the source DB (informational only, never used for ordering).</param>
public sealed record DebeziumSource(long Lsn, long TsMs, string? Table);

public static class DebeziumOp
{
    public const string Create = "c";
    public const string Update = "u";
    public const string Delete = "d";
    public const string Read = "r";
}
