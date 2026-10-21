using System.Globalization;

namespace Fleet.Contracts;

/// <summary>
/// Builds order-preserving version tokens compared inside Valkey Lua scripts.
/// </summary>
/// <remarks>
/// Tokens are fixed-width, zero-padded decimal strings, so lexicographic order equals
/// numeric order. Comparing strings (not Lua numbers) avoids the 2^53 precision loss
/// Lua doubles would introduce for 64-bit LSNs and microsecond timestamps.
/// </remarks>
public static class VersionToken
{
    private const string Width = "D20";

    /// <summary>State-group version: the Debezium <c>source.lsn</c> of the change.</summary>
    public static string FromLsn(long lsn)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(lsn);
        return lsn.ToString(Width, CultureInfo.InvariantCulture);
    }

    /// <summary>Position-group version: business time first, position id as a deterministic tie-break.</summary>
    public static string FromPosition(DateTimeOffset recordedAt, long positionId)
    {
        var micros = (recordedAt.UtcTicks - DateTimeOffset.UnixEpoch.UtcTicks) / 10;
        ArgumentOutOfRangeException.ThrowIfNegative(micros, nameof(recordedAt));
        ArgumentOutOfRangeException.ThrowIfNegative(positionId);
        return string.Create(CultureInfo.InvariantCulture, $"{micros:D20}:{positionId:D20}");
    }
}
