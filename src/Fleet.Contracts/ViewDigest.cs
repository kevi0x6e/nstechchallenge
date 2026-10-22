using System.Security.Cryptography;
using System.Text;

namespace Fleet.Contracts;

/// <summary>
/// Canonical SHA-256 of a whole vehicle view (keys and fields sorted ordinally).
/// Two caches are identical iff their digests are equal: this is the replay oracle.
/// </summary>
public static class ViewDigest
{
    public static string Compute(IEnumerable<KeyValuePair<string, IReadOnlyDictionary<string, string>>> hashes)
    {
        ArgumentNullException.ThrowIfNull(hashes);
        using var sha = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);

        foreach (var (key, fields) in hashes.OrderBy(h => h.Key, StringComparer.Ordinal))
        {
            sha.AppendData(Encoding.UTF8.GetBytes($"{key}\n"));
            foreach (var (field, value) in fields.OrderBy(f => f.Key, StringComparer.Ordinal))
            {
                sha.AppendData(Encoding.UTF8.GetBytes($"  {field}={value}\n"));
            }
        }

        return Convert.ToHexStringLower(sha.GetHashAndReset());
    }
}
