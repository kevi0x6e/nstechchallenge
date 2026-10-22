using System.Globalization;

namespace Fleet.Scenario;

/// <summary>Minimal <c>--name value</c> parser shared by the CLI tools.</summary>
public sealed class CliArgs
{
    private readonly Dictionary<string, string> _values;

    private CliArgs(Dictionary<string, string> values) => _values = values;

    public static CliArgs Parse(IEnumerable<string> args)
    {
        var list = args.ToList();
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < list.Count - 1; i++)
        {
            if (list[i].StartsWith("--", StringComparison.Ordinal))
            {
                values[list[i][2..]] = list[++i];
            }
        }

        return new CliArgs(values);
    }

    public string? Get(string name) => _values.GetValueOrDefault(name);

    public int Get(string name, int fallback) =>
        _values.TryGetValue(name, out var v) ? int.Parse(v, CultureInfo.InvariantCulture) : fallback;

    public double Get(string name, double fallback) =>
        _values.TryGetValue(name, out var v) ? double.Parse(v, CultureInfo.InvariantCulture) : fallback;
}
