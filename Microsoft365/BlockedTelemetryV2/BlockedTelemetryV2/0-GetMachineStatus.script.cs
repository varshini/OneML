using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using ScopeRuntime;
using System.Text.RegularExpressions;

public class Utils
{

    private static readonly int GuidLength = Guid.Empty.ToString().Length;
    private const int OlsLicenseIdLength = 16;
    /// <summary>
    /// Trims the OLS LicenseId we get from rule 3 so it doesn't include the checksum.
    /// </summary>
    public static string TrimLicenseId(string licenseId)
    {
        if (String.IsNullOrWhiteSpace(licenseId))
        {
            return licenseId;
        }
        if (licenseId.Length >= GuidLength)
        {
            return licenseId.Substring(0, GuidLength);
        }
        if (licenseId.Length >= OlsLicenseIdLength)
        {
            return licenseId.Substring(0, OlsLicenseIdLength);
        }
        return licenseId;
    }
}