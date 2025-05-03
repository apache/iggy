namespace Iggy_SDK.Configuration;

public class TlsSettings
{
    public bool Enabled { get; set; } 
    public string Hostname { get; set; }
    public bool Authenticate { get; set; } = false;
}