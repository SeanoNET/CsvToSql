




using CsvDataLoader;
using Microsoft.Extensions.Configuration;

var configuration = new ConfigurationBuilder()
                        .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                        .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
                        .Build();


var csvPath = @"C:\temp\test.csv";


var loader = new CsvFile(configuration["SQLServer"])
    .RetrieveFileFromFTP(csvPath, configuration.GetSection("FTP")["Host"], configuration.GetSection("FTP")["User"], configuration.GetSection("FTP")["Password"])
    .WriteToSqlTable("Test");