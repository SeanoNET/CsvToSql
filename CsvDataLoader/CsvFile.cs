using CsvHelper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CsvDataLoader
{
    internal class CsvFile
    {
        private string _connectionString;
        private string _fileLocation;


        private Byte[] _data;

        public CsvFile(string connectionString)
        {
            this._connectionString = connectionString;
        }


        public CsvFile RetrieveFileFromLocal(string fileLocation)
        {
            _data = File.ReadAllBytes(fileLocation);
            return this;
        }
        public CsvFile RetrieveFileFromFTP(string remotePath, string host, string user, string password)
        {
            DownloadFtpFile(host+remotePath, new NetworkCredential(user, password));
            return this;
        }

        public CsvFile WriteToSqlTable(string tableName)
        {
            using (var reader = new StreamReader(new MemoryStream(_data)))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();
                string[] headerRows = csv.HeaderRecord;
                var dataReader = new CsvDataReader(csv);

                BulkCopy(tableName, dataReader, headerRows);
            }
            return this;
        }

        private void DownloadFtpFile(string remotePath, NetworkCredential credentials)
        {
            FtpWebRequest downloadRequest = (FtpWebRequest)WebRequest.Create(remotePath);
            downloadRequest.Method = WebRequestMethods.Ftp.DownloadFile;
            downloadRequest.Credentials = credentials;

            using (FtpWebResponse downloadResponse = (FtpWebResponse)downloadRequest.GetResponse())
            using (Stream sourceStream = downloadResponse.GetResponseStream())
            using (MemoryStream ms = new MemoryStream())
            {
                byte[] buffer = new byte[10240];
                int read;
                while ((read = sourceStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                _data = ms.ToArray();
            }
        }
        private void BulkCopy(string tableName, IDataReader dataReader, string[] columns, Action<SqlBulkCopy> configureSqlBulkCopy = null)
        {




            using (SqlConnection dbConnection = new SqlConnection(_connectionString))
            {
                dbConnection.Open();

                //Create table if doesnt exist
                var checkTableIfExistsCommand = new SqlCommand("IF EXISTS (SELECT 1 FROM sysobjects WHERE name =  '" + tableName + "') SELECT 1 ELSE SELECT 0", dbConnection);
                var exists = checkTableIfExistsCommand.ExecuteScalar().ToString().Equals("1");

                if (!exists)
                {
                    var createTableBuilder = new StringBuilder("CREATE TABLE [" + tableName + "]");
                    createTableBuilder.AppendLine("(");

                    foreach (string column in columns)
                    {
                        createTableBuilder.AppendLine("  [" + column + "] VARCHAR(MAX),");
                    }

                    createTableBuilder.Remove(createTableBuilder.Length - 1, 1);
                    createTableBuilder.AppendLine(")");

                    var createTableCommand = new SqlCommand(createTableBuilder.ToString(), dbConnection);
                    createTableCommand.ExecuteNonQuery();
                }
                else
                {
                    var truncate = new SqlCommand("TRUNCATE TABLE  " + tableName, dbConnection);
                    truncate.ExecuteScalar();
                }

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(dbConnection))
                {
                    bulkCopy.BatchSize = 3000; //Data will be sent to SQL Server in batches of this size
                    bulkCopy.EnableStreaming = true;
                    bulkCopy.DestinationTableName = tableName;

                    //This will ensure mapping based on names rather than column position
                    foreach (string column in columns)
                    {
                        bulkCopy.ColumnMappings.Add(column, column);
                    }

                    //If additional, custom configuration is required, invoke the action
                    configureSqlBulkCopy?.Invoke(bulkCopy);

                    try
                    {
                        // Write from the source to the destination.
                        bulkCopy.WriteToServer(dataReader);
                    }
                    finally
                    {
                        dataReader.Close();
                    }
                }
            }
        }
    }
}
