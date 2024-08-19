using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using Npgsql;
using System.Threading.Tasks;
using System.IO;
using System.Net;
using System.Net.Mail;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json.Linq;

namespace OrlovAV_Testovoe_Zadaniye
{
    internal class Program
    {       
        static void LogAndEmailResults(List<string> deletedFiles, List<string> emailRecipients, string emailSender, string emailPassword, string smtpHost, string smtpPort)
        {
            string logFilePath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + "\\" +"deleted_files_log.txt";

            if (!File.Exists(logFilePath))
                File.Create(logFilePath);
            else
                File.WriteAllText(logFilePath, "");

            File.WriteAllLines(logFilePath, deletedFiles);

            MailAddress from = new MailAddress(emailSender, "Artem");

            foreach (string recipient in emailRecipients)
            {
                MailAddress to = new MailAddress(recipient);
                MailMessage m = new MailMessage(from, to);

                m.Subject = "Список удаленных файлов и ошибок";
               
                m.Body = "Список файлов, на которые нет ссылок из других таблиц";

                m.Attachments.Add(new Attachment(logFilePath));
                string errorLogPath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + "\\" + "error_log.txt";
                if (File.ReadAllText(errorLogPath) != "")
                    m.Attachments.Add(new Attachment(errorLogPath));

                SmtpClient smtp = new SmtpClient(smtpHost, 587);

                smtp.Credentials = new NetworkCredential(emailSender, emailPassword);
                smtp.EnableSsl = true;

                try
                {
                    smtp.Send(m);
                    Console.WriteLine($"Письмо отпправлено на {recipient}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка отправки на {recipient}: {ex.Message}");
                }
            }          
        }

        static void LogError(string ex)
        {
            string errorLogPath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + "\\" + "error_log.txt";
            File.AppendAllText(errorLogPath, $"{DateTime.Now}: {ex}{Environment.NewLine}");
        }

        static List<string> FindNonUsedFiles(string connectionString, int subd)
        {
            List<string> tables = new List<string>(); 
            List<string> deletedFiles = new List<string>();
            string tableName = "File";

            string fileIdColName = "id_file";
            string otherTabFileIdColName = "file_id";
            string filePathColName = "path_File";

            if (subd == 1)
            {
                using (NpgsqlConnection connection = new NpgsqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();

                        string query = $"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '{otherTabFileIdColName}'";

                        NpgsqlCommand command = new NpgsqlCommand(query, connection);
                        using (NpgsqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                tables.Add(reader["TABLE_NAME"].ToString());
                            }
                        }

                        query = $"SELECT f.{filePathColName} FROM \"{tableName}\" f  ";

                        for (int i = 0; i < tables.Count; i++)
                        {
                            if (tables[i] != tableName)
                                query += $" LEFT JOIN \"{tables[i]}\" t{i+1} ON f.{fileIdColName} = t{i+1}.{otherTabFileIdColName}";
                        }

                        query += " WHERE";

                        for (int i = 0; i < tables.Count; i++)
                        {
                            if (tables[i] != tableName)
                            {
                                query +=$" t{i+1}.{otherTabFileIdColName} IS NULL";
                                if (i < tables.Count - 1)
                                {
                                    query += " AND";
                                }
                            }
                        }

                        command = new NpgsqlCommand(query, connection);

                        using (NpgsqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string filePath = reader.GetString(0);
                                deletedFiles.Add(filePath);
                                if (File.Exists(filePath))
                                {
                                    File.Delete(filePath);
                                }
                            }
                        }

                        foreach (string filePath in deletedFiles)
                        {
                            command.CommandText = $"DELETE FROM \"{tableName}\" WHERE {filePathColName} = '{filePath}'";
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }

                    catch (Exception ex)
                    {
                        LogError("Ошибка: " + ex.Message);
                    }
                    return deletedFiles;
                }
            }
            else
            {
                using (SqlConnection connection = new SqlConnection(connectionString))
                {
                    try
                    {
                        connection.Open();

                        string query = $"SELECT t.name AS TABLE_NAME FROM sys.tables AS t, sys.columns AS c WHERE t.OBJECT_ID = c.OBJECT_ID AND c.name LIKE '{otherTabFileIdColName}'";

                        SqlCommand command = new SqlCommand(query, connection);
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                tables.Add(reader["TABLE_NAME"].ToString());
                            }
                        }

                        query = $"SELECT f.{filePathColName} FROM \"{tableName}\" f  ";

                        for (int i = 0; i < tables.Count; i++)
                        {
                            if (tables[i] != tableName)
                                query += $" LEFT JOIN \"{tables[i]}\" t{i+1} ON f.{fileIdColName} = t{i+1}.{otherTabFileIdColName}";
                        }

                        query += " WHERE";

                        for (int i = 0; i < tables.Count; i++)
                        {
                            if (tables[i] != tableName)
                            {
                                query +=$" t{i+1}.{otherTabFileIdColName} IS NULL";
                                if (i < tables.Count - 1)
                                {
                                    query += " AND";
                                }
                            }
                        }

                        command = new SqlCommand(query, connection);

                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                string filePath = reader.GetString(0);
                                deletedFiles.Add(filePath);
                                if (File.Exists(filePath))
                                {
                                    File.Delete(filePath);
                                }
                            }
                        }

                        foreach (string filePath in deletedFiles)
                        {
                            command.CommandText = $"DELETE FROM \"{tableName}\" WHERE {filePathColName} = '{filePath}'";
                            command.ExecuteNonQuery();
                        }
                        connection.Close();
                    }

                    catch (Exception ex)
                    {
                        LogError("Ошибка: " + ex.Message);
                    }
                    return deletedFiles;
                }
            }
        }
        static List<string> GetEmailRecipients(string filePath)
        {
            List<string> emailRecipients = new List<string>();

            if (File.Exists(filePath))
            {
                string json = File.ReadAllText(filePath);
                JObject jObject = JObject.Parse(json);
                List<string> recipients = jObject["EmailRecipients"].ToObject<List<string>>();

                emailRecipients.AddRange(recipients);
            }
            return emailRecipients;
        }

        static void Main(string[] args)
        {
            string errFilePath = Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + "\\" + "error_log.txt";
            if (!File.Exists(errFilePath))
                File.Create(errFilePath);
            else
                File.WriteAllText(errFilePath, "");

            IConfigurationBuilder builder = new ConfigurationBuilder()
            .SetBasePath(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName)
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            string servName = configuration["Database:Server"];
            string dbName = configuration["Database:Database"];
            string userName = configuration["Database:User"];
            string password = configuration["Database:Password"];

            string connectionString = $"Server={servName};Database={dbName};User Id={userName};Password={password};";

            string emailSender = configuration["Email:Sender"];
            string emailPassword = configuration["Email:Password"];
            string smtpHost = configuration["Email:SmtpHost"];
            string smtpPort = configuration["Email:SmtpPort"];

            List<string> emailRecipients = GetEmailRecipients(Directory.GetParent(Directory.GetCurrentDirectory()).Parent.FullName + "\\" + "emailRecipients.json");

            int subd = 0;
            while (subd != 1 && subd != 2)
            {
                Console.WriteLine("Выберите СУБД:\n1-PostgreSQL\n2-MS SQL Server");
                try
                { 
                    subd = Convert.ToInt32(Console.ReadLine()); 
                }
                catch (Exception ex)
                {
                    LogError(ex.Message);
                }
            }

            LogAndEmailResults(FindNonUsedFiles(connectionString, subd), emailRecipients, emailSender, emailPassword, smtpHost, smtpPort);

            Console.WriteLine("Нажмите Enter для выхода");
            Console.ReadLine();
        }
    }
}
