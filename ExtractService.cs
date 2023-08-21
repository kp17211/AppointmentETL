using AppointmentReminderFunction.Interfaces;
using AppointmentReminderFunction.Models;
using AppointmentReminderFunction.Repositories;
using CsvHelper;
using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Renci.SshNet;
using Renci.SshNet.Sftp;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;


namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Extractor Service, Download the file from SFTP or Azure and extract the data, transform the data and load into database
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>03-May-2023</createdDate>
    public class ExtractService : IExtractService
    {
        #region ctor
        private readonly ILogger<ExtractService> _logger;
        private readonly IJobService _jobService;
        private ITransformService _transformService;
        private readonly ILoadService _loadService;
        private readonly IFileService _fileService;
        private readonly IAppointmentRepository _repository;
        private readonly IFileRepository _filerepository;

        public ExtractService(ILogger<ExtractService> logger, IJobService jobService, ITransformService transformService, ILoadService loadService, IFileService fileService, IAppointmentRepository repository, IFileRepository filerepository)
        {
            _logger = logger;
            _jobService = jobService;
            _transformService = transformService;
            _loadService = loadService;
            _fileService = fileService;
            _repository = repository;
            _filerepository = filerepository;
        }
        #endregion

        #region Public Methods
        
        /// <summary>
        /// Process file from SFTP
        /// </summary>
        /// <param name="config">Configuration data for SFTP</param>
        /// <returns>Task Status</returns>
        public async Task ProcessSFTPETL(ForecastConfig config)
        {
            try
            {
                _logger.LogInformation($"Reading File at " + DateTime.UtcNow.ToString());
                using (var sftp = GetSftpClient(config.SFTPConfig))
                {
                    _logger.LogInformation($"Start connecting to SFTP {config.SFTPConfig.KeyFileName}");

                    sftp.Connect();

                    _logger.LogInformation($"SFTP Connected {config.SFTPConfig.KeyFileName}");

                    var listDirectory = sftp.ListDirectory($"/{config.SFTPConfig.HomeDirectory}{(string.IsNullOrEmpty(config.SFTPConfig.AppointmentDirectory) ? "" : "/")}{config.SFTPConfig.AppointmentDirectory}");

                    _logger.LogInformation($"SFTP Directory Changed to /{config.SFTPConfig.HomeDirectory}/{config.SFTPConfig.AppointmentDirectory}");

                    foreach (var fi in listDirectory)
                    {
                        _logger.LogInformation($"SFTP File {fi.FullName}");
                        if (fi.IsRegularFile)
                        {
                            try
                            {
                                sftp.ChangeDirectory($"/{config.SFTPConfig.HomeDirectory}{(string.IsNullOrEmpty(config.SFTPConfig.AppointmentDirectory) ? "" : "/")}{config.SFTPConfig.AppointmentDirectory}");

                                var fileName = fi.Name;
                                
                                _logger.LogInformation($"Reading file {fi.Name}");

                                await ProcessStream(config, sftp.OpenRead(fi.FullName), fileName);

                                MoveFile(config.SFTPConfig, sftp, fi);
                            }
                            catch (Exception e)
                            {
                                _logger.LogInformation($"Error in files execution {e.Message}");

                            }
                        }
                    }

                    if (sftp.IsConnected)
                        sftp.Disconnect();
                }
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error in execution {ex.Message}");
            }
        }

        /// <summary>
        /// Process file from Azure Blob Container
        /// </summary>
        /// <param name="config">Configuration data for SFT</param>
        /// <returns>Task Status</returns>
        public async Task ProcessAzureETL(ForecastConfig config)
        {
            try
            {
                _logger.LogInformation($"Reading File at " + DateTime.UtcNow.ToString());
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(config.SFTPConfig.AzureWebStorage);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer containerSource = blobClient.GetContainerReference(config.SFTPConfig.AppointmentDirectory);
                CloudBlobContainer containerDestination = blobClient.GetContainerReference(config.SFTPConfig.TargetDirectory);

                BlobContinuationToken continuationToken = null;
               
                //Use maxResultsPerQuery to limit the number of results per query as desired. `null` will have the query return the entire contents of the blob container
                int? maxResultsPerQuery = null;

                do
                {
                    var response = await containerSource.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.None, maxResultsPerQuery, continuationToken, null, null);
                    continuationToken = response.ContinuationToken;
                    _logger.LogInformation($"Reading the file from blob ");

                    foreach (CloudAppendBlob blob in response.Results.OfType<CloudAppendBlob>())
                    {
                        using (var steam = new MemoryStream())
                        {
                            MemoryStream fileStream = new MemoryStream();
                            
                            await blob.DownloadToStreamAsync(fileStream);
                            fileStream.Position = 0;

                            string fileName = blob.Name;
                            string newFileName = DateTime.UtcNow.ToString("MM-dd-yyyy-HH-mm-ss") + "_" + fileName;
                            
                            await ProcessStream(config, fileStream, fileName);

                            CloudBlockBlob existingblob = containerSource.GetBlockBlobReference(fileName);
                            CloudBlockBlob blobCopy = containerDestination.GetBlockBlobReference(newFileName);
                            try
                            {
                                _logger.LogInformation($"Start file moving");
                                await blobCopy.StartCopyAsync(existingblob);
                            }
                            catch (Exception ex)
                            {
                                _logger.LogInformation($"Error in file moving : {ex.Message}");
                            }
                            try
                            {
                                _logger.LogInformation($"Start file Deleting");
                                await existingblob.DeleteIfExistsAsync();
                            }
                            catch (Exception ex)
                            {
                                _logger.LogInformation($"Error in file Deleting : {ex.Message}");
                            }
                        }
                    }
                } while (continuationToken != null);
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error in execution {ex.Message}");
            }
        }
        #endregion

        #region Private Methods
        
        /// <summary>
        /// Process the File Stream Extract data Transform in known formate and Load into database
        /// </summary>
        /// <param name="config">Configuration Data</param>
        /// <param name="fileStream">File stream</param>
        /// <param name="fileName">File Name</param>
        /// <returns>Task Status</returns>
        private async Task ProcessStream(ForecastConfig config, Stream fileStream, string fileName)
        {
            try
            {
                MemoryStream memory = new MemoryStream();
                await fileStream.CopyToAsync(memory);
                memory.Position = 0;
                fileStream.Position = 0;

                using (StreamReader stream = new StreamReader(fileStream))
                {
                    var data = await ReadData(config, fileName, stream);
                    if (data == null || (data != null && data.Count() <= 0))
                        return;

                    switch (config.Client.ClientId.ToString().ToLower())
                    {
                        //Client 2
                        case "dca42d06-155d-46e3-8194-e47549f042a5":
                            _transformService = new Client2TransformService(_logger, _repository);
                            break;
                        //Client 1
                        case "61a04be0-c0a0-440b-a395-01c1ec49c882":
                            _transformService = new Client1TransformService(_logger);
                            break;
                        default:
                            _transformService = new TransformService();
                            break;
                    }
                    var languages = await _filerepository.GetAllLanguageCode();
                    _transformService.Languages = languages;

                    var errors = await _transformService.TransformData(data, config.DateFormat);
                    var extension = Path.GetExtension(fileName);
                    var name = fileName.Substring(0, fileName.LastIndexOf('.'));

                    if (errors != null && errors.Count() <= 0)
                    {
                        await _loadService.LoadAppStatus(data, config.Client.Id);
                        await _loadService.LoadAppTypes(data, config.Client.Id);
                        await _loadService.LoadPractitioners(data, config.Client.Id);
                        await _loadService.LoadLocations(data, config.Client.Id);

                        var job = new JobBlob()
                        {
                            Id = Guid.NewGuid(),
                            Description = $"Appointment Import",
                            Status = JobStatus.Scheduled,
                            DateTimeScheduled = DateTime.UtcNow.AddMinutes(15)
                        };
                        await _jobService.CreateJob(name, extension, job, data, config.Client, config.Settings);
                    }
                    else
                    {
                        var job = new JobBlob()
                        {
                            Id = Guid.NewGuid(),
                            Status = JobStatus.Failed,
                            Description = $"Error in file: {String.Join(",", errors.ToArray())}"
                        };
                        await _jobService.CreateJob(name, extension, job, data, config.Client, config.Settings);
                    }

                    var streamLength = memory.Length;
                    var blobFile = await _fileService.SaveFile(name, extension, memory, config.Settings);

                    if (blobFile != null)
                    {
                        await _fileService.SaveFileMetaData(fileName, streamLength, blobFile.Url, blobFile.BlobName, $"{blobFile.Notes} - reading", blobFile.MimeType, config.Client.Id);
                    }
                    else
                    {
                        _logger.LogInformation($"Error in creating files records in DB");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogInformation($"Error in Processing Stream files {ex.Message}");
            }
        }

        /// <summary>
        /// Read the File and Load the Data into the List of FileTemplate
        /// </summary>
        /// <param name="config">Configuration Data</param>
        /// <param name="fileName">File Name</param>
        /// <param name="stream">File Stream</param>
        /// <returns>Task Status</returns>
        private async Task<List<FileTemplate>> ReadData(ForecastConfig config, string fileName, StreamReader stream)
        {
            var response = new ProvidertechResponse<string>();

            List<FileTemplate> records = null;

            var csvConfig = new CsvHelper.Configuration.CsvConfiguration(CultureInfo.InvariantCulture)
            {
                ShouldSkipRecord = record => record.Row.Parser.Record.All(field => String.IsNullOrWhiteSpace(field) || field == "null"),
                DetectDelimiter = true,
                IgnoreBlankLines = true,
                BadDataFound = context =>
                {
                    _logger.LogError($"Error in Record : {context.RawRecord}");
                },
                PrepareHeaderForMatch = header => Regex.Replace(header.Header.ToLower(), @"\s", string.Empty),
                TrimOptions = CsvHelper.Configuration.TrimOptions.Trim
            };

            var extension = Path.GetExtension(fileName);
            var name = fileName.Substring(0, fileName.LastIndexOf('.'));

            try
            {
                using (var csv = new CsvReader(stream, csvConfig))
                {
                    csv.Context.RegisterClassMap<FileTemplateMap>();
                    records = csv.GetRecords<FileTemplate>().ToList();
                }
                return records;
            }
            catch (Exception ex)
            {
                response.Errors = new[] { ex.Message };
                var jobBlob = new JobBlob()
                {
                    Id = Guid.NewGuid(),
                    Status = JobStatus.Failed,
                    Description = $"SFTP | Error in reading file: {ex.Message}"
                };
                _logger.LogError($"Error in Reading file : {ex.Message}");
                await _jobService.CreateJob(name, extension, jobBlob, records, config.Client, config.Settings);

                return null;
            }
        }

        /// <summary>
        /// Move File in SFTP to Processed Directory
        /// </summary>
        /// <param name="config">Configuration Data</param>
        /// <param name="sftp">SFTP Client</param>
        /// <param name="file">SFTP File</param>
        /// <returns>Task Status</returns>
        private bool MoveFile(SFTPConfig config, SftpClient sftp, SftpFile file)
        {
            bool success = true;
            _logger.LogInformation($"Moving the file : {file.Name}");
            try
            {
                if (sftp.IsConnected)
                {
                    sftp.ChangeDirectory("/");
                    file.MoveTo('/' + config.HomeDirectory + '/' + config.TargetDirectory + '/' + DateTime.UtcNow.ToString("MM-dd-yyyy-HH-mm-ss") + "_" + file.Name);
                }
                else
                {
                    sftp.Connect();
                    sftp.ChangeDirectory("/");
                    file.MoveTo('/' + config.HomeDirectory + '/' + config.TargetDirectory + '/' + DateTime.UtcNow.ToString("MM-dd-yyyy-HH-mm-ss") + "_" + file.Name);
                }
            }
            catch (Exception e)
            {
                success = false;
                _logger.LogInformation($"Error in moving the file  - {e.Message}");
            }
            return success;
        }
        
        /// <summary>
        /// Get the SFTP Client Object
        /// </summary>
        /// <param name="config">Configuration Data</param>
        /// <returns>SFTP Client</returns>
        private SftpClient GetSftpClient(SFTPConfig config)
        {
            AuthenticationMethod authentication;
            Renci.SshNet.ConnectionInfo connectionInfo;
            var fileData = (byte[])AppointmentReminderFunction.Properties.Resources.ResourceManager.GetObject(config.KeyFileName);

            if (config.PrivateKeyAuthentication.HasValue && config.PrivateKeyAuthentication.Value)
                authentication = new PrivateKeyAuthenticationMethod(config.UserName, new PrivateKeyFile(new MemoryStream(fileData)));
            else
                authentication = new PasswordAuthenticationMethod(config.UserName, config.Password);

            if (config.Port > 0)
                connectionInfo = new Renci.SshNet.ConnectionInfo(config.Host, config.Port.Value, config.UserName, authentication);
            else
                connectionInfo = new Renci.SshNet.ConnectionInfo(config.Host, config.UserName, authentication);

            return new SftpClient(connectionInfo);
        }
        #endregion
    }
}
