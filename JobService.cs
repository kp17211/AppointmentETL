using Microsoft.Extensions.Logging;
using AppointmentReminderFunction.Repositories;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using AppointmentReminderFunction.Models;
using System.IO;
using System.Globalization;
using System.Text;
using Azure.Storage.Blobs;
using CsvHelper.Configuration;
using CsvHelper;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Job Service to create a job
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>05-May-2023</createdDate>
    public class JobService : IJobService
    {
        #region Ctor       
        private readonly IJobRepository _jobRepository;
        private readonly ILogger<JobService> _logger;
        private readonly IIntakeTriggerService _intakeTriggerService;
       
        public JobService(ILogger<JobService> logger, IJobRepository jobRepository,  IIntakeTriggerService intakeTriggerService)
        {
            _logger = logger;
            _jobRepository = jobRepository;
            _intakeTriggerService = intakeTriggerService;
        }
        #endregion

        #region Methods
       /// <summary>
       /// Create a Job and Trigger the Schedular
       /// </summary>
       /// <param name="protocolRuleId">Protocol Rule Id</param>
       /// <param name="protocolName">Protocol Rule Name</param>
       /// <param name="records">Patient Records</param>
       /// <param name="client">Client Object</param>
       /// <returns>Return true for success and false for failed</returns>
        public async Task<bool> CreateJob(int protocolRuleId,  string protocolName, List<PatientDTO> records, Client client)
        {
            bool isSuccess = false;
            _logger.LogInformation("Creating Job");
            var jobBlob = new JobBlob()
            {
                Id = Guid.NewGuid(),
                ClientId = client.Id,
                DateTimeCreated = DateTime.UtcNow,
                JobType = "AutoSearchJob",
                Status = JobStatus.Importing,
                ProtocolRuleId = protocolRuleId,
                Description =$"Appointment Reminder for {protocolName} ",
                PatientIds = records.Select(x=>x.PatientId).ToArray(),
                TotalCount = records.Count(),
                EnglishCount = records.Where(x => x.Language == "ENG" || string.IsNullOrEmpty(x.Language)).Count(),
                SpanishCount = records.Where(x => x.Language == "SPA").Count(),
                OtherLangCount = records.Where(x => string.IsNullOrEmpty(x.Language) && x.Language != "SPA" && x.Language != "ENG" ).Count(),
            };
            await _jobRepository.SaveJob(jobBlob.Id, System.Text.Json.JsonSerializer.Serialize<JobBlob>(jobBlob));
            _logger.LogInformation("Triggering Job Scheduler");
            // Call Intake function to calling schedule job functions.
            await _intakeTriggerService.ScheduleJobFunction(jobBlob.Id.ToString(), client.ClientId.ToString());

            return isSuccess;
        }

        /// <summary>
        /// Create a Job
        /// </summary>
        /// <param name="fileName">File Name</param>
        /// <param name="extension">File Extension</param>
        /// <param name="jobBlob">Job blob</param>
        /// <param name="records">Appointment Records</param>
        /// <param name="client">Client Object</param>
        /// <param name="settings">Client Settings</param>
        /// <returns>Return true for success and false for failed</returns>
        public async Task<bool> CreateJob(string fileName, string extension, JobBlob jobBlob, List<FileTemplate> records, Client client, List<ClientSetting> settings)
        {
            bool isSuccess = false;
            var name = fileName + "_" + DateTime.Now.ToFileTime() + extension;
            
            var fileConnectionString = settings.FirstOrDefault(x => x.KeyName == "Azure.FileUpload.ConnectionString")?.KeyValue;
            var fileContainerName = settings.FirstOrDefault(x => x.KeyName == "Azure.FileUpload.ContainerName")?.KeyValue;

            _logger.LogInformation("Creating Job");
            if (records != null)
            {
                BlobClient blobClient = new BlobClient(connectionString: fileConnectionString, blobContainerName: fileContainerName, blobName: name);
                var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    Delimiter = ",",
                    HasHeaderRecord = true,
                    Encoding = Encoding.UTF8
                };

                using (var mem = new MemoryStream())
                using (var writer = new StreamWriter(mem))
                using (var csvWriter = new CsvWriter(writer, csvConfig))
                {
                    csvWriter.Context.RegisterClassMap<FileTemplateMap>();
                    csvWriter.WriteHeader<FileTemplate>();
                    csvWriter.NextRecord();
                    csvWriter.WriteRecords(records);
                    writer.Flush();
                    // upload the file
                    mem.Position = 0;
                    var blobInfo = await blobClient.UploadAsync(mem);
                }
            }
            else
            {
                records = new List<FileTemplate>();
            }

            jobBlob.ClientId = client.Id;
            jobBlob.ProtocolId = int.Parse(settings.FirstOrDefault(x => x.KeyName == "Appointment.ProtocolId")?.KeyValue);
            jobBlob.DateTimeCreated = DateTime.UtcNow;
            jobBlob.JobType = "AppointmentImport";
            jobBlob.FilePath = fileContainerName;
            jobBlob.FileConnection = fileConnectionString;
            jobBlob.DateTimeCreated = DateTime.UtcNow;
            jobBlob.FileId = name;
            jobBlob.TotalCount = records.Count();

            jobBlob.EnglishCount = records.Where(x => string.IsNullOrEmpty(x.Language) || x.Language.ToLower() == "eng").Count();
            jobBlob.SpanishCount = records.Where(x => x.Language.ToLower() == "spa").Count();
            jobBlob.OtherLangCount = records.Where(x => !string.IsNullOrEmpty(x.Language) && x.Language.ToLower() != "spa" && x.Language.ToLower() != "eng").Count();

            await _jobRepository.SaveJob(jobBlob.Id, System.Text.Json.JsonSerializer.Serialize<JobBlob>(jobBlob));
            return isSuccess;
        }

        #endregion
    }
}