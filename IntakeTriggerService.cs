using Microsoft.Extensions.Logging;
using AppointmentReminderFunction.Interfaces;
using AppointmentReminderFunction.Models;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Intake Trigger Service to Trigger Azure Function
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>05-May-2023</createdDate>
    public class IntakeTriggerService : IIntakeTriggerService
    {
        #region ctor
        private readonly IRestClientService _restClientService;
        private readonly ILogger<IntakeTriggerService> _logger;
        private string intakeUrl = string.Empty;
        private bool isTestIntake = false;
        private string messageTypePrefix = string.Empty;
        public IntakeTriggerService(IRestClientService restClientService, ILogger<IntakeTriggerService> logger)
        {
            _restClientService = restClientService;
            _logger = logger;
            intakeUrl = FunctionSetting.IntakeUrl;
            isTestIntake = intakeUrl.Contains("pt-test-intake");
            messageTypePrefix = isTestIntake ? "Test-" : string.Empty;

        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Triggger Schedule Job Function
        /// </summary>
        /// <param name="jobGuid">Job Guid</param>
        /// <param name="clientId">Client Id</param>
        /// <returns>Task Status</returns>
        public async Task ScheduleJobFunction(string jobGuid, string clientId)
        {
            try
            {
                var payload = new
                {
                    Version = "1.0",
                    MessageType = $"{messageTypePrefix}schedule-job",
                    TimeStamp = DateTime.UtcNow,
                    PublishedBy = "ScheduleJob",
                    Body = new
                    {
                        ClientId = clientId,
                        JobId = Guid.Parse(jobGuid)
                    }
                };

                _logger.LogInformation($"IntakeTriggerService.ScheduleJobFunction.{payload}");

                var httpResponse = await _restClientService.PostAsync(new Uri(intakeUrl), $"api/message", BuildHeaders(), payload).ConfigureAwait(false);

                string responseData = await httpResponse?.Content?.ReadAsStringAsync();
                _logger.LogInformation($"IntakeTriggerService.ScheduleJobFunction : {responseData}");

            }
            catch (Exception e)
            {
                _logger.LogInformation($"IntakeTriggerService.ScheduleJobFunction : {e.Message}");
            }
        }
        #endregion

        #region Private methods
        /// <summary>
        /// API Build Header
        /// </summary>
        /// <returns></returns>
        private Dictionary<string, string> BuildHeaders()
        {
            Dictionary<string, string> headers = new Dictionary<string, string>();
            headers.Add("Content-Type", "application/json");
            return headers;
        }

        #endregion
    }
}
