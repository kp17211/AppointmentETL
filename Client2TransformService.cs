using AppointmentReminderFunction.Models;
using AppointmentReminderFunction.Repositories;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Transform Service for Client2 to transfer specific file to common known file formate
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>04-May-2023</createdDate>
    public class Client2TransformService : TransformService
    {
        #region ctor
        private readonly ILogger<ExtractService> _logger;
        private readonly IAppointmentRepository _repository;
        public Client2TransformService(ILogger<ExtractService> logger, IAppointmentRepository repository)
        {
            _logger = logger;
            _repository = repository;
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Transform Data with known format and validate file data
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="dateFormat">DateTime Format</param>
        /// <returns>Error Records if Any</returns>
        public override async Task<List<string>> TransformData(List<FileTemplate> records, string dateFormat)
        {
            records.ForEach(r => {
                r.AppNotes = r.AppTypeDesc;
                r.AppStatus = "Scheduled";
                r.DateofBirth = GetConvertedDate(r.DateofBirth, dateFormat);
                r.AppDate = GetConvertedDate(r.AppDate, dateFormat);
                r.AppTime = r.AppTime.Trim();
                r.PatientPrimaryPhone = (string.IsNullOrEmpty(r.PatientPrimaryPhone) || (!string.IsNullOrEmpty(r.PatientPrimaryPhone) && r.PatientPrimaryPhone.Trim().Replace("-","").Length < 10) ? r.PatientSecondaryPhone.Replace("-", "") : r.PatientPrimaryPhone).Replace("-","");
                r.ProviderFirstName = !string.IsNullOrEmpty(r.ProviderName) ? r.ProviderName.Split(' ')[0].Trim() : "";
                r.ProviderLastName = !string.IsNullOrEmpty(r.ProviderName) && r.ProviderName.Split(' ').Length > 1 ? r.ProviderName.Substring(r.ProviderName.IndexOf(" ") + 1).Trim() : "";
                r.Language = !string.IsNullOrEmpty(r.Language) ? (r.Language.Length > 3 ? r.Language.Substring(0, 3) : r.Language) : "";
            });
            
            var appIds = records.Select(x => $"'{x.AppId}'").Distinct();
            await _repository.UpdateStatusForNSGastro(appIds);

            return await ValidateFileData(records);
        }
        #endregion
    }
}
