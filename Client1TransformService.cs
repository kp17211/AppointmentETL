using AppointmentReminderFunction.Models;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Transform Service for Client1 Client to transfer specific file to common known file formate
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>04-May-2023</createdDate>
    public class Client1TransformService : TransformService
    {
        #region ctor
        private readonly ILogger<ExtractService> _logger;
        public Client1TransformService(ILogger<ExtractService> logger)
        {
            _logger = logger;
        }
        #endregion

        #region Public Method
        /// <summary>
        /// Transform Data with known format and validate file data
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="dateFormat">DateTime Format</param>
        /// <returns>Error Records if Any</returns>
        public override async Task<List<string>> TransformData(List<FileTemplate> records, string dateFormat)
        {
            records.ForEach(r => {
                r.DateofBirth = GetConvertedDate(r.DateofBirth, dateFormat);
                r.AppDate = GetConvertedDate(r.AppDate, dateFormat);
                r.Language = !string.IsNullOrEmpty(r.Language) ? (r.Language.Length > 3 ? r.Language.Substring(0, 3) : r.Language) : "";
                r.AppStatus = !string.IsNullOrEmpty(r.Custom1) ? "Canceled" : (!string.IsNullOrEmpty(r.Custom2) ? "Confirmed" : r.AppStatus);
                r.AppTypeDesc = (r.AppType.Split("[")[0]).Trim();
                r.AppType = r.AppType.Split("[").Length > 1 ? (r.AppType.Split("[")[1]).Replace("]", "") : r.AppType;
                r.ProviderId = r.ProviderName.Split("[").Length > 1 ? (r.ProviderName.Split("[")[1]).Replace("]", "").Trim() : r.ProviderName;
                r.ProviderName = (r.ProviderName.Split("[")[0]).Trim();
                r.ProviderFirstName = !string.IsNullOrEmpty(r.ProviderName) ? r.ProviderName.Split(' ')[0].Trim() : "";
                r.ProviderLastName = !string.IsNullOrEmpty(r.ProviderName) && r.ProviderName.Split(' ').Length > 1 ? r.ProviderName.Substring(r.ProviderName.IndexOf(" ") + 1).Trim() : "";
                r.LastName = r.FirstName.Split(",")[0];
                r.FirstName = r.FirstName.Split(",").Length > 1 ? r.FirstName.Split(",")[1] : r.FirstName;
                r.PatientPrimaryPhone = (string.IsNullOrEmpty(r.PatientPrimaryPhone) || (!string.IsNullOrEmpty(r.PatientPrimaryPhone) && r.PatientPrimaryPhone.Trim().Replace("-", "").Length < 10) ? r.PatientSecondaryPhone.Replace("-", "") : r.PatientPrimaryPhone).Replace("-", "");
            });

            return await ValidateFileData(records);
        }
        #endregion
    }
}
