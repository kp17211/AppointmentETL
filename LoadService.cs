using AppointmentReminderFunction.Interfaces;
using AppointmentReminderFunction.Models;
using AppointmentReminderFunction.Repositories;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Load Service - Load the Data into Database
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>03-May-2023</createdDate>
    public class LoadService : ILoadService
    {
        #region ctor
        private readonly AppointmentRepository _repository;
        public LoadService(AppointmentRepository repository)
        {
            _repository = repository;
        }
        #endregion

        /// <summary>
        /// Load Appointment Status
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="clientId">Client Id</param>
        /// <returns>Task Status</returns>
        public async Task LoadAppStatus(List<FileTemplate> records, int clientId)
        {
            var status = records.Select(x => x.AppStatus).Distinct();
            await _repository.InsertBulkAppStatus(status, clientId);
        }

        /// <summary>
        /// Load Appointment Types
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="clientId">Client Id</param>
        /// <returns>Task Status</returns>
        public async Task LoadAppTypes(List<FileTemplate> records, int clientId)
        {
            var appTypes = records.Select(x => new { x.AppType, x.AppTypeDesc }).Distinct();
            DataTable dataTable = new();
            dataTable.Columns.Add(new DataColumn("AppType", typeof(String)));
            dataTable.Columns.Add(new DataColumn("AppTypeDesc", typeof(String)));
            foreach (var appType in appTypes)
            {
                DataRow row = dataTable.NewRow();
                row["AppType"] = appType.AppType;
                row["AppTypeDesc"] = appType.AppTypeDesc;
                dataTable.Rows.Add(row);
            }
            await _repository.InsertBulkAppType(dataTable, clientId);
        }

        /// <summary>
        /// Load Locations
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="clientId">Client Id</param>
        /// <returns>Task Status</returns>
        public async Task LoadLocations(List<FileTemplate> records, int clientId)
        {
            var locations = records.Select(x => new { x.LocationId, x.LocationName }).Distinct();
            DataTable dataTable = new();
            dataTable.Columns.Add(new DataColumn("LocationId", typeof(String)));
            dataTable.Columns.Add(new DataColumn("LocationName", typeof(String)));
            foreach (var location in locations)
            {
                DataRow row = dataTable.NewRow();
                row["LocationId"] = location.LocationId;
                row["LocationName"] = location.LocationName;
                dataTable.Rows.Add(row);
            }
            await _repository.InsertBulkLocations(dataTable, clientId);
        }

        /// <summary>
        /// Load Practitioners
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="clientId">Client Id</param>
        /// <returns>Task Status</returns>
        public async Task LoadPractitioners(List<FileTemplate> records, int clientId)
        {
            var practitioners = records.Select(x => new { x.ProviderId, x.ProviderName, x.ProviderFirstName, x.ProviderLastName }).Distinct();
            DataTable dataTable = new();
            dataTable.Columns.Add(new DataColumn("ProviderId", typeof(String)));
            dataTable.Columns.Add(new DataColumn("ProviderName", typeof(String)));
            dataTable.Columns.Add(new DataColumn("ProviderFirstName", typeof(String)));
            dataTable.Columns.Add(new DataColumn("ProviderLastName", typeof(String)));
            foreach (var practitioner in practitioners)
            {
                DataRow row = dataTable.NewRow();
                row["ProviderId"] = practitioner.ProviderId;
                row["ProviderName"] = practitioner.ProviderName;
                row["ProviderFirstName"] = practitioner.ProviderFirstName;
                row["ProviderLastName"] = practitioner.ProviderLastName;
                dataTable.Rows.Add(row);
            }
            await _repository.InsertBulkPractitioners(dataTable, clientId);
        }

       
    }
}
