using AppointmentReminderFunction.Interfaces;
using AppointmentReminderFunction.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace AppointmentReminderFunction.Services
{
    /// <summary>
    /// Common Transform Service for common known file formate
    /// </summary>
    /// <createdBy>Kaushik Patel</createdBy>
    /// <createdDate>04-May-2023</createdDate>
    public class TransformService : ITransformService
    {
        #region ctor
        public IEnumerable<string> Languages { get; set; }

        public TransformService()
        {
        }
        #endregion

        #region Public Methods
        /// <summary>
        /// Transform Data with known format and validate file data
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <param name="dateFormat">DateTime Format</param>
        /// <returns>Error Records if Any</returns>
        public virtual async Task<List<string>> TransformData(List<FileTemplate> records, string dateFormat)
        {
            records.ForEach(r => {
                r.DateofBirth = GetConvertedDate(r.DateofBirth, dateFormat);
                r.AppTime = GetConvertedTime(r.AppDate, dateFormat);
                r.AppDate = GetConvertedDate(r.AppDate, dateFormat);
                r.PatientPrimaryPhone = r.PatientPrimaryPhone.Replace("-", "");
                r.Language = !string.IsNullOrEmpty(r.Language) ? (r.Language.Length > 3 ? r.Language.Substring(0, 3) : r.Language) : "";
            });

            var appIds = records.Select(x => $"'{x.AppId}'").Distinct();
            
            return await ValidateFileData(records);
        }
        #endregion

        #region Protected Methods
        /// <summary>
        /// Convert DateTime field to common Time format from given format
        /// </summary>
        /// <param name="date">DateTime object</param>
        /// <param name="dateFormat">Given DateTime Format</param>
        /// <returns>Time String</returns>
        protected string GetConvertedTime(string date, string dateFormat)
        {
            if (!string.IsNullOrEmpty(date))
            {
                if (date.Length > dateFormat.Length && !date.Contains("-") && !date.Contains("/"))
                    date = date.Substring(0, dateFormat.Length);

                if (date.Contains("."))
                    date = date.Substring(0, date.IndexOf("."));

                var newdate = DateTime.ParseExact(date, dateFormat, System.Globalization.CultureInfo.InvariantCulture);
                return newdate.ToString("h:mmtt");
            }
            return date;
        }

        /// <summary>
        /// Convert DateTime field to common Date format from given format
        /// </summary>
        /// <param name="date">DateTime object</param>
        /// <param name="dateFormat">Given DateTime Format</param>
        /// <returns>DateTime String</returns>
        protected string GetConvertedDate(string date, string dateFormat)
        {
            if (!string.IsNullOrEmpty(date))
            {
                if (date.Length > dateFormat.Length && !date.Contains("-") && !date.Contains("/"))
                    date = date.Substring(0, dateFormat.Length);

                if (date.Contains("."))
                    date = date.Substring(0, date.IndexOf("."));

                var newdate = DateTime.ParseExact(date, dateFormat, System.Globalization.CultureInfo.InvariantCulture);
                return newdate.ToString("MMddyyyy");
            }
            return date;
        }

        /// <summary>
        /// Validate the File data
        /// </summary>
        /// <param name="records">Appointment Records</param>
        /// <returns>Invalid record List</returns>
        protected async Task<List<string>> ValidateFileData(List<FileTemplate> records)
        {
            List<string> errors = new List<string>();

            // Make sure all records have valid birthdate with common format
            var birthDateValidator = records.Where(x => !string.IsNullOrEmpty(x.DateofBirth) && Regex.IsMatch(x.DateofBirth, @"^(0[1-9]|1[0-2])(0[1-9]|1\d|2\d|3[0-1])(19|20)\d{2}$") == false);
            var phoneValidator = records.Where(x => Regex.IsMatch(x.PatientPrimaryPhone, @"\d{10}") == false);
            records.RemoveAll(x => Regex.IsMatch(x.PatientPrimaryPhone, @"\d{10}") == false);
            var identifierValidator = records.Where(x => Regex.IsMatch(x.PatientIdentifier.Trim(), @"^$") == true);
            var locationValidator = records.Where(x => Regex.IsMatch(x.LocationId.Trim(), @"^$") == true);

            records.ForEach(s =>
            {
                s.Language = !string.IsNullOrEmpty(s.Language) ? (s.Language.Length > 3 ? s.Language.Substring(0, 3) : s.Language) : "";
            });

            var lanList = records.Where(x => !string.IsNullOrEmpty(x.Language)).Select(x => x.Language).Distinct().ToList();
            var nonLan = lanList.Where(x => Languages.Any(y => y.ToLower() == x.ToLower()) == false);

            if (nonLan != null && nonLan.Count() > 0)
            {
                records.RemoveAll(predicate => Languages.Any(y => y.ToLower() == predicate.Language.ToLower()) == false);
            }

            if (phoneValidator.Count() > 0)
                errors.Add($"There are records with incorrect phone no(Count: {phoneValidator.Count()}). Make sure phone no is with 10 digits without +1");

            if (birthDateValidator.Count() > 0)
                errors.Add($"There are records with incorrect birthdate(Count: {birthDateValidator.Count()}). Make sure Birthdate is with mmddyyyy");

            if (identifierValidator.Count() > 0)
                errors.Add($"There are records without Patient Identifier(Count: {identifierValidator.Count()}), Please make sure all patients have Identifier");

            if (locationValidator.Count() > 0)
                errors.Add($"There are records without Location Information(Count: {locationValidator.Count()}), Please make sure all patients have Location");

            return errors;
        }
        #endregion
    }
}
