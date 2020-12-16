#region Related components
using System;
using System.IO;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using ExcelDataReader;
using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Spreadsheet;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using net.vieapps.Components.Repository;
using net.vieapps.Components.Utility;
#endregion

#if !SIGN
[assembly: System.Runtime.CompilerServices.InternalsVisibleTo("VIEApps.Components.XUnitTests")]
#endif

namespace net.vieapps.Components.Utility
{
	/// <summary>
	/// Helper for working with Excel
	/// </summary>
	public static partial class ExcelService
	{
		static ExcelService()
			=> Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

		/// <summary>
		/// Creates a stream that contains Excel document from this data-set
		/// </summary>
		/// <param name="dataSet">DataSet containing the data to be written to the Excel in OpenXML format</param>
		/// <returns>A stream that contains the Excel document</returns>
		/// <remarks>The stream that contains an Excel document in OpenXML format with MIME type is 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'</remarks>
		public static MemoryStream SaveAsExcel(this DataSet dataSet)
		{
			// check dataset
			if (dataSet == null || dataSet.Tables == null || dataSet.Tables.Count < 1)
				throw new InformationNotFoundException("DataSet must be not null and contains at least one table");

			// write dataset into stream
			var stream = UtilityService.CreateMemoryStream();
			using (var document = SpreadsheetDocument.Create(stream, SpreadsheetDocumentType.Workbook, true))
			{
				dataSet.WriteExcelDocument(document);
			}
			return stream;
		}

		/// <summary>
		/// Creates a stream that contains Excel document from this data-set
		/// </summary>
		/// <param name="dataSet">DataSet containing the data to be written to the Excel in OpenXML format</param>
		/// <param name="cancellationToken">The cancellation token.</param>
		/// <returns>A stream that contains the Excel document</returns>
		/// <remarks>The stream that contains an Excel document in OpenXML format with MIME type is 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'</remarks>
		public static Task<MemoryStream> SaveAsExcelAsync(this DataSet dataSet, CancellationToken cancellationToken = default)
			=> UtilityService.ExecuteTask(() => dataSet.SaveAsExcel(), cancellationToken);

		#region Write a data-set to Excel document
		static void WriteExcelDocument(this DataSet dataset, SpreadsheetDocument spreadsheet)
		{
			//  Create the Excel document contents.
			// This function is used when creating an Excel file either writing to a file, or writing to a MemoryStream.
			spreadsheet.AddWorkbookPart();
			spreadsheet.WorkbookPart.Workbook = new Workbook();

			//  My thanks to James Miera for the following line of code (which prevents crashes in Excel 2010)
			spreadsheet.WorkbookPart.Workbook.Append(new BookViews(new WorkbookView()));

			//  If we don't add a "WorkbookStylesPart", OLEDB will refuse to connect to this .xlsx file !
			var workbookStylesPart = spreadsheet.WorkbookPart.AddNewPart<WorkbookStylesPart>("rIdStyles");
			var stylesheet = new Stylesheet();
			workbookStylesPart.Stylesheet = stylesheet;

			//  Loop through each of the DataTables in our DataSet, and create a new Excel Worksheet for each.
			uint worksheetNumber = 1;
			foreach (DataTable dataTable in dataset.Tables)
			{
				//  For each worksheet you want to create
				var workSheetID = "rId" + worksheetNumber.ToString();
				var worksheetName = dataTable.TableName;

				var newWorksheetPart = spreadsheet.WorkbookPart.AddNewPart<WorksheetPart>();
				newWorksheetPart.Worksheet = new Worksheet();

				// create sheet data
				newWorksheetPart.Worksheet.AppendChild(new SheetData());

				// save worksheet
				ExcelService.WriteDataTableToExcelWorksheet(dataTable, newWorksheetPart);
				newWorksheetPart.Worksheet.Save();

				// create the worksheet to workbook relation
				if (worksheetNumber == 1)
					spreadsheet.WorkbookPart.Workbook.AppendChild(new Sheets());

				spreadsheet.WorkbookPart.Workbook.GetFirstChild<Sheets>().AppendChild(new Sheet
				{
					Id = spreadsheet.WorkbookPart.GetIdOfPart(newWorksheetPart),
					SheetId = worksheetNumber,
					Name = dataTable.TableName
				});

				worksheetNumber++;
			}

			spreadsheet.WorkbookPart.Workbook.Save();
		}

		static void WriteDataTableToExcelWorksheet(DataTable dataTable, WorksheetPart worksheetPart)
		{
			var worksheet = worksheetPart.Worksheet;
			var sheetData = worksheet.GetFirstChild<SheetData>();

			//  Create a Header Row in our Excel file, containing one header for each Column of data in our DataTable.
			//
			//  We'll also create an array, showing which type each column of data is (Text or Numeric), so when we come to write the actual
			//  cells of data, we'll know if to write Text values or Numeric cell values.
			var numberOfColumns = dataTable.Columns.Count;
			var isNumericColumn = new bool[numberOfColumns];

			var excelColumnNames = new string[numberOfColumns];
			for (var column = 0; column < numberOfColumns; column++)
				excelColumnNames[column] = ExcelService.GetExcelColumnName(column);

			//
			//  Create the Header row in our Excel Worksheet
			//
			uint rowIndex = 1;

			// add a row at the top of spreadsheet
			var headerRow = new Row
			{
				RowIndex = rowIndex
			};
			sheetData.Append(headerRow);

			for (int index = 0; index < numberOfColumns; index++)
			{
				var col = dataTable.Columns[index];
				ExcelService.AppendTextCell(excelColumnNames[index] + "1", col.ColumnName, headerRow);
				isNumericColumn[index] = (col.DataType.FullName == "System.Decimal") || (col.DataType.FullName == "System.Int32");
			}

			//
			//  Now, step through each row of data in our DataTable...
			//
			double cellNumericValue;
			foreach (DataRow dataRow in dataTable.Rows)
			{
				// ...create a new row, and append a set of this row's data to it.
				++rowIndex;

				// add a row at the top of spreadsheet
				var newExcelRow = new Row
				{
					RowIndex = rowIndex
				};
				sheetData.Append(newExcelRow);

				for (var index = 0; index < numberOfColumns; index++)
				{
					var cellValue = dataRow.ItemArray[index].ToString();

					// Create cell with data
					if (isNumericColumn[index])
					{
						//  For numeric cells, make sure our input data IS a number, then write it out to the Excel file.
						//  If this numeric value is NULL, then don't write anything to the Excel file.
						cellNumericValue = 0;
						if (double.TryParse(cellValue, out cellNumericValue))
						{
							cellValue = cellNumericValue.ToString();
							ExcelService.AppendNumericCell(excelColumnNames[index] + rowIndex.ToString(), cellValue, newExcelRow);
						}
					}
					//  For text cells, just write the input data straight out to the Excel file.
					else
						ExcelService.AppendTextCell(excelColumnNames[index] + rowIndex.ToString(), cellValue, newExcelRow);
				}
			}
		}

		static void AppendTextCell(string cellReference, string cellStringValue, Row excelRow)
		{
			var cell = new Cell
			{
				CellReference = cellReference,
				DataType = CellValues.String
			};

			cell.Append(new CellValue
			{
				Text = cellStringValue
			});

			excelRow.Append(cell);
		}

		static void AppendNumericCell(string cellReference, string cellStringValue, Row excelRow)
		{
			var cell = new Cell
			{
				CellReference = cellReference
			};

			cell.Append(new CellValue
			{
				Text = cellStringValue
			});

			excelRow.Append(cell);
		}

		static string GetExcelColumnName(int columnIndex)
		{
			//  Convert a zero-based column index into an Excel column reference  (A, B, C.. Y, Y, AA, AB, AC... AY, AZ, B1, B2..)
			//
			//		GetExcelColumnName(0) should return "A"
			//    GetExcelColumnName(1) should return "B"
			//    GetExcelColumnName(25) should return "Z"
			//    GetExcelColumnName(26) should return "AA"
			//    GetExcelColumnName(27) should return "AB"
			//    ..etc..
			//
			if (columnIndex < 26)
				return ((char)('A' + columnIndex)).ToString();

			var firstChar = (char)('A' + (columnIndex / 26) - 1);
			var secondChar = (char)('A' + (columnIndex % 26));

			return $"{firstChar}{secondChar}";
		}
		#endregion

		/// <summary>
		/// Reads an Excel file as data-set
		/// </summary>
		/// <param name="fileInfo"></param>
		/// <param name="readerConfig"></param>
		/// <param name="datasetConfig"></param>
		/// <returns></returns>
		public static DataSet ReadExcelAsDataSet(FileInfo fileInfo, ExcelReaderConfiguration readerConfig = null, ExcelDataSetConfiguration datasetConfig = null)
		{
			if (fileInfo == null || !fileInfo.Exists)
				throw new FileNotFoundException();

			using (var stream = File.Open(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
			{
				using (var reader = ExcelReaderFactory.CreateReader(stream, readerConfig))
				{
					return reader.AsDataSet(datasetConfig ?? new ExcelDataSetConfiguration
					{
						// gets or sets a value indicating whether to set the DataColumn.DataType property in a second pass
						UseColumnDataType = true,

						// gets or sets a callback to determine whether to include the current sheet in the DataSet, called once per sheet before ConfigureDataTable
						FilterSheet = (tableReader, sheetIndex) => true,

						// gets or sets a callback to obtain configuration options for a DataTable 
						ConfigureDataTable = tableReader => new ExcelDataTableConfiguration
						{
							// gets or sets a value indicating the prefix of generated column names
							EmptyColumnNamePrefix = "Column",

							// gets or sets a value indicating whether to use a row from the data as column names
							UseHeaderRow = true,

							// gets or sets a callback to determine whether to include the  current row in the DataTable
							FilterRow = rowReader => true,

							// gets or sets a callback to determine whether to include the specific column in the DataTable, called once per column after reading the headers
							FilterColumn = (rowReader, columnIndex) => true
						}
					});
				}
			}
		}

		/// <summary>
		/// Reads an Excel file as data-set
		/// </summary>
		/// <param name="fileInfo"></param>
		/// <param name="readerConfig"></param>
		/// <param name="datasetConfig"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static Task<DataSet> ReadExcelAsDataSetAsync(FileInfo fileInfo, ExcelReaderConfiguration readerConfig = null, ExcelDataSetConfiguration datasetConfig = null, CancellationToken cancellationToken = default)
			=> UtilityService.ExecuteTask(() => ExcelService.ReadExcelAsDataSet(fileInfo, readerConfig, datasetConfig), cancellationToken);

		/// <summary>
		/// Reads an Excel file as data-set
		/// </summary>
		/// <param name="filePath"></param>
		/// <param name="readerConfig"></param>
		/// <param name="datasetConfig"></param>
		/// <returns></returns>
		public static DataSet ReadExcelAsDataSet(string filePath, ExcelReaderConfiguration readerConfig = null, ExcelDataSetConfiguration datasetConfig = null)
			=> ExcelService.ReadExcelAsDataSet(new FileInfo(filePath), readerConfig, datasetConfig);

		/// <summary>
		/// Reads an Excel file as data-set
		/// </summary>
		/// <param name="filePath"></param>
		/// <param name="readerConfig"></param>
		/// <param name="datasetConfig"></param>
		/// <param name="cancellationToken"></param>
		/// <returns></returns>
		public static Task<DataSet> ReadExcelAsDataSetAsync(string filePath, ExcelReaderConfiguration readerConfig = null, ExcelDataSetConfiguration datasetConfig = null, CancellationToken cancellationToken = default)
			=> UtilityService.ExecuteTask(() => ExcelService.ReadExcelAsDataSet(filePath, readerConfig, datasetConfig), cancellationToken);

		#region Conversions of data-set/data-table/objects
		static Regex InvalidXmlCharacters { get; } = new Regex("[\x00-\x08\x0B\x0C\x0E-\x1F]", RegexOptions.Compiled);

		/// <summary>
		/// Creates a data-table from the collection of objects
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="dataSet"></param>
		/// <param name="objects"></param>
		/// <param name="repositoryEntityID"></param>
		/// <param name="onCompleted"></param>
		/// <returns></returns>
		public static DataTable CreateDataTable<T>(this DataSet dataSet, IEnumerable<T> objects, string repositoryEntityID = null, Action<DataTable> onCompleted = null) where T : class
		{
			var dataTable = new DataTable(typeof(T).GetTypeName(true));
			var definition = RepositoryMediator.GetEntityDefinition<T>(false);
			if (definition != null)
			{
				var standardAttributes = definition.Attributes.Where(attribute => !attribute.IsIgnored() && !attribute.IsIgnoredIfNull()).ToList();
				standardAttributes.ForEach(attribute =>
				{
					var type = attribute.IsStoredAsJson() || attribute.IsEnumString() || attribute.IsMappings() || attribute.IsMultipleParentMappings() || attribute.IsChildrenMappings()
						? typeof(string)
						: attribute.Type;
					dataTable.Columns.Add(attribute.Name, Nullable.GetUnderlyingType(type) ?? type);
				});
				var extendedAttributes = !string.IsNullOrWhiteSpace(repositoryEntityID) && definition.BusinessRepositoryEntities.ContainsKey(repositoryEntityID)
					? definition.BusinessRepositoryEntities[repositoryEntityID].ExtendedPropertyDefinitions
					: new List<ExtendedPropertyDefinition>();
				extendedAttributes?.ForEach(attribute =>
				{
					var type = typeof(string);
					switch (attribute.Mode)
					{
						case ExtendedPropertyMode.YesNo:
							type = typeof(bool);
							break;
						case ExtendedPropertyMode.IntegralNumber:
							type = typeof(long);
							break;
						case ExtendedPropertyMode.FloatingPointNumber:
							type = typeof(decimal);
							break;
						case ExtendedPropertyMode.DateTime:
							type = typeof(DateTime);
							break;
					}
					dataTable.Columns.Add(attribute.Name, Nullable.GetUnderlyingType(type) ?? type);
				});
			}
			else
				typeof(T).GetPublicAttributes().ForEach(attribute => dataTable.Columns.Add(attribute.Name, Nullable.GetUnderlyingType(attribute.Type) ?? attribute.Type));
			dataTable.UpdateDataTable(objects, repositoryEntityID, onCompleted);
			dataSet.Tables.Add(dataTable);
			return dataTable;
		}

		/// <summary>
		/// Updates a data-table with values from the collection of objects
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="dataTable"></param>
		/// <param name="objects"></param>
		/// <param name="repositoryEntityID"></param>
		/// <param name="onCompleted"></param>
		/// <returns></returns>
		public static DataTable UpdateDataTable<T>(this DataTable dataTable, IEnumerable<T> objects, string repositoryEntityID = null, Action<DataTable> onCompleted = null) where T : class
		{
			var definition = RepositoryMediator.GetEntityDefinition<T>(false);
			if (definition != null)
			{
				var standardAttributes = definition.Attributes.Where(attribute => !attribute.IsIgnored() && !attribute.IsIgnoredIfNull()).ToList();
				var extendedAttributes = !string.IsNullOrWhiteSpace(repositoryEntityID) && definition.BusinessRepositoryEntities.ContainsKey(repositoryEntityID)
					? definition.BusinessRepositoryEntities[repositoryEntityID].ExtendedPropertyDefinitions
					: new List<ExtendedPropertyDefinition>();
				objects?.ForEach(@object =>
				{
					var dataRow = dataTable.NewRow();
					standardAttributes.ForEach(attribute =>
					{
						var value = @object.GetAttributeValue(attribute);
						if (attribute.IsEnumString())
							value = value?.ToString();
						else if (attribute.IsStoredAsJson())
							value = value != null
								? value is JToken json ? json.ToString(Formatting.None) : value.ToJson().ToString(Formatting.None)
								: value;
						else if (attribute.IsMappings() || attribute.IsMultipleParentMappings() || attribute.IsChildrenMappings())
							value = value != null && value.IsGenericListOrHashSet()
								? (value as IEnumerable<object>).Select(obj => obj?.ToString()).Where(str => !string.IsNullOrWhiteSpace(str)).Join(",")
								: value?.ToJson().ToString(Formatting.None);
						dataRow[attribute.Name] = (value is string strValue ? strValue != null ? ExcelService.InvalidXmlCharacters.Replace(strValue, string.Empty) : null : value) ?? DBNull.Value;
					});
					if (extendedAttributes != null && extendedAttributes.Count > 0)
					{
						var extendedProperties = (@object as IBusinessEntity)?.ExtendedProperties ?? new Dictionary<string, object>();
						extendedAttributes.ForEach(attribute => dataRow[attribute.Name] = extendedProperties.TryGetValue(attribute.Name, out var value) ? (value is string strValue ? strValue != null ? ExcelService.InvalidXmlCharacters.Replace(strValue, string.Empty) : null : value) ?? DBNull.Value : DBNull.Value);
					}
					dataTable.Rows.Add(dataRow);
				});
			}
			else
			{
				var attributes = typeof(T).GetPublicAttributes();
				objects?.ForEach(@object =>
				{
					var dataRow = dataTable.NewRow();
					attributes.ForEach(attribute => dataRow[attribute.Name] = @object.GetAttributeValue(attribute) ?? DBNull.Value);
					dataTable.Rows.Add(dataRow);
				});
			}
			onCompleted?.Invoke(dataTable);
			return dataTable;
		}

		/// <summary>
		/// Converts this collection of objects to data-set
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="objects"></param>
		/// <param name="repositoryEntityID"></param>
		/// <param name="onCompleted"></param>
		/// <returns></returns>
		public static DataSet ToDataSet<T>(this IEnumerable<T> objects, string repositoryEntityID = null, Action<DataSet> onCompleted = null) where T : class
		{
			var dataSet = new DataSet(typeof(T).GetTypeName(true));
			dataSet.CreateDataTable(objects, repositoryEntityID);
			onCompleted?.Invoke(dataSet);
			return dataSet;
		}

		/// <summary>
		/// Converts the data-table to collection of objects
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="dataTable"></param>
		/// <param name="repositoryEntityID"></param>
		/// <param name="onCompleted"></param>
		/// <returns></returns>
		public static IEnumerable<T> ToObjects<T>(this DataTable dataTable, string repositoryEntityID = null, Action<IEnumerable<T>> onCompleted = null) where T : class
		{
			var objects = new List<T>();
			var definition = RepositoryMediator.GetEntityDefinition<T>(false);
			if (definition != null)
			{
				var standardAttributes = definition.Attributes.Where(attribute => !attribute.IsIgnored() && !attribute.IsIgnoredIfNull()).ToList().ToDictionary(attribute => attribute.Name);
				var extendedAttributes = (!string.IsNullOrWhiteSpace(repositoryEntityID) && definition.BusinessRepositoryEntities.ContainsKey(repositoryEntityID)
					? definition.BusinessRepositoryEntities[repositoryEntityID].ExtendedPropertyDefinitions
					: new List<ExtendedPropertyDefinition>()).ToDictionary(attribute => attribute.Name);
				foreach (DataRow dataRow in dataTable.Rows)
					objects.Add(ObjectService.CreateInstance<T>().Copy(dataRow, standardAttributes, extendedAttributes));
			}
			else
			{
				var attributes = typeof(T).GetPublicAttributes().Select(attribute => attribute.Name).ToHashSet();
				foreach (DataRow dataRow in dataTable.Rows)
				{
					var @object = ObjectService.CreateInstance<T>();
					for (var index = 0; index < dataTable.Columns.Count; index++)
					{
						var name = dataTable.Columns[index].ColumnName;
						var value = dataRow[name];
						if (value != null && attributes.Contains(name))
							@object.SetAttributeValue(name, value);
					}
					objects.Add(@object);
				}
			}
			onCompleted?.Invoke(objects);
			return objects;
		}

		/// <summary>
		/// Converts the first data-table of the data-set to collection of objects
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="dataSet"></param>
		/// <param name="repositoryEntityID"></param>
		/// <param name="onCompleted"></param>
		/// <returns></returns>
		public static IEnumerable<T> ToObjects<T>(this DataSet dataSet, string repositoryEntityID = null, Action<IEnumerable<T>> onCompleted = null) where T : class
			=> dataSet != null && dataSet.Tables != null && dataSet.Tables.Count > 0
				? dataSet.Tables[0].ToObjects(repositoryEntityID, onCompleted)
				: null;
		#endregion

	}
}