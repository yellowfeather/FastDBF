// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DbfReader.cs" company="Social Explorer">
//   Copyright Social Explorer.
// </copyright>
// <summary>
//   The dbf reader.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace SocialExplorer.IO.FastDBF
{
    using System;
    using System.Collections;
    using System.Data;
    using System.Data.Common;
    using System.IO;

    /// <summary>
    /// Provides a way of reading a forward-only stream of data rows from a DBF file.
    /// </summary>
    public class DbfReader : DbDataReader
    {
        /// <summary>
        /// The dbf file.
        /// </summary>
        private readonly DbfFile dbfFile;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbfReader"/> class.
        /// </summary>
        public DbfReader()
        {
            this.dbfFile = new DbfFile();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbfReader"/> class and opens it.
        /// </summary>
        /// <param name="path">
        /// The path.
        /// </param>
        public DbfReader(string path)
            : this()
        {
            this.Open(path);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbfReader"/> class and opens it.
        /// </summary>
        /// <param name="stream">
        /// The stream.
        /// </param>
        public DbfReader(Stream stream)
            : this()
        {
            this.Open(stream);
        }

        /// <summary>
        /// Gets or sets a value indicating whether to skip deleted records.
        /// </summary>
        public bool SkipDeletedRecords { get; set; }

        /// <summary>
        /// Gets the dbf record.
        /// </summary>
        public DbfRecord DbfRecord { get; private set; }

        /// <summary>
        /// Gets the record index.
        /// </summary>
        public int RecordIndex
        {
            get
            {
                return this.DbfRecord.RecordIndex;
            }
        }

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        /// <returns>
        /// The depth of nesting for the current row.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override int Depth
        {
            get
            {
                this.GuardOpen("NextResult");
                return 0;
            }
        }

        /// <summary>
        /// Gets a value indicating whether the <see cref="T:System.Data.Common.DbDataReader"/> is closed.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Data.Common.DbDataReader"/> is closed; otherwise false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool IsClosed
        {
            get
            {
                return !this.dbfFile.IsOpen;
            }
        }

        /// <summary>
        /// Gets the number of rows changed, inserted, or deleted by execution of the SQL statement. 
        /// </summary>
        /// <returns>
        /// The number of rows changed, inserted, or deleted. -1 for SELECT statements; 0 if no rows were affected or the statement failed.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override int RecordsAffected
        {
            get
            {
                this.GuardOpen("NextResult");
                return (int)this.dbfFile.Header.RecordCount;
            }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        /// <returns>
        /// The number of columns in the current row.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override int FieldCount
        {
            get
            {
                this.GuardOpen("FieldCount");
                return this.dbfFile.Header.ColumnCount;
            }
        }

        /// <summary>
        /// Gets a value that indicates whether this <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows.
        /// </summary>
        /// <returns>
        /// true if the <see cref="T:System.Data.Common.DbDataReader"/> contains one or more rows; otherwise false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool HasRows
        {
            get
            {
                this.GuardOpen("HasRows");
                return this.dbfFile.Header.RecordCount >= 1;
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override object this[int ordinal]
        {
            get
            {
                this.GuardOpen("array accessor");
                return this.GetValue(ordinal);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="name">The name of the column.</param>
        /// <filterpriority>1</filterpriority>
        public override object this[string name]
        {
            get
            {
                this.GuardOpen("array accessor");
                var ordinal = this.GetOrdinal(name);
                return this.GetValue(ordinal);
            }
        }

        /// <summary>
        /// Opens the <see cref="T:SocialExplorer.IO.FastDBF.DbfReader" /> with the specified file path.
        /// </summary>
        /// <param name="path">
        /// The full path to the file to open.
        /// </param>
        public void Open(string path)
        {
            this.dbfFile.Open(path, FileMode.Open);
            this.DbfRecord = new DbfRecord(this.dbfFile.Header);
        }

        /// <summary>
        /// Opens the <see cref="T:SocialExplorer.IO.FastDBF.DbfReader" /> with the specified stream.
        /// </summary>
        /// <param name="stream">
        /// The stream to open.
        /// </param>
        public void Open(Stream stream)
        {
            this.dbfFile.Open(stream);
            this.DbfRecord = new DbfRecord(this.dbfFile.Header);
        }

        /// <summary>
        /// Closes the <see cref="T:System.Data.Common.DbDataReader"/> object.
        /// </summary>
        /// <filterpriority>1</filterpriority>
        public override void Close()
        {
            this.dbfFile.Close();
        }

        /// <summary>
        /// Returns a <see cref="T:System.Data.DataTable"/> that describes the column metadata of the <see cref="T:System.Data.Common.DbDataReader"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.Data.DataTable"/> that describes the column metadata.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override DataTable GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.
        /// </summary>
        /// <returns>
        /// true if there are more result sets; otherwise false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool NextResult()
        {
            this.GuardOpen("NextResult");

            return this.ReadInternal();
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        /// <returns>
        /// true if there are more rows; otherwise false.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override bool Read()
        {
            this.GuardOpen("Read");

            return this.ReadInternal();
        }

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override bool GetBoolean(int ordinal)
        {
            this.GuardGetValue("GetBoolean", ordinal, DbfColumn.DbfColumnType.Boolean);

            var segment = this.DbfRecord.ColumnData(ordinal);
            this.GuardLength("GetBoolean", segment, 1);
            return BitConverter.ToBoolean(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override byte GetByte(int ordinal)
        {
            this.GuardGetValue("GetByte", ordinal, DbfColumn.DbfColumnType.Binary);

            var segment = this.DbfRecord.ColumnData(ordinal);
            this.GuardLength("GetByte", segment, 1);
            return segment.Array[segment.Offset];
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the buffer, starting at the location indicated by <paramref name="bufferOffset"/>.
        /// </summary>
        /// <returns>
        /// The actual number of bytes read.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <filterpriority>1</filterpriority>
        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            this.GuardGetValue("GetBytes", ordinal, DbfColumn.DbfColumnType.Binary);
            var segment = this.DbfRecord.ColumnData(ordinal);

            Array.Copy(segment.Array, dataOffset, buffer, bufferOffset, length);
            return segment.Count;
        }

        /// <summary>
        /// Gets the value of the specified column as a single character.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override char GetChar(int ordinal)
        {
            this.GuardGetValue("GetChar", ordinal, DbfColumn.DbfColumnType.Character);

            var segment = this.DbfRecord.ColumnData(ordinal);
            return BitConverter.ToChar(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Reads a stream of characters from the specified column, starting at location indicated by 
        /// <paramref name="dataOffset"/>, into the buffer, starting at the location indicated by 
        /// <paramref name="bufferOffset"/>.
        /// </summary>
        /// <returns>
        /// The actual number of characters read.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The index within the row from which to begin the read operation.</param>
        /// <param name="buffer">The buffer into which to copy the data.</param>
        /// <param name="bufferOffset">The index with the buffer to which the data will be copied.</param>
        /// <param name="length">The maximum number of characters to read.</param>
        /// <filterpriority>1</filterpriority>
        public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
        {
            this.GuardGetValue("GetChars", ordinal, DbfColumn.DbfColumnType.Character);
            var segment = this.DbfRecord.ColumnData(ordinal);

            Array.Copy(segment.Array, dataOffset, buffer, bufferOffset, length);
            return segment.Count;
        }

        /// <summary>
        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override Guid GetGuid(int ordinal)
        {
            this.GuardGetValue("GetGuid", ordinal, DbfColumn.DbfColumnType.Binary);
            var value = this.DbfRecord[ordinal];
            return new Guid(value);
        }

        /// <summary>
        /// Gets the value of the specified column as a 16-bit signed integer.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>2</filterpriority>
        public override short GetInt16(int ordinal)
        {
            this.GuardGetValue("GetInt16", ordinal, DbfColumn.DbfColumnType.Integer);
            var segment = this.DbfRecord.ColumnData(ordinal);
            return BitConverter.ToInt16(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Gets the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override int GetInt32(int ordinal)
        {
            this.GuardGetValue("GetInt32", ordinal, DbfColumn.DbfColumnType.Integer);
            var segment = this.DbfRecord.ColumnData(ordinal);
            return BitConverter.ToInt32(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Gets the value of the specified column as a 64-bit signed integer.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>2</filterpriority>
        public override long GetInt64(int ordinal)
        {
            this.GuardGetValue("GetInt64", ordinal, DbfColumn.DbfColumnType.Integer);
            var segment = this.DbfRecord.ColumnData(ordinal);
            return BitConverter.ToInt64(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="T:System.DateTime"/> object.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override DateTime GetDateTime(int ordinal)
        {
            this.GuardGetValue("GetDateTime", ordinal, DbfColumn.DbfColumnType.Date);

            var segment = this.DbfRecord.ColumnData(ordinal);
            var year = BitConverter.ToInt32(segment.Array, segment.Offset);
            var month = BitConverter.ToInt32(segment.Array, segment.Offset + 4);
            var day = BitConverter.ToInt32(segment.Array, segment.Offset + 6);

            return new DateTime(year, month, day);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="T:System.String"/>.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override string GetString(int ordinal)
        {
            this.GuardOpen("GetString");
            this.GuardOrdinal("GetString", ordinal);

            return this.DbfRecord[ordinal];
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="T:System.Object"/>.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override object GetValue(int ordinal)
        {
            this.GuardOpen("GetValue");
            this.GuardOrdinal("GetValue", ordinal);

            return this.DbfRecord[ordinal];
        }

        /// <summary>
        /// Gets all attribute columns in the collection for the current row.
        /// </summary>
        /// <returns>
        /// The number of instances of <see cref="T:System.Object"/> in the array.
        /// </returns>
        /// <param name="values">An array of <see cref="T:System.Object"/> into which to copy the attribute columns.</param>
        /// <filterpriority>1</filterpriority>
        public override int GetValues(object[] values)
        {
            this.GuardOpen("GetValues");
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains nonexistent or missing values.
        /// </summary>
        /// <returns>
        /// true if the specified column is equivalent to <see cref="T:System.DBNull"/>; otherwise false.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override bool IsDBNull(int ordinal)
        {
            this.GuardOpen("IsDBNull");
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="T:System.Decimal"/> object.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override decimal GetDecimal(int ordinal)
        {
            this.GuardGetValue("GetDecimal", ordinal, DbfColumn.DbfColumnType.Number);
            var segment = this.DbfRecord.ColumnData(ordinal);
            var value = BitConverter.ToDouble(segment.Array, segment.Offset);
            return Convert.ToDecimal(value);
        }

        /// <summary>
        /// Gets the value of the specified column as a double-precision floating point number.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override double GetDouble(int ordinal)
        {
            this.GuardGetValue("GetDouble", ordinal, DbfColumn.DbfColumnType.Number);
            var segment = this.DbfRecord.ColumnData(ordinal);
            return BitConverter.ToDouble(segment.Array, segment.Offset);
        }

        /// <summary>
        /// Gets the value of the specified column as a single-precision floating point number.
        /// </summary>
        /// <returns>
        /// The value of the specified column.
        /// </returns>`
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>2</filterpriority>
        public override float GetFloat(int ordinal)
        {
            this.GuardGetValue("GetFloat", ordinal, DbfColumn.DbfColumnType.Number);
            var data = this.DbfRecord[ordinal];
            return float.Parse(data);
        }

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <returns>
        /// The name of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override string GetName(int ordinal)
        {
            this.GuardOpen("GetName");
            this.GuardOrdinal("GetName", ordinal);

            var dbfColumn = this.dbfFile.Header[ordinal];
            return dbfColumn.Name;
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <returns>
        /// The zero-based column ordinal.
        /// </returns>
        /// <param name="name">The name of the column.</param>
        /// <exception cref="T:System.IndexOutOfRangeException">The name specified is not a valid column name.</exception>
        /// <filterpriority>1</filterpriority>
        public override int GetOrdinal(string name)
        {
            this.GuardOpen("GetOrdinal");

            var ordinal = this.dbfFile.Header.FindColumn(name);
            this.GuardOrdinal("GetOrdinal", ordinal);

            return ordinal;
        }

        /// <summary>
        /// Gets name of the data type of the specified column.
        /// </summary>
        /// <returns>
        /// A string representing the name of the data type.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override string GetDataTypeName(int ordinal)
        {
            this.GuardOpen("GetDataTypeName");
            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <returns>
        /// The data type of the specified column.
        /// </returns>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <filterpriority>1</filterpriority>
        public override Type GetFieldType(int ordinal)
        {
            this.GuardOpen("GetFieldType");
            throw new NotImplementedException();
        }

        /// <summary>
        /// Returns an <see cref="T:System.Collections.IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </summary>
        /// <returns>
        /// An <see cref="T:System.Collections.IEnumerator"/> that can be used to iterate through the rows in the data reader.
        /// </returns>
        /// <filterpriority>1</filterpriority>
        public override IEnumerator GetEnumerator()
        {
            this.GuardOpen("GetEnumerator");
            return new DbEnumerator(this);
        }

        /// <summary>
        /// Reads the next record, skipping deleted records if necessary.
        /// </summary>
        /// <returns>true if successfully read</returns>
        private bool ReadInternal()
        {
            var success = this.dbfFile.ReadNext(this.DbfRecord);
            while (success && this.SkipDeletedRecords && this.DbfRecord.IsDeleted)
            {
                success = this.dbfFile.ReadNext(this.DbfRecord);
            }

            return success;
        }

        /// <summary>
        /// Guards against accessing a closed DBF file.
        /// </summary>
        /// <param name="methodName">The name of the calling method.</param>
        private void GuardOpen(string methodName)
        {
            if (this.IsClosed)
            {
                throw new InvalidOperationException(methodName);
            }
        }

        /// <summary>
        /// Guards against using an invalid column ordinal.
        /// </summary>
        /// <param name="methodName">The name of the calling method.</param>
        /// <param name="ordinal">The zero based column ordinal.</param>
// ReSharper disable UnusedParameter.Local
        private void GuardOrdinal(string methodName, int ordinal)
// ReSharper restore UnusedParameter.Local
        {
            if (ordinal >= this.dbfFile.Header.ColumnCount)
            {
                throw new IndexOutOfRangeException(methodName);
            }
        }

        /// <summary>
        /// Guards against accessing a column as an invalid type.
        /// </summary>
        /// <param name="methodName">The name of the calling method.</param>
        /// <param name="ordinal">The zero based column ordinal.</param>
        /// <param name="dbfColumnType">The type of the column.</param>
// ReSharper disable UnusedParameter.Local
        private void GuardColumnType(string methodName, int ordinal, DbfColumn.DbfColumnType dbfColumnType)
// ReSharper restore UnusedParameter.Local
        {
            if (this.dbfFile.Header[ordinal].ColumnType != dbfColumnType)
            {
                throw new InvalidOperationException(methodName);
            }
        }

        /// <summary>
        /// Guards against incorrect segment length.
        /// </summary>
        /// <param name="methodName">The name of the calling method.</param>
        /// <param name="segment">The array segment.</param>
        /// <param name="length">The expected length.</param>
// ReSharper disable UnusedParameter.Local
        private void GuardLength(string methodName, ArraySegment<byte> segment, int length)
// ReSharper restore UnusedParameter.Local
        {
            if (segment.Count != length)
            {
                throw new InvalidOperationException(methodName);
            }
        }

        /// <summary>
        /// Guards against incorrect column value access.
        /// </summary>
        /// <param name="methodName">The name of the calling method.</param>
        /// <param name="ordinal">The zero based column ordinal.</param>
        /// <param name="dbfColumnType">The type of the column.</param>
        private void GuardGetValue(string methodName, int ordinal, DbfColumn.DbfColumnType dbfColumnType)
        {
            this.GuardOpen(methodName);
            this.GuardOrdinal(methodName, ordinal);
            this.GuardColumnType(methodName, ordinal, dbfColumnType);
        }
    }
}