using System;
using System.IO;

namespace SocialExplorer.IO.FastDBF
{
    public class DbfReader : IDisposable
    {
        private readonly DbfFile dbfFile;

        public DbfReader()
        {
            dbfFile = new DbfFile();
        }

        public DbfReader(string filepath)
            : this()
        {
            this.Open(filepath);
        }

        public DbfReader(Stream stream)
            : this()
        {
            this.Open(stream);
        }

        public void Open(string filepath)
        {
            dbfFile.Open(filepath, FileMode.Open);
        }

        public void Open(Stream stream)
        {
            dbfFile.Open(stream);
        }

        public void Close()
        {
            dbfFile.Close();
        }

        public void Dispose()
        {
            this.Close();
        }

        public DbfRecord CreateRecord()
        {
            return new DbfRecord(dbfFile.Header);
        }

        public bool ReadNext(DbfRecord record)
        {
            return dbfFile.ReadNext(record);
        }
    }
}