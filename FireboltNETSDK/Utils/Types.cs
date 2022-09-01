/*
 * Copyright (c) 2022 FireBolt All rights reserved.
 */

using System.Collections;
using System.Data;
using System.Globalization;
using System.Text;
using FireboltDotNetSdk.Exception;
using FireboltDotNetSdk.Utils;

namespace FireboltDoNetSdk.Utils
{
    public enum FireBoltDataType
    {
        Nothing, Nullable, Int8, UInt8, Int16, UInt16, Int32, UInt32, Int64,
        UInt64, Float32, Float64, String, Date, Date32, DateTime, ARRAY
    }
    public static class TypesConverter
    {
        private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);
        internal static object ConvertToCSharpVal(string? val, string destType)
        {
            // Create an UTF8Buffer with an offset to get better testing
            var b1 = Encoding.UTF8.GetBytes(val);
            var b2 = new byte[b1.Length + 100];
            Array.Copy(b1, 0, b2, 100, b1.Length);
            var v = new Utf8Buffer(b2, 100, b1.Length);
            return ToCSharpValues(v, destType);
        }

        private static object ToCSharpValues(Utf8Buffer srcVal, string destType)
        {
            try
            {
                // The most common conversions are checked first for maximum performance
                if (destType == "long")
                {
                    return FastParseInt64(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                }
                if (destType == "ulong")
                {
                    return FastParseInt64(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                }
                else if (destType == "int")
                {
                    return FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                }
                else if (destType == "uint")
                {
                    return FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                }
                else if (destType == "decimal")
                {
                    return FastParseDecimal(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                }
                else if (destType == "string")
                {
                    return srcVal.ToString();
                }
                else if (destType == "DateTime")
                {
                    return ConvertToDateTime(srcVal, FireBoltDataType.DateTime);
                }
                else if (destType == "short")
                {
                    int result = FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                    return checked((short)result);
                }
                else if (destType == "ushort")
                {
                    int result = FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                    return checked((ushort)result);
                }
                else if (destType == "sbyte")
                {
                    int result = FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                    return checked((sbyte)result);
                }
                else if (destType == "byte")
                {
                    int result = FastParseInt32(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                    return checked((byte)result);
                }
                else if (destType == "double")
                {
                    return Convert.ToDouble(srcVal.ToString(), CultureInfo.InvariantCulture);
                }
                else if (destType == "float")
                {
                    return Convert.ToSingle(srcVal.ToString(), CultureInfo.InvariantCulture);
                }
                else if (destType == "Boolean")
                {
                    var val = srcVal.Buffer[srcVal.Offset];
                    return val == '1';
                }
                else
                {
                    throw new FireboltException("Invalid destination type.");
                }
            }
            catch (FireboltException)
            {
                throw new FireboltException($"Error converting ' to '. Use GetString() to handle very large values");
            }
        }

        private static decimal FastParseDecimal(byte[] s, int offset, int len)
        {
            // Find any decimal point
            // Parse integer part and decimal part as 64-bit numbers
            // Calculate decimal number to return
            var decimalPos = Array.IndexOf(s, (byte)'.', offset, len);

            // No decimal point found, just parse as integer
            if (decimalPos < 0)
            {
                // If len > 19 (the number of digits in int64.MaxValue), the value is likely bigger
                // than max int64. Potentially, if it is a negative number it could be ok, but it 
                // is better to not to find out during the call to FastParseInt64.
                // Fallback to regular decimal constructor from string instead.
                if (len > 19)
                    return decimal.Parse(Utf8Buffer.Utf8.GetString(s, offset, len));

                try
                {
                    long i1 = FastParseInt64(s, offset, len);
                    return i1;
                }
                catch (OverflowException)
                {
                    // Fallback to regular decimal constructor from string instead.
                    return decimal.Parse(Utf8Buffer.Utf8.GetString(s, offset, len));
                }
            }
            else
            {
                decimalPos -= offset;
                var decimalLen = len - decimalPos - 1;
                long intPart;
                long decimalPart = 0;
                try
                {
                    intPart = FastParseInt64(s, offset, decimalPos);
                    decimalPart = FastParseInt64(s, offset + decimalPos + 1, decimalLen);
                }
                catch (OverflowException)
                {
                    // Fallback to regular decimal constructor from string instead.
                    return decimal.Parse(Utf8Buffer.Utf8.GetString(s, offset, len));
                }

                var isMinus = false;
                if (decimalPart < 0)
                    throw new FormatException();
                switch (intPart)
                {
                    case < 0:
                    {
                        isMinus = true;
                        intPart = -intPart;
                        if (intPart < 0)
                            throw new OverflowException();
                        break;
                    }
                    case 0:
                    {
                        // Sign is stripped from the Int64 for value of "-0"
                        if (s[offset] == '-')
                        {
                            isMinus = true;
                        }

                        break;
                    }
                }
                var d1 = new decimal(intPart);
                var d2 = new decimal((int)(decimalPart & 0xffffffff), (int)((decimalPart >> 32) & 0xffffffff), 0, false, (byte)decimalLen);
                var result = d1 + d2;
                if (isMinus)
                    result = -result;
                return result;
            }
        }

        private static Int64 FastParseInt64(byte[] s, int offset, int len)
        {
            long result = 0;
            var i = offset;
            var isMinus = false;
            if (len > 0 && s[i] == '-')
            {
                isMinus = true;
                i++;
            }
            var end = len + offset;
            for (; i < end; i++)
            {
                if ((ulong)result > (0x7fffffffffffffff / 10))
                    throw new OverflowException();
                var c = s[i] - '0';
                if (c < 0 || c > 9)
                    throw new FormatException();
                result = result * 10 + c;
            }
            if (isMinus)
            {
                result = -result;
                if (result > 0)
                    throw new OverflowException();
            }
            else
            {
                if (result < 0)
                    throw new OverflowException();
            }
            return result;
        }

        private static int FastParseInt32(byte[] s, int offset, int len)
        {
            var result = 0;
            var i = offset;
            var isMinus = false;
            if (len > 0 && s[i] == '-')
            {
                isMinus = true;
                i++;
            }
            var end = len + offset;
            for (; i < end; i++)
            {
                if ((uint)result > (0x7fffffff / 10))
                    throw new OverflowException();
                var c = s[i] - '0';
                if (c is < 0 or > 9)
                    throw new FormatException();
                result = result * 10 + c;
            }
            if (isMinus)
            {
                result = -result;
                if (result > 0)
                    throw new OverflowException();
            }
            else
            {
                if (result < 0)
                    throw new OverflowException();
            }
            return result;
        }

        private static DateTime ConvertToDateTime(Utf8Buffer srcVal, FireBoltDataType srcType)
        {
            switch (srcType)
            {
                case FireBoltDataType.DateTime:
                    var srcValLong = FastParseInt64(srcVal.Buffer, srcVal.Offset, srcVal.Length);
                    return UnixEpoch.AddDays(srcValLong);

                case FireBoltDataType.Date:
                case FireBoltDataType.Nothing:
                case FireBoltDataType.Nullable:
                case FireBoltDataType.Int8:
                case FireBoltDataType.UInt8:
                case FireBoltDataType.Int16:
                case FireBoltDataType.UInt16:
                case FireBoltDataType.Int32:
                case FireBoltDataType.UInt32:
                case FireBoltDataType.Int64:
                case FireBoltDataType.UInt64:
                case FireBoltDataType.Float32:
                case FireBoltDataType.Float64:
                case FireBoltDataType.String:
                case FireBoltDataType.Date32:
                case FireBoltDataType.ARRAY:
                default:
                    throw new FireboltException("Wrong date type");
            }
        }

        public static object ConvertFireBoltMetaTypes(Meta meta)
        {
            var csharpType = meta.Type switch
            {
                "Int8" => "sbyte",
                "UInt8" => "byte",
                "Int16" => "short",
                "UInt16" => "ushort",
                "Int32" => "int",
                "UInt32" => "uint",
                "Int64" => "long",
                "UInt64" => "ulong",
                "Float32" => "float",
                "Float64" => "double",
                "String" => "string",
                "Date" => "Date",
                "Date32" => "Date",
                "DateTime" => "DateTime",
                "Nothing" => "null",
                "Nullable" => "null",
                "decimal" => "decimal",
                _ => throw new FireboltException("Wrong date type"),
            };
            return csharpType;
        }
    }
}
public class NewMeta
{
    public ArrayList Data { get; set; }
    public string Meta { get; set; }
}

public class Utf8Buffer
{
    // Cache for maximum performance
    public static readonly Encoding Utf8 = Encoding.UTF8;

    public readonly byte[] Buffer;
    public readonly int Offset;
    public readonly int Length;

    public Utf8Buffer(byte[] buffer, int offset, int length)
    {
        this.Buffer = buffer;
        this.Offset = offset;
        this.Length = length;
    }

    public override string ToString() => Utf8.GetString(Buffer, Offset, Length);

}

/// <summary>
/// Specifies the data type of a field (column) or a <see cref="FireboltParameter"/> object.
/// </summary>
public enum FireboltDbType
{
    /// <summary>
    /// The type is not supported by the client. An encoding should be defined explicitly via <see cref="FireboltParameter.StringEncoding"/>
    /// or <see cref="FireboltColumnSettings.StringEncoding"/>.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.AnsiString"/>.</remarks>
    AnsiString = DbType.AnsiString,

    /// <summary>
    /// An array of bytes.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Binary"/>.</remarks>
    Binary = DbType.Binary,

    /// <summary>
    /// An 8-bit unsigned integer ranging in value from 0 to 255.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Byte"/>.</remarks>
    Byte = DbType.Byte,

    /// <summary>
    /// A simple type representing Boolean values of <see langword="true"/> or <see langword="false"/>.        
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Boolean"/>.</remarks>
    Boolean = DbType.Boolean,

    /// <summary>
    /// A type representing a date value without a time.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Date"/>.</remarks>
    Date = DbType.Date,

    /// <summary>
    /// A type representing a date and time value.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.DateTime"/>.</remarks>
    DateTime = DbType.DateTime,

    /// <summary>
    /// The Firebolt type Decimal(38, 9).
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Decimal"/>.</remarks>
    Decimal = DbType.Decimal,

    /// <inheritdoc cref="DbType.Double"/>
    /// <remarks>This value corresponds to <see cref="DbType.Double"/>.</remarks>
    Double = DbType.Double,

    /// <inheritdoc cref="DbType.Int16"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int16"/>.</remarks>
    Int16 = DbType.Int16,

    /// <inheritdoc cref="DbType.Int32"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int32"/>.</remarks>
    Int32 = DbType.Int32,

    /// <inheritdoc cref="DbType.Int64"/>
    /// <remarks>This value corresponds to <see cref="DbType.Int64"/>.</remarks>
    Int64 = DbType.Int64,

    /// <summary>
    /// A general type representing a value of either an unknown type or the Firebolt type Nothing.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Object"/>.</remarks>
    Object = DbType.Object,

    /// <inheritdoc cref="DbType.SByte"/>
    /// <remarks>This value corresponds to <see cref="DbType.SByte"/>.</remarks>
    SByte = DbType.SByte,

    /// <inheritdoc cref="DbType.Single"/>
    /// <remarks>This value corresponds to <see cref="DbType.Single"/>.</remarks>
    Single = DbType.Single,

    /// <summary>
    /// A variable-length string.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.String"/>.</remarks>
    String = DbType.String,

    /// <summary>
    /// The type is not supported by the client.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.Time"/>.</remarks>
    Time = DbType.Time,

    /// <inheritdoc cref="DbType.UInt16"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt16"/>.</remarks>
    UInt16 = DbType.UInt16,

    /// <inheritdoc cref="DbType.UInt32"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt32"/>.</remarks>
    UInt32 = DbType.UInt32,

    /// <inheritdoc cref="DbType.UInt64"/>
    /// <remarks>This value corresponds to <see cref="DbType.UInt64"/>.</remarks>
    UInt64 = DbType.UInt64,

    /// <summary>
    /// A type representing a date and time value with time zone awareness.
    /// </summary>
    /// <remarks>This value corresponds to <see cref="DbType.DateTimeOffset"/>.</remarks>
    DateTimeOffset = DbType.DateTimeOffset,

}


