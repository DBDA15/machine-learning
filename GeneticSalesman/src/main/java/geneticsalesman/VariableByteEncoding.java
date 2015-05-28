package geneticsalesman;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.google.common.primitives.Ints;

public class VariableByteEncoding {
	public static void writeVNumber(DataOutput out, long n) throws IOException {
		if ((n < 128) && (n >= -32)) {
			out.writeByte((int) n);
			return;
		}

		long un = (n < 0) ? ~n : n;
		// how many bytes do we need to represent the number with sign bit?
		int len = (Long.SIZE - Long.numberOfLeadingZeros(un)) / 8 + 1;
		int firstByte = (int) (n >> ((len - 1) * 8));
		switch (len) {
		case 1:
			// fall it through to firstByte==-1, len=2.
			firstByte >>= 8;
		case 2:
			if ((firstByte < 20) && (firstByte >= -20)) {
				out.writeByte(firstByte - 52);
				out.writeByte((int) n);
				return;
			}
			// fall it through to firstByte==0/-1, len=3.
			firstByte >>= 8;
		case 3:
			if ((firstByte < 16) && (firstByte >= -16)) {
				out.writeByte(firstByte - 88);
				out.writeShort((int) n);
				return;
			}
			// fall it through to firstByte==0/-1, len=4.
			firstByte >>= 8;
		case 4:
			if ((firstByte < 8) && (firstByte >= -8)) {
				out.writeByte(firstByte - 112);
				out.writeShort(((int) n) >>> 8);
				out.writeByte((int) n);
				return;
			}
			out.writeByte(len - 129);
			out.writeInt((int) n);
			return;
		case 5:
			out.writeByte(len - 129);
			out.writeInt((int) (n >>> 8));
			out.writeByte((int) n);
			return;
		case 6:
			out.writeByte(len - 129);
			out.writeInt((int) (n >>> 16));
			out.writeShort((int) n);
			return;
		case 7:
			out.writeByte(len - 129);
			out.writeInt((int) (n >>> 24));
			out.writeShort((int) (n >>> 8));
			out.writeByte((int) n);
			return;
		case 8:
			out.writeByte(len - 129);
			out.writeLong(n);
			return;
		default:
			throw new RuntimeException("Internel error");
		}
	}

	public static long readVLong(DataInput in) throws IOException {
		int firstByte = in.readByte();
		if (firstByte >= -32) {
			return firstByte;
		}

		switch ((firstByte + 128) / 8) {
		case 11:
		case 10:
		case 9:
		case 8:
		case 7:
			return ((firstByte + 52) << 8) | in.readUnsignedByte();
		case 6:
		case 5:
		case 4:
		case 3:
			return ((firstByte + 88) << 16) | in.readUnsignedShort();
		case 2:
		case 1:
			return ((firstByte + 112) << 24) | (in.readUnsignedShort() << 8)
					| in.readUnsignedByte();
		case 0:
			int len = firstByte + 129;
			switch (len) {
			case 4:
				return in.readInt();
			case 5:
				return ((long) in.readInt()) << 8 | in.readUnsignedByte();
			case 6:
				return ((long) in.readInt()) << 16 | in.readUnsignedShort();
			case 7:
				return ((long) in.readInt()) << 24
						| (in.readUnsignedShort() << 8) | in.readUnsignedByte();
			case 8:
				return in.readLong();
			default:
				throw new IOException("Corrupted VLong encoding");
			}
		default:
			throw new RuntimeException("Internal error");
		}
	}

	public static int readVInt(DataInput in) throws IOException {
		return Ints.checkedCast(readVLong(in));
	}
	
	public static void writeVInts(DataOutput out, int[] values) throws IOException {
		writeVNumber(out, values.length);
		int prev=0;
		for(int i=0;i<values.length;i++) {
			writeVNumber(out, values[i]-prev);
			prev=values[i];
		}
	}
	
	public static int[] readVInts(DataInput in) throws IOException {
		int length=readVInt(in);
		int[] values=new int[length];
		int prev=0;
		for(int i=0;i<values.length;i++) {
			values[i]=readVInt(in)+prev;
			prev=values[i];
		}
		return values;
	}
	
	public static void writeVLongs(DataOutput out, long[] values) throws IOException {
		writeVNumber(out, values.length);
		long prev=0;
		for(int i=0;i<values.length;i++) {
			writeVNumber(out, values[i]-prev);
			prev=values[i];
		}
	}
	
	public static long[] readVLongs(DataInput in) throws IOException {
		int length=readVInt(in);
		long[] values=new long[length];
		long prev=0;
		for(int i=0;i<values.length;i++) {
			values[i]=readVLong(in)+prev;
			prev=values[i];
		}
		return values;
	}
}
