package geneticsalesman;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.primitives.Ints;

public class VariableByteEncoding {
	public static void writeVNumber(Output output, long n) {
		if ((n < 128) && (n >= -32)) {
			output.writeByte((int) n);
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
				output.writeByte(firstByte - 52);
				output.writeByte((int) n);
				return;
			}
			// fall it through to firstByte==0/-1, len=3.
			firstByte >>= 8;
		case 3:
			if ((firstByte < 16) && (firstByte >= -16)) {
				output.writeByte(firstByte - 88);
				output.writeShort((int) n);
				return;
			}
			// fall it through to firstByte==0/-1, len=4.
			firstByte >>= 8;
		case 4:
			if ((firstByte < 8) && (firstByte >= -8)) {
				output.writeByte(firstByte - 112);
				output.writeShort(((int) n) >>> 8);
				output.writeByte((int) n);
				return;
			}
			output.writeByte(len - 129);
			output.writeInt((int) n);
			return;
		case 5:
			output.writeByte(len - 129);
			output.writeInt((int) (n >>> 8));
			output.writeByte((int) n);
			return;
		case 6:
			output.writeByte(len - 129);
			output.writeInt((int) (n >>> 16));
			output.writeShort((int) n);
			return;
		case 7:
			output.writeByte(len - 129);
			output.writeInt((int) (n >>> 24));
			output.writeShort((int) (n >>> 8));
			output.writeByte((int) n);
			return;
		case 8:
			output.writeByte(len - 129);
			output.writeLong(n);
			return;
		default:
			throw new RuntimeException("Internel error");
		}
	}

	public static long readVLong(Input input) {
		int firstByte = input.readByte();
		if (firstByte >= -32) {
			return firstByte;
		}

		switch ((firstByte + 128) / 8) {
		case 11:
		case 10:
		case 9:
		case 8:
		case 7:
			return ((firstByte + 52) << 8) | input.readByteUnsigned();
		case 6:
		case 5:
		case 4:
		case 3:
			return ((firstByte + 88) << 16) | input.readShortUnsigned();
		case 2:
		case 1:
			return ((firstByte + 112) << 24) | (input.readShortUnsigned() << 8)
					| input.readByteUnsigned();
		case 0:
			int len = firstByte + 129;
			switch (len) {
			case 4:
				return input.readInt();
			case 5:
				return ((long) input.readInt()) << 8 | input.readByteUnsigned();
			case 6:
				return ((long) input.readInt()) << 16 | input.readShortUnsigned();
			case 7:
				return ((long) input.readInt()) << 24
						| (input.readShortUnsigned() << 8) | input.readByteUnsigned();
			case 8:
				return input.readLong();
			default:
				throw new RuntimeException("Corrupted VLong encoding");
			}
		default:
			throw new RuntimeException("Internal error");
		}
	}

	public static int readVInt(Input input) {
		return Ints.checkedCast(readVLong(input));
	}
	
	public static void writeVInts(Output output, int[] values) {
		writeVNumber(output, values.length);
		int prev=0;
		for(int i=0;i<values.length;i++) {
			writeVNumber(output, values[i]-prev);
			prev=values[i];
		}
	}
	
	public static int[] readVInts(Input input) {
		int length=readVInt(input);
		int[] values=new int[length];
		int prev=0;
		for(int i=0;i<values.length;i++) {
			values[i]=readVInt(input)+prev;
			prev=values[i];
		}
		return values;
	}
	
	public static void writeVLongs(Output out, long[] values) {
		writeVNumber(out, values.length);
		long prev=0;
		for(int i=0;i<values.length;i++) {
			writeVNumber(out, values[i]-prev);
			prev=values[i];
		}
	}
	
	public static long[] readVLongs(Input in) {
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
