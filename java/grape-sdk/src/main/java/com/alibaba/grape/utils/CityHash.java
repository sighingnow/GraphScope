/*
 * Copyright 2021 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.grape.utils;

public class CityHash {
  private static final boolean IS_BIG_EDIAN =
      !"little".equals(System.getProperty("sun.cpu.endian"));

  // Some primes between 2^63 and 2^64 for various uses.
  public static final long k0 = 0xc3a5c85c97cb3127L;
  public static final long k1 = 0xb492b66fbe98f273L;
  public static final long k2 = 0x9ae16a3b2f90404fL;
  public static final long kMul = 0x9ddfea08eb382d69L;

  // Magic numbers for 32-bit hashing.  Copied from Murmur3.
  public static final int c1 = 0xcc9e2d51;
  public static final int c2 = 0x1b873593;

  public static long hash64raw(byte[] byteArray, int len) {
    if (len <= 32) {
      if (len <= 16) {
        return hashLen0to16(byteArray, len);
      } else {
        return hashLen17to32(byteArray, len);
      }
    } else if (len <= 64) {
      return hashLen33to64(byteArray, len);
    }

    // For strings over 64 bytes we hash the end first, and then as we
    // loop we keep 56 bytes of state: v, w, x, y, and z.
    long x = fetch64(byteArray, len - 40);
    long y = fetch64(byteArray, len - 16) + fetch64(byteArray, len - 56);
    long z = hashLen16(fetch64(byteArray, len - 48) + len, fetch64(byteArray, len - 24));
    Number128 v = weakHashLen32WithSeeds(byteArray, len - 64, len, z);
    Number128 w = weakHashLen32WithSeeds(byteArray, len - 32, y + k1, x);
    x = x * k1 + fetch64(byteArray, 0);

    // Decrease len to the nearest multiple of 64, and operate on 64-byte chunks.
    len = (len - 1) & ~63;
    int pos = 0;
    do {
      x = rotate(x + y + v.getLowValue() + fetch64(byteArray, pos + 8), 37) * k1;
      y = rotate(y + v.getHiValue() + fetch64(byteArray, pos + 48), 42) * k1;
      x ^= w.getHiValue();
      y += v.getLowValue() + fetch64(byteArray, pos + 40);
      z = rotate(z + w.getLowValue(), 33) * k1;
      v = weakHashLen32WithSeeds(byteArray, pos, v.getHiValue() * k1, x + w.getLowValue());
      w = weakHashLen32WithSeeds(byteArray, pos + 32, z + w.getHiValue(),
                                 y + fetch64(byteArray, pos + 16));
      // swap z,x value
      long swapValue = x;
      x = z;
      z = swapValue;
      pos += 64;
      len -= 64;
    } while (len != 0);
    return hashLen16(hashLen16(v.getLowValue(), w.getLowValue()) + shiftMix(y) * k1 + z,
                     hashLen16(v.getHiValue(), w.getHiValue()) + x);
  }

  private static long hashLen0to16(byte[] byteArray, int len) {
    if (len >= 8) {
      long mul = k2 + len * 2;
      long a = fetch64(byteArray, 0) + k2;
      long b = fetch64(byteArray, len - 8);
      long c = rotate(b, 37) * mul + a;
      long d = (rotate(a, 25) + b) * mul;
      return hashLen16(c, d, mul);
    }
    if (len >= 4) {
      long mul = k2 + len * 2;
      long a = fetch32(byteArray, 0) & 0xffffffffL;
      return hashLen16(len + (a << 3), fetch32(byteArray, len - 4) & 0xffffffffL, mul);
    }
    if (len > 0) {
      int a = byteArray[0] & 0xff;
      int b = byteArray[len >>> 1] & 0xff;
      int c = byteArray[len - 1] & 0xff;
      int y = a + (b << 8);
      int z = len + (c << 2);
      return shiftMix(y * k2 ^ z * k0) * k2;
    }
    return k2;
  }

  // This probably works well for 16-byte strings as well, but it may be overkill
  // in that case.
  private static long hashLen17to32(byte[] byteArray, int len) {
    long mul = k2 + len * 2;
    long a = fetch64(byteArray, 0) * k1;
    long b = fetch64(byteArray, 8);
    long c = fetch64(byteArray, len - 8) * mul;
    long d = fetch64(byteArray, len - 16) * k2;
    return hashLen16(rotate(a + b, 43) + rotate(c, 30) + d, a + rotate(b + k2, 18) + c, mul);
  }

  private static long hashLen33to64(byte[] byteArray, int len) {
    long mul = k2 + len * 2;
    long a = fetch64(byteArray, 0) * k2;
    long b = fetch64(byteArray, 8);
    long c = fetch64(byteArray, len - 24);
    long d = fetch64(byteArray, len - 32);
    long e = fetch64(byteArray, 16) * k2;
    long f = fetch64(byteArray, 24) * 9;
    long g = fetch64(byteArray, len - 8);
    long h = fetch64(byteArray, len - 16) * mul;
    long u = rotate(a + g, 43) + (rotate(b, 30) + c) * 9;
    long v = ((a + g) ^ d) + f + 1;
    long w = Long.reverseBytes((u + v) * mul) + h;
    long x = rotate(e + f, 42) + c;
    long y = (Long.reverseBytes((v + w) * mul) + g) * mul;
    long z = e + f + c;
    a = Long.reverseBytes((x + z) * mul + y) + b;
    b = shiftMix((z + a) * mul + d + h) * mul;
    return b + x;
  }

  private static long loadUnaligned64(final byte[] byteArray, final int start) {
    long result = 0;
    OrderIter orderIter = new OrderIter(8, IS_BIG_EDIAN);
    while (orderIter.hasNext()) {
      int next = orderIter.next();
      long value = (byteArray[next + start] & 0xffL) << (next * 8);
      result |= value;
    }
    return result;
  }

  private static int loadUnaligned32(final byte[] byteArray, final int start) {
    int result = 0;
    OrderIter orderIter = new OrderIter(4, IS_BIG_EDIAN);
    while (orderIter.hasNext()) {
      int next = orderIter.next();
      int value = (byteArray[next + start] & 0xff) << (next * 8);
      result |= value;
    }
    return result;
  }

  private static long fetch64(byte[] byteArray, final int start) {
    return loadUnaligned64(byteArray, start);
  }

  private static int fetch32(byte[] byteArray, final int start) {
    return loadUnaligned32(byteArray, start);
  }

  private static long rotate(long val, int shift) {
    // Avoid shifting by 64: doing so yields an undefined result.
    return shift == 0 ? val : ((val >>> shift) | (val << (64 - shift)));
  }

  private static int rotate32(int val, int shift) {
    // Avoid shifting by 32: doing so yields an undefined result.
    return shift == 0 ? val : ((val >>> shift) | (val << (32 - shift)));
  }

  private static long hashLen16(long u, long v, long mul) {
    // Murmur-inspired hashing.
    long a = (u ^ v) * mul;
    a ^= (a >>> 47);
    long b = (v ^ a) * mul;
    b ^= (b >>> 47);
    b *= mul;
    return b;
  }

  private static long hashLen16(long u, long v) {
    return hash128to64(new Number128(u, v));
  }

  private static long hash128to64(final Number128 number128) {
    // Murmur-inspired hashing.
    long a = (number128.getLowValue() ^ number128.getHiValue()) * kMul;
    a ^= (a >>> 47);
    long b = (number128.getHiValue() ^ a) * kMul;
    b ^= (b >>> 47);
    b *= kMul;
    return b;
  }

  private static long shiftMix(long val) {
    return val ^ (val >>> 47);
  }

  private static Number128 weakHashLen32WithSeeds(long w, long x, long y, long z, long a, long b) {
    a += w;
    b = rotate(b + a + z, 21);
    long c = a;
    a += x;
    a += y;
    b += rotate(a, 44);
    return new Number128(a + z, b + c);
  }

  // Return a 16-byte hash for s[0] ... s[31], a, and b.  Quick and dirty.
  private static Number128 weakHashLen32WithSeeds(byte[] byteArray, int start, long a, long b) {
    return weakHashLen32WithSeeds(fetch64(byteArray, start), fetch64(byteArray, start + 8),
                                  fetch64(byteArray, start + 16), fetch64(byteArray, start + 24), a,
                                  b);
  }

  private static class OrderIter {
    private final int size;
    private final boolean isBigEdian;
    private int index;

    OrderIter(int size, boolean isBigEdian) {
      this.size = size;
      this.isBigEdian = isBigEdian;
    }

    boolean hasNext() {
      return index < size;
    }

    int next() {
      if (!isBigEdian) {
        return index++;
      } else {
        return size - 1 - index++;
      }
    }
  }
}
