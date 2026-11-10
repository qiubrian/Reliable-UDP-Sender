#include "CheckSum.h"

CheckSum::CheckSum() {
    for (DWORD i = 0; i < 256; i++) {
        DWORD c = i;
        for (int j = 0; j < 8; j++) {
            if ((c & 1) != 0) {
                c = 0xEDB88320 ^ (c >> 1);
            } else {
                c = c >> 1;
            }
        }
        crcTable[i] = c;
    }
}

DWORD CheckSum::CRC32(unsigned char* buf, size_t len) {
    DWORD c = 0xFFFFFFFF;
    for (size_t i = 0; i < len; i++) {
        c = crcTable[(c ^ buf[i]) & 0xFF] ^ (c >> 8);
    }
	return c ^ 0xFFFFFFFF;
}