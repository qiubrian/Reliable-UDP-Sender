#include <windows.h>
#include <stddef.h>

class CheckSum {
private:
    DWORD crcTable[256];

public:
    CheckSum();
    DWORD CRC32(unsigned char* buf, size_t len);
};
