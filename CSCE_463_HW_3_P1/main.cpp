#include "SenderSocket.h"
#include "CheckSum.h"
#include <stdlib.h>
#include <errno.h>
#include <limits.h>
#include <cmath>
#include <new>

using namespace std;

bool readInt(char* text, int* value) {
    char* end;
    errno = 0;
    long number = strtol(text, &end, 10);
    if (end == text || *end != '\0' || errno == ERANGE || number < INT_MIN || number > INT_MAX) {
        return false;
    }
    *value = (int)number;
    return true;
}

bool readDouble(char* text, double* value) {
    char* end;
    errno = 0;
    *value = strtod(text, &end);
    return end != text && *end == '\0' && errno != ERANGE && isfinite(*value);
}

void printUsage(char* program) {
    printf("Usage: %s <host> <power> <window> <RTT-sec> <forward-loss> <return-loss> <link-Mbps>\n", program);
    printf("power: 0..32, window: 1..%d, RTT: [0,30), loss: [0,1), Mbps: (0,10000]\n", MAX_ROUTER_PACKETS - FIN_ATTEMPTS);
}

int main(int argc, char** argv) {
    if (argc != 8) {
        printUsage(argv[0]);
        return 1;
    }
    char* targetHost = argv[1];
    int power;
    int senderWindow;
    double rtt;
    double forwardLoss;
    double returnLoss;
    double linkSpeed;
    if (*targetHost == '\0' || !readInt(argv[2], &power) || !readInt(argv[3], &senderWindow) || !readDouble(argv[4], &rtt) || !readDouble(argv[5], &forwardLoss) || !readDouble(argv[6], &returnLoss) || !readDouble(argv[7], &linkSpeed)) {
        printUsage(argv[0]);
        return 1;
    }
    if (power < 0 || power > 32 || senderWindow < 1 || senderWindow > MAX_ROUTER_PACKETS - FIN_ATTEMPTS || rtt < 0 || rtt >= 30 || forwardLoss < 0 || forwardLoss >= 1 || returnLoss < 0 || returnLoss >= 1 || linkSpeed <= 0 || linkSpeed > 10000) {
        printUsage(argv[0]);
        return 1;
    }
    size_t maxDwords = (size_t)-1 / sizeof(DWORD);
    if (((UINT64)1 << power) > maxDwords) {
        printUsage(argv[0]);
        return 1;
    }
    LinkProperties lp;
    lp.RTT = (float)rtt;
    lp.speed = (float)(1000000 * linkSpeed);
    lp.pLoss[FORWARD_PATH] = (float)forwardLoss;
    lp.pLoss[RETURN_PATH] = (float)returnLoss;
    if (lp.RTT >= 30 || lp.speed <= 0 || lp.pLoss[FORWARD_PATH] >= 1 || lp.pLoss[RETURN_PATH] >= 1) {
        printUsage(argv[0]);
        return 1;
    }
    printf("Main:   sender W = %d, RTT %.3f sec, loss %g / %g, link %g Mbps\n", senderWindow, rtt, forwardLoss, returnLoss, linkSpeed);
    printf("Main:   initializing DWORD array with 2^%d elements... ", power);
    fflush(stdout);
    double initTime = getTime();
    UINT64 dwordBufSize = (UINT64)1 << power;
    DWORD* dwordBuf = new (nothrow) DWORD[(size_t)dwordBufSize];
    if (dwordBuf == NULL) {
        printf("allocation failed\n");
        return 1;
    }
    for (UINT64 i = 0; i < dwordBufSize; i++) {
        dwordBuf[i] = (DWORD)i;
    }
    printf("done in %.0f ms\n", (getTime() - initTime) * 1000);
    SenderSocket ss;
    double openTime = getTime();
    int status = ss.Open(targetHost, MAGIC_PORT, senderWindow, &lp);
    double connectedTime = getTime();
    if (status != STATUS_OK) {
        printf("Main:   connect failed with status %d\n", status);
        delete[] dwordBuf;
        return status;
    }
    printf("Main:   connected to %s in %.3f sec, pkt size %d bytes\n", targetHost, connectedTime - openTime, MAX_PKT_SIZE);
    char* charBuf = (char*)dwordBuf;
    UINT64 byteBufferSize = dwordBufSize << 2;
    UINT64 off = 0;
    while (off < byteBufferSize) {
        int bytes = MAX_PKT_SIZE - sizeof(SenderDataHeader);
        if (byteBufferSize - off < (UINT64)bytes) {
            bytes = (int)(byteBufferSize - off);
        }
        status = ss.Send(charBuf + off, bytes);
        if (status != STATUS_OK) {
            printf("Main:   send failed with status %d\n", status);
            delete[] dwordBuf;
            return status;
        }
        off += bytes;
    }
    double elapsedTime;
    status = ss.Close(&elapsedTime);
    if (status != STATUS_OK) {
        printf("Main:   close failed with status %d\n", status);
        delete[] dwordBuf;
        return status;
    }
    CheckSum cs;
    DWORD check = cs.CRC32((unsigned char*)charBuf, (size_t)byteBufferSize);
    double estRtt = ss.GetRTT();
    double rate = 0;
    double idealRate = 0;
    if (elapsedTime > 0) {
        rate = (double)byteBufferSize * 8 / elapsedTime / 1000;
    }
    if (estRtt > 0) {
        idealRate = (double)senderWindow * (MAX_PKT_SIZE - sizeof(SenderDataHeader)) * 8 / estRtt / 1000;
    }
    printf("Main:   transfer finished in %.3f sec, %.2f Kbps, checksum %lX\n", elapsedTime, rate, check);
    printf("Main:   estRTT %.3f, ideal rate %.2f Kbps\n", estRtt, idealRate);
    delete[] dwordBuf;
    if (check != ss.GetChecksum()) {
        printf("Main:   checksum mismatch: sender %lX, receiver %lX\n", check, ss.GetChecksum());
        return 1;
    }
    return 0;
}