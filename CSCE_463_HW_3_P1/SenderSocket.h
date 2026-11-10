#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <stdio.h>
#include <string.h>

#pragma comment(lib, "ws2_32.lib")

#define MAGIC_PORT 22345
#define MAX_PKT_SIZE (1500 - 28)
#define MAGIC_PROTOCOL 0x8311AA
#define FORWARD_PATH 0
#define RETURN_PATH 1
#define STATUS_OK 0
#define ALREADY_CONNECTED 1
#define NOT_CONNECTED 2
#define INVALID_NAME 3
#define FAILED_SEND 4
#define TIMEOUT 5
#define FAILED_RECV 6
#define SYN_ATTEMPTS 50
#define FIN_ATTEMPTS 50
#define DATA_ATTEMPTS 50
#define MAX_ROUTER_PACKETS 1000000

#pragma pack(push, 1)

class Flags {
public:
    DWORD reserved : 5;
    DWORD SYN : 1;
    DWORD ACK : 1;
    DWORD FIN : 1;
    DWORD magic : 24;
    Flags() {
        memset(this, 0, sizeof(*this));
        magic = MAGIC_PROTOCOL;
    }
};

class SenderDataHeader {
public:
    Flags flags;
    DWORD seq;
};

class LinkProperties {
public:
    float RTT;
    float speed;
    float pLoss[2];
    DWORD bufferSize;
    LinkProperties() {
        memset(this, 0, sizeof(*this));
    }
};

class SenderSynHeader {
public:
    SenderDataHeader sdh;
    LinkProperties lp;
};

class ReceiverHeader {
public:
    Flags flags;
    DWORD recvWnd;
    DWORD ackSeq;
};

#pragma pack(pop)

class Packet {
public:
    int size;
    int attempts;
    double txTime;
    char pkt[MAX_PKT_SIZE];
};

double getTime();
class SenderSocket {
public:
    SenderSocket();
    ~SenderSocket();
    int Open(char* targetHost, int port, int senderWindow, LinkProperties* lp);
    int Send(char* buffer, int bytes);
    int Close(double* elapsedTime);
    double GetRTT();
    DWORD GetChecksum();

private:
    SOCKET sock;
    sockaddr_in server;
    Packet* pending;
    int windowSize;
    int wsaError;
    int errorCode;
    bool connected;
    DWORD senderBase;
    DWORD nextSeq;
    DWORD nextToSend;
    DWORD recvWnd;
    DWORD effectiveWindow;
    DWORD lastReleased;
    DWORD timeouts;
    DWORD fastRetx;
    DWORD dupAcks;
    DWORD checksum;
    UINT64 ackBytes;
    double startTime;
    double estRtt;
    double devRtt;
    double rto;
    double timerExpire;
    double firstDataTime;
    double lastAckTime;
    HANDLE eventQuit;
    HANDLE eventDrained;
    HANDLE socketReady;
    HANDLE empty;
    HANDLE full;
    HANDLE workerThread;
    HANDLE statsThread;
    CRITICAL_SECTION lock;

    static DWORD WINAPI workerStart(void* data);
    static DWORD WINAPI statsStart(void* data);
    void workerRun();
    void statsRun();
    void stopThreads();
    void cleanUp();
    void fail(int status);
    void updateRtt(double sample);
    int waitForAck(bool isSyn, double sendTime, ReceiverHeader* reply);
    int sendPacket(DWORD seq);
    int receiveAck();
};