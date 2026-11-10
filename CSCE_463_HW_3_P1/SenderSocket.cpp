#include "SenderSocket.h"
#include <cmath>
#include <new>

using namespace std;

double getTime() {
    LARGE_INTEGER count;
    LARGE_INTEGER frequency;
    QueryPerformanceCounter(&count);
    QueryPerformanceFrequency(&frequency);
    return (double)count.QuadPart / frequency.QuadPart;
}
SenderSocket::SenderSocket() {
    sock = INVALID_SOCKET;
    memset(&server, 0, sizeof(server));
    pending = NULL;
    windowSize = 0;
    errorCode = STATUS_OK;
    connected = false;
    senderBase = 0;
    nextSeq = 0;
    nextToSend = 0;
    recvWnd = 0;
    effectiveWindow = 0;
    lastReleased = 0;
    timeouts = 0;
    fastRetx = 0;
    dupAcks = 0;
    checksum = 0;
    ackBytes = 0;
    startTime = getTime();
    estRtt = 0;
    devRtt = 0;
    rto = 1.0;
    timerExpire = 0;
    firstDataTime = 0;
    lastAckTime = 0;
    eventQuit = NULL;
    eventDrained = NULL;
    socketReady = NULL;
    empty = NULL;
    full = NULL;
    workerThread = NULL;
    statsThread = NULL;
    InitializeCriticalSection(&lock);
    WSADATA wsaData;
    wsaError = WSAStartup(MAKEWORD(2, 2), &wsaData);
}
SenderSocket::~SenderSocket() {
    cleanUp();
    DeleteCriticalSection(&lock);
    if (wsaError == 0) {
        WSACleanup();
    }
}
void SenderSocket::stopThreads() {
    if (eventQuit != NULL) {
        SetEvent(eventQuit);
    }
    if (workerThread != NULL) {
        WaitForSingleObject(workerThread, INFINITE);
        CloseHandle(workerThread);
        workerThread = NULL;
    }
    if (statsThread != NULL) {
        WaitForSingleObject(statsThread, INFINITE);
        CloseHandle(statsThread);
        statsThread = NULL;
    }
}
void SenderSocket::cleanUp() {
    stopThreads();
    if (sock != INVALID_SOCKET) {
        closesocket(sock);
        sock = INVALID_SOCKET;
    }
    if (eventQuit != NULL) {
        CloseHandle(eventQuit);
    }
    if (eventDrained != NULL) {
        CloseHandle(eventDrained);
    }
    if (socketReady != NULL) {
        CloseHandle(socketReady);
    }
    if (empty != NULL) {
        CloseHandle(empty);
    }
    if (full != NULL) {
        CloseHandle(full);
    }
    eventQuit = NULL;
    eventDrained = NULL;
    socketReady = NULL;
    empty = NULL;
    full = NULL;
    delete[] pending;
    pending = NULL;
    connected = false;
}
void SenderSocket::fail(int status) {
    EnterCriticalSection(&lock);
    if (errorCode == STATUS_OK) {
        errorCode = status;
    }
    LeaveCriticalSection(&lock);
    SetEvent(eventQuit);
}
void SenderSocket::updateRtt(double sample) {
    devRtt = 0.75 * devRtt + 0.25 * fabs(sample - estRtt);
    estRtt = 0.875 * estRtt + 0.125 * sample;
    double deviation = devRtt;
    if (deviation < 0.010) {
        deviation = 0.010;
    }
    rto = estRtt + 4 * deviation;
}
int SenderSocket::Open(char* targetHost, int port, int senderWindow, LinkProperties* lp) {
    if (connected) {
        return ALREADY_CONNECTED;
    }
    cleanUp();
    if (wsaError != 0) {
        printf("Main: WSAStartup failed with %d\n", wsaError);
        return FAILED_SEND;
    }
    addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_DGRAM;
    hints.ai_protocol = IPPROTO_UDP;
    addrinfo* hostInfo = NULL;
    if (getaddrinfo(targetHost, NULL, &hints, &hostInfo) != 0) {
        printf("[%7.3f] --> target %s is invalid\n", getTime() - startTime, targetHost);
        return INVALID_NAME;
    }
    server = *(sockaddr_in*)hostInfo->ai_addr;
    freeaddrinfo(hostInfo);
    server.sin_port = htons((u_short)port);
    sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    if (sock == INVALID_SOCKET) {
        printf("Main: socket failed with %d\n", WSAGetLastError());
        return FAILED_SEND;
    }
    sockaddr_in localAddr;
    memset(&localAddr, 0, sizeof(localAddr));
    localAddr.sin_family = AF_INET;
    localAddr.sin_addr.s_addr = htonl(INADDR_ANY);
    localAddr.sin_port = htons(0);
    if (bind(sock, (sockaddr*)&localAddr, sizeof(localAddr)) == SOCKET_ERROR) {
        printf("Main: bind failed with %d\n", WSAGetLastError());
        cleanUp();
        return FAILED_SEND;
    }
    int kernelBuffer = 20000000;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVBUF, (char*)&kernelBuffer, sizeof(kernelBuffer)) == SOCKET_ERROR || setsockopt(sock, SOL_SOCKET, SO_SNDBUF, (char*)&kernelBuffer, sizeof(kernelBuffer)) == SOCKET_ERROR) {
        printf("Main: setsockopt failed with %d\n", WSAGetLastError());
        cleanUp();
        return FAILED_SEND;
    }
    windowSize = senderWindow;
    lp->bufferSize = senderWindow + FIN_ATTEMPTS;
    rto = 2 * lp->RTT;
    if (rto < 1) {
        rto = 1;
    }
    SenderSynHeader synPacket;
    synPacket.sdh.flags.SYN = 1;
    synPacket.sdh.seq = 0;
    synPacket.lp = *lp;
    ReceiverHeader reply;
    double sample = 0;
    int status = TIMEOUT;
    for (int attempt = 1; attempt <= SYN_ATTEMPTS; attempt++) {
        double sendTime = getTime();
        int bytes = sendto(sock, (char*)&synPacket, sizeof(synPacket), 0, (sockaddr*)&server, sizeof(server));
        if (bytes == SOCKET_ERROR) {
            int error = WSAGetLastError();
            printf("[%7.3f]\t--> failed sendto with %d\n", getTime() - startTime, error);
            cleanUp();
            return FAILED_SEND;
        }
        status = waitForAck(true, sendTime, &reply);
        if (status == STATUS_OK) {
            sample = getTime() - sendTime;
            break;
        }
        if (status != TIMEOUT) {
            cleanUp();
            return status;
        }
    }
    if (status != STATUS_OK) {
        cleanUp();
        return status;
    }
    estRtt = sample;
    devRtt = sample / 2;
    double deviation = devRtt;
    if (deviation < 0.010) {
        deviation = 0.010;
    }
    rto = estRtt + 4 * deviation;
    senderBase = 0;
    nextSeq = 0;
    nextToSend = 0;
    timeouts = 0;
    fastRetx = 0;
    dupAcks = 0;
    checksum = 0;
    ackBytes = 0;
    firstDataTime = 0;
    lastAckTime = 0;
    timerExpire = 0;
    recvWnd = reply.recvWnd;
    effectiveWindow = recvWnd;
    if (effectiveWindow > (DWORD)windowSize) {
        effectiveWindow = windowSize;
    }
    lastReleased = effectiveWindow;
    errorCode = STATUS_OK;
    pending = new (nothrow) Packet[windowSize];
    eventQuit = CreateEvent(NULL, TRUE, FALSE, NULL);
    eventDrained = CreateEvent(NULL, TRUE, TRUE, NULL);
    socketReady = CreateEvent(NULL, FALSE, FALSE, NULL);
    empty = CreateSemaphore(NULL, 0, windowSize, NULL);
    full = CreateSemaphore(NULL, 0, windowSize, NULL);
    if (pending == NULL || eventQuit == NULL || eventDrained == NULL || socketReady == NULL || empty == NULL || full == NULL) {
        printf("Main: cannot allocate sender resources\n");
        cleanUp();
        return FAILED_SEND;
    }
    if (effectiveWindow > 0) {
        ReleaseSemaphore(empty, effectiveWindow, NULL);
    }
    if (WSAEventSelect(sock, socketReady, FD_READ) == SOCKET_ERROR) {
        printf("Main: WSAEventSelect failed with %d\n", WSAGetLastError());
        cleanUp();
        return FAILED_RECV;
    }
    workerThread = CreateThread(NULL, 0, workerStart, this, 0, NULL);
    if (workerThread == NULL) {
        cleanUp();
        return FAILED_SEND;
    }
    statsThread = CreateThread(NULL, 0, statsStart, this, 0, NULL);
    if (statsThread == NULL) {
        cleanUp();
        return FAILED_SEND;
    }
    connected = true;
    return STATUS_OK;
}
int SenderSocket::Send(char* buffer, int bytes) {
    if (!connected) {
        return NOT_CONNECTED;
    }
    if (bytes < 1 || bytes > MAX_PKT_SIZE - (int)sizeof(SenderDataHeader)) {
        return FAILED_SEND;
    }
    HANDLE events[] = { eventQuit, empty };
    DWORD result = WaitForMultipleObjects(2, events, FALSE, INFINITE);
    EnterCriticalSection(&lock);
    if (result != WAIT_OBJECT_0 + 1 || errorCode != STATUS_OK) {
        int status = errorCode;
        if (status == STATUS_OK) {
            status = FAILED_SEND;
        }
        LeaveCriticalSection(&lock);
        return status;
    }
    Packet* packet = pending + nextSeq % windowSize;
    SenderDataHeader header;
    header.seq = nextSeq;
    memcpy(packet->pkt, &header, sizeof(header));
    memcpy(packet->pkt + sizeof(header), buffer, bytes);
    packet->size = sizeof(header) + bytes;
    packet->attempts = 0;
    packet->txTime = 0;
    ResetEvent(eventDrained);
    nextSeq++;
    LeaveCriticalSection(&lock);
    ReleaseSemaphore(full, 1, NULL);
    return STATUS_OK;
}
int SenderSocket::sendPacket(DWORD seq) {
    Packet* packet = pending + seq % windowSize;
    if (packet->attempts >= DATA_ATTEMPTS) {
        return TIMEOUT;
    }
    double sendTime = 0;
    while (true) {
        if (WaitForSingleObject(eventQuit, 0) == WAIT_OBJECT_0) {
            return NOT_CONNECTED;
        }
        sendTime = getTime();
        int bytes = sendto(sock, packet->pkt, packet->size, 0, (sockaddr*)&server, sizeof(server));
        if (bytes != SOCKET_ERROR) {
            break;
        }
        int error = WSAGetLastError();
        if (error != WSAEWOULDBLOCK) {
            printf("[%7.3f] --> failed sendto with %d\n", getTime() - startTime, error);
            return FAILED_SEND;
        }
        fd_set writeSet;
        FD_ZERO(&writeSet);
        FD_SET(sock, &writeSet);
        timeval waitTime = { 0, 10000 };
        if (select(0, NULL, &writeSet, NULL, &waitTime) == SOCKET_ERROR) {
            printf("Main: send wait failed with %d\n", WSAGetLastError());
            return FAILED_SEND;
        }
    }
    EnterCriticalSection(&lock);
    bool retransmission = packet->attempts > 0;
    packet->attempts++;
    packet->txTime = sendTime;
    if (firstDataTime == 0) {
        firstDataTime = sendTime;
    }
    if (retransmission || seq == senderBase) {
        timerExpire = sendTime + rto;
    }
    LeaveCriticalSection(&lock);
    return STATUS_OK;
}
int SenderSocket::receiveAck() {
    ReceiverHeader reply;
    sockaddr_in fromAddr;
    int addrSize = sizeof(fromAddr);
    int bytes = recvfrom(sock, (char*)&reply, sizeof(reply), 0, (sockaddr*)&fromAddr, &addrSize);
    double recvTime = getTime();
    if (bytes == SOCKET_ERROR) {
        int error = WSAGetLastError();
        if (error == WSAEWOULDBLOCK) {
            return STATUS_OK;
        }
        printf("[%7.3f] <-- failed recvfrom with %d\n", getTime() - startTime, error);
        return FAILED_RECV;
    }
    if (bytes != (int)sizeof(reply)) {
        return STATUS_OK;
    }
    if (fromAddr.sin_addr.s_addr != server.sin_addr.s_addr || fromAddr.sin_port != server.sin_port) {
        return STATUS_OK;
    }
    if (reply.flags.magic != MAGIC_PROTOCOL || reply.flags.reserved != 0 || !reply.flags.ACK || reply.flags.SYN || reply.flags.FIN) {
        return STATUS_OK;
    }
    bool retransmit = false;
    DWORD seq = 0;
    EnterCriticalSection(&lock);
    if (reply.ackSeq < senderBase || reply.ackSeq > nextToSend) {
        LeaveCriticalSection(&lock);
        return STATUS_OK;
    }
    recvWnd = reply.recvWnd;
    effectiveWindow = recvWnd;
    if (effectiveWindow > (DWORD)windowSize) {
        effectiveWindow = windowSize;
    }
    if (reply.ackSeq > senderBase) {
        DWORD oldBase = senderBase;
        Packet* first = pending + oldBase % windowSize;
        Packet* last = pending + (reply.ackSeq - 1) % windowSize;
        if (first->attempts == 1 && last->attempts == 1) {
            updateRtt(recvTime - last->txTime);
        }
        for (DWORD i = oldBase; i < reply.ackSeq; i++) {
            ackBytes += pending[i % windowSize].size - sizeof(SenderDataHeader);
        }
        senderBase = reply.ackSeq;
        lastAckTime = recvTime;
        dupAcks = 0;
        timerExpire = recvTime + rto;
        if (senderBase == nextSeq) {
            SetEvent(eventDrained);
        }
    }
    else if (senderBase < nextToSend) {
        dupAcks++;
        if (dupAcks == 3) {
            retransmit = true;
            seq = senderBase;
        }
    }
    DWORD newEdge = senderBase + effectiveWindow;
    if (newEdge > lastReleased) {
        DWORD newSlots = newEdge - lastReleased;
        if (!ReleaseSemaphore(empty, newSlots, NULL)) {
            LeaveCriticalSection(&lock);
            return FAILED_RECV;
        }
        lastReleased = newEdge;
    }
    LeaveCriticalSection(&lock);
    if (retransmit) {
        int status = sendPacket(seq);
        if (status != STATUS_OK) {
            return status;
        }
        EnterCriticalSection(&lock);
        fastRetx++;
        LeaveCriticalSection(&lock);
    }
    return STATUS_OK;
}
DWORD WINAPI SenderSocket::workerStart(void* data) {
    SenderSocket* sender = (SenderSocket*)data;
    sender->workerRun();
    return 0;
}
DWORD WINAPI SenderSocket::statsStart(void* data) {
    SenderSocket* sender = (SenderSocket*)data;
    sender->statsRun();
    return 0;
}
void SenderSocket::workerRun() {
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
    HANDLE events[] = { eventQuit, socketReady, full };
    while (true) {
        if (WaitForSingleObject(eventQuit, 0) == WAIT_OBJECT_0) {
            return;
        }
        EnterCriticalSection(&lock);
        bool expired = senderBase < nextToSend && getTime() >= timerExpire;
        DWORD seq = senderBase;
        DWORD waitTime = INFINITE;
        if (senderBase < nextToSend) {
            double remaining = timerExpire - getTime();
            if (remaining < 0) {
                remaining = 0;
            }
            waitTime = (DWORD)ceil(remaining * 1000);
        }
        bool canSend = nextToSend < senderBase + effectiveWindow;
        LeaveCriticalSection(&lock);
        if (expired) {
            int status = sendPacket(seq);
            if (status != STATUS_OK) {
                fail(status);
                return;
            }
            EnterCriticalSection(&lock);
            timeouts++;
            LeaveCriticalSection(&lock);
            continue;
        }
        DWORD eventCount = 2;
        if (canSend) {
            eventCount = 3;
        }
        DWORD result = WaitForMultipleObjects(eventCount, events, FALSE, waitTime);
        if (result == WAIT_OBJECT_0) {
            return;
        }
        if (result == WAIT_TIMEOUT) {
            continue;
        }
        if (result == WAIT_OBJECT_0 + 1) {
            WSANETWORKEVENTS networkEvents;
            if (WSAEnumNetworkEvents(sock, NULL, &networkEvents) == SOCKET_ERROR) {
                printf("Main: WSAEnumNetworkEvents failed with %d\n", WSAGetLastError());
                fail(FAILED_RECV);
                return;
            }
            if ((networkEvents.lNetworkEvents & FD_READ) && networkEvents.iErrorCode[FD_READ_BIT] != 0) {
                printf("Main: socket receive event failed with %d\n", networkEvents.iErrorCode[FD_READ_BIT]);
                fail(FAILED_RECV);
                return;
            }
            int status = receiveAck();
            if (status != STATUS_OK) {
                fail(status);
                return;
            }
        }
        else if (result == WAIT_OBJECT_0 + 2) {
            EnterCriticalSection(&lock);
            seq = nextToSend;
            LeaveCriticalSection(&lock);
            int status = sendPacket(seq);
            if (status != STATUS_OK) {
                fail(status);
                return;
            }
            EnterCriticalSection(&lock);
            nextToSend++;
            LeaveCriticalSection(&lock);
        }
        else {
            printf("Main: worker wait failed with %lu\n", GetLastError());
            fail(FAILED_RECV);
            return;
        }
    }
}
void SenderSocket::statsRun() {
    double lastTime = startTime;
    UINT64 lastBytes = 0;
    double firstWait = startTime + 2 - getTime();
    DWORD waitTime = 0;
    if (firstWait > 0) {
        waitTime = (DWORD)ceil(firstWait * 1000);
    }
    while (WaitForSingleObject(eventQuit, waitTime) == WAIT_TIMEOUT) {
        double now = getTime();
        EnterCriticalSection(&lock);
        DWORD base = senderBase;
        DWORD next = nextSeq;
        DWORD totalTimeouts = timeouts;
        DWORD totalFast = fastRetx;
        DWORD win = effectiveWindow;
        UINT64 totalBytes = ackBytes;
        double rtt = estRtt;
        LeaveCriticalSection(&lock);
        double speed = (double)(totalBytes - lastBytes) * 8 / (now - lastTime) / 1000000;
        printf("[%3.0f] B %8lu (%6.1f MB) N %8lu T %lu F %lu W %lu S %.3f Mbps RTT %.3f\n", now - startTime, base, ((double)totalBytes + (double)base * sizeof(SenderDataHeader)) / 1000000, next, totalTimeouts, totalFast, win, speed, rtt);
        fflush(stdout);
        lastBytes = totalBytes;
        lastTime = now;
        waitTime = 2000;
    }
}
int SenderSocket::Close(double* elapsedTime) {
    if (!connected) {
        return NOT_CONNECTED;
    }
    HANDLE events[] = { eventQuit, eventDrained };
    DWORD result = WaitForMultipleObjects(2, events, FALSE, INFINITE);
    EnterCriticalSection(&lock);
    int status = errorCode;
    if (result != WAIT_OBJECT_0 + 1 && status == STATUS_OK) {
        status = FAILED_RECV;
    }
    *elapsedTime = 0;
    if (firstDataTime != 0) {
        *elapsedTime = lastAckTime - firstDataTime;
    }
    DWORD finalSeq = nextSeq;
    LeaveCriticalSection(&lock);
    stopThreads();
    if (status != STATUS_OK) {
        cleanUp();
        return status;
    }
    if (WSAEventSelect(sock, NULL, 0) == SOCKET_ERROR) {
        cleanUp();
        return FAILED_RECV;
    }
    u_long mode = 0;
    if (ioctlsocket(sock, FIONBIO, &mode) == SOCKET_ERROR) {
        cleanUp();
        return FAILED_RECV;
    }
    SenderDataHeader finPacket;
    finPacket.flags.FIN = 1;
    finPacket.seq = finalSeq;
    ReceiverHeader reply;
    status = TIMEOUT;
    for (int attempt = 1; attempt <= FIN_ATTEMPTS; attempt++) {
        double sendTime = getTime();
        int bytes = sendto(sock, (char*)&finPacket, sizeof(finPacket), 0, (sockaddr*)&server, sizeof(server));
        if (bytes == SOCKET_ERROR) {
            int error = WSAGetLastError();
            printf("[%7.3f]\t--> failed sendto with %d\n", getTime() - startTime, error);
            status = FAILED_SEND;
            break;
        }
        status = waitForAck(false, sendTime, &reply);
        if (status == STATUS_OK) {
            break;
        }
        if (status != TIMEOUT) {
            break;
        }
    }
    if (status == STATUS_OK) {
        checksum = reply.recvWnd;
        printf("[%7.3f]\t<-- FIN-ACK %lu window %lX\n", getTime() - startTime, reply.ackSeq, checksum);
    }
    cleanUp();
    return status;
}
double SenderSocket::GetRTT() {
    return estRtt;
}
DWORD SenderSocket::GetChecksum() {
    return checksum;
}
int SenderSocket::waitForAck(bool isSyn, double sendTime, ReceiverHeader* reply) {
    DWORD seq = 0;
    if (!isSyn) {
        seq = nextSeq;
    }
    double deadline = sendTime + rto;
    while (true) {
        double remainingTime = deadline - getTime();
        if (remainingTime <= 0) {
            return TIMEOUT;
        }
        timeval waitTime;
        waitTime.tv_sec = (long)remainingTime;
        waitTime.tv_usec = (long)((remainingTime - waitTime.tv_sec) * 1000000);
        fd_set readSet;
        FD_ZERO(&readSet);
        FD_SET(sock, &readSet);
        int result = select(0, &readSet, NULL, NULL, &waitTime);
        if (result == SOCKET_ERROR) {
            int error = WSAGetLastError();
            printf("[%7.3f]\t<-- failed select with %d\n", getTime() - startTime, error);
            return FAILED_RECV;
        }
        if (result == 0) {
            continue;
        }
        sockaddr_in fromAddr;
        int addrSize = sizeof(fromAddr);
        int bytes = recvfrom(sock, (char*)reply, sizeof(*reply), 0, (sockaddr*)&fromAddr, &addrSize);
        if (bytes == SOCKET_ERROR) {
            int error = WSAGetLastError();
            printf("[%7.3f]\t<-- failed recvfrom with %d\n", getTime() - startTime, error);
            return FAILED_RECV;
        }
        if (bytes != (int)sizeof(*reply)) {
            continue;
        }
        if (fromAddr.sin_family != AF_INET || fromAddr.sin_addr.s_addr != server.sin_addr.s_addr || fromAddr.sin_port != server.sin_port) {
            continue;
        }
        if (reply->flags.magic != MAGIC_PROTOCOL || reply->flags.reserved != 0 || reply->flags.ACK != 1 || reply->ackSeq != seq) {
            continue;
        }
        if (reply->flags.SYN != (DWORD)isSyn || reply->flags.FIN != (DWORD)!isSyn) {
            continue;
        }
        return STATUS_OK;
    }
}