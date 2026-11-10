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
    mode = 0;
    workerCount = 1;
    sendOwner = -1;
    connected = false;
    fastRecovery = false;
    retransmitPending = false;
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
    cwndLimitedUntil = 0;
    startTime = getTime();
    estRtt = 0;
    devRtt = 0;
    rto = 1.0;
    timerExpire = 0;
    firstDataTime = 0;
    lastAckTime = 0;
    cwnd = 1;
    ssthresh = 2;
    ackCredit = 0;
    eventQuit = NULL;
    eventDrained = NULL;
    stateChanged = NULL;
    socketReady = NULL;
    empty = NULL;
    full = NULL;
    statsThread = NULL;
    for (int i = 0; i < 2; i++) {
        workerSent[i] = 0;
        sendReady[i] = NULL;
        workerThread[i] = NULL;
    }
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
    for (int i = 0; i < 2; i++) {
        if (workerThread[i] != NULL) {
            WaitForSingleObject(workerThread[i], INFINITE);
            CloseHandle(workerThread[i]);
            workerThread[i] = NULL;
        }
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
    if (stateChanged != NULL) {
        CloseHandle(stateChanged);
    }
    if (empty != NULL) {
        CloseHandle(empty);
    }
    if (full != NULL) {
        CloseHandle(full);
    }
    for (int i = 0; i < 2; i++) {
        if (sendReady[i] != NULL) {
            CloseHandle(sendReady[i]);
        }
        sendReady[i] = NULL;
    }
    eventQuit = NULL;
    eventDrained = NULL;
    socketReady = NULL;
    stateChanged = NULL;
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
int SenderSocket::Open(char* targetHost, int port, int senderWindow, LinkProperties* lp, int extraMode) {
    if (connected) {
        return ALREADY_CONNECTED;
    }
    cleanUp();
    mode = extraMode;
    workerCount = 1;
    sendOwner = -1;
    if (mode != 0) {
        workerCount = 2;
    }
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
    cwndLimitedUntil = 0;
    firstDataTime = 0;
    lastAckTime = 0;
    timerExpire = 0;
    cwnd = 1;
    ssthresh = windowSize;
    ackCredit = 0;
    fastRecovery = false;
    retransmitPending = false;
    workerSent[0] = 0;
    workerSent[1] = 0;
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
    stateChanged = CreateEvent(NULL, FALSE, FALSE, NULL);
    sendReady[0] = CreateEvent(NULL, TRUE, FALSE, NULL);
    sendReady[1] = CreateEvent(NULL, TRUE, FALSE, NULL);
    empty = CreateSemaphore(NULL, 0, windowSize, NULL);
    full = CreateSemaphore(NULL, 0, windowSize, NULL);
    if (pending == NULL || eventQuit == NULL || eventDrained == NULL || socketReady == NULL || stateChanged == NULL || sendReady[0] == NULL || sendReady[1] == NULL || empty == NULL || full == NULL) {
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
    workerThread[0] = CreateThread(NULL, 0, workerStart, this, 0, NULL);
    if (workerThread[0] == NULL) {
        cleanUp();
        return FAILED_SEND;
    }
    if (workerCount == 2) {
        workerThread[1] = CreateThread(NULL, 0, secondWorkerStart, this, 0, NULL);
        if (workerThread[1] == NULL) {
            cleanUp();
            return FAILED_SEND;
        }
    }
    statsThread = CreateThread(NULL, 0, statsStart, this, 0, NULL);
    if (statsThread == NULL) {
        cleanUp();
        return FAILED_SEND;
    }
    connected = true;
    return STATUS_OK;
}
DWORD SenderSocket::getSendWindow() {
    DWORD limit = effectiveWindow;
    if (mode == 2) {
        DWORD congestionWindow = (DWORD)cwnd;
        if (congestionWindow < 1) {
            congestionWindow = 1;
        }
        if (limit > congestionWindow) {
            limit = congestionWindow;
        }
    }
    return limit;
}
void SenderSocket::updateSendEvents() {
    int owner = -1;
    if (!retransmitPending && nextToSend < nextSeq && nextToSend - senderBase < getSendWindow()) {
        owner = (nextToSend / WORKER_BATCH) % workerCount;
    }
    if (owner != sendOwner) {
        if (sendOwner >= 0) {
            ResetEvent(sendReady[sendOwner]);
        }
        if (owner >= 0) {
            SetEvent(sendReady[owner]);
        }
        sendOwner = owner;
    }
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
    packet->timedOut = false;
    ResetEvent(eventDrained);
    nextSeq++;
    ReleaseSemaphore(full, 1, NULL);
    updateSendEvents();
    LeaveCriticalSection(&lock);
    return STATUS_OK;
}
int SenderSocket::sendPacket(DWORD seq, int worker, bool newPacket) {
    while (true) {
        if (WaitForSingleObject(eventQuit, 0) == WAIT_OBJECT_0) {
            return NOT_CONNECTED;
        }
        EnterCriticalSection(&lock);
        if (newPacket) {
            DWORD owner = (nextToSend / WORKER_BATCH) % workerCount;
            if (seq != nextToSend || seq >= nextSeq || owner != (DWORD)worker || retransmitPending || nextToSend - senderBase >= getSendWindow()) {
                updateSendEvents();
                LeaveCriticalSection(&lock);
                return STATUS_OK;
            }
        }
        Packet* packet = pending + seq % windowSize;
        if (packet->attempts >= DATA_ATTEMPTS) {
            LeaveCriticalSection(&lock);
            return TIMEOUT;
        }
        double sendTime = getTime();
        int bytes = sendto(sock, packet->pkt, packet->size, 0, (sockaddr*)&server, sizeof(server));
        if (bytes != SOCKET_ERROR) {
            if (newPacket && WaitForSingleObject(full, 0) != WAIT_OBJECT_0) {
                LeaveCriticalSection(&lock);
                return FAILED_SEND;
            }
            bool wakeTimer = worker == 1 && newPacket && seq == senderBase;
            packet->attempts++;
            packet->txTime = sendTime;
            if (firstDataTime == 0) {
                firstDataTime = sendTime;
            }
            if (!newPacket || seq == senderBase) {
                timerExpire = sendTime + rto;
            }
            if (newPacket) {
                nextToSend++;
                workerSent[worker]++;
            } else {
                retransmitPending = false;
            }
            updateSendEvents();
            LeaveCriticalSection(&lock);
            if (wakeTimer) {
                SetEvent(stateChanged);
            }
            return STATUS_OK;
        }
        int error = WSAGetLastError();
        LeaveCriticalSection(&lock);
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
    double lossWindow = 0;
    double lossThreshold = 0;
    EnterCriticalSection(&lock);
    if (reply.ackSeq < senderBase || reply.ackSeq > nextToSend) {
        LeaveCriticalSection(&lock);
        return STATUS_OK;
    }
    DWORD flight = nextToSend - senderBase;
    if (mode == 2 && flight >= (DWORD)cwnd) {
        cwndLimitedUntil = nextToSend;
    }
    recvWnd = reply.recvWnd;
    effectiveWindow = recvWnd;
    if (effectiveWindow > (DWORD)windowSize) {
        effectiveWindow = windowSize;
    }
    if (reply.ackSeq > senderBase) {
        DWORD oldBase = senderBase;
        DWORD newlyAcked = reply.ackSeq - oldBase;
        Packet* first = pending + oldBase % windowSize;
        Packet* last = pending + (reply.ackSeq - 1) % windowSize;
        if (first->attempts == 1 && last->attempts == 1) {
            updateRtt(recvTime - last->txTime);
        }
        for (DWORD i = oldBase; i < reply.ackSeq; i++) {
            ackBytes += pending[i % windowSize].size - sizeof(SenderDataHeader);
        }
        if (mode == 2) {
            if (fastRecovery) {
                cwnd = ssthresh;
                fastRecovery = false;
                ackCredit = 0;
            } else if (senderBase < cwndLimitedUntil) {
                if (cwnd < ssthresh) {
                    cwnd++;
                } else {
                    DWORD limitedPackets = cwndLimitedUntil - senderBase;
                    if (newlyAcked > limitedPackets) {
                        newlyAcked = limitedPackets;
                    }
                    ackCredit += newlyAcked;
                    if (ackCredit >= cwnd) {
                        cwnd++;
                        ackCredit = 0;
                    }
                }
            }
        }
        senderBase = reply.ackSeq;
        lastAckTime = recvTime;
        dupAcks = 0;
        timerExpire = recvTime + rto;
        if (senderBase == nextSeq) {
            SetEvent(eventDrained);
        }
    } else if (senderBase < nextToSend) {
        dupAcks++;
        if (dupAcks == 3) {
            retransmit = true;
            retransmitPending = true;
            seq = senderBase;
            if (mode == 2) {
                ssthresh = (double)flight / 2;
                if (ssthresh < 2) {
                    ssthresh = 2;
                }
                cwnd = ssthresh + 3;
                cwndLimitedUntil = senderBase;
                ackCredit = 0;
                fastRecovery = true;
                lossThreshold = ssthresh;
            }
        } else if (mode == 2 && fastRecovery && dupAcks > 3) {
            cwnd++;
        }
    }
    if (cwnd > windowSize) {
        cwnd = windowSize;
    }
    lossWindow = cwnd;
    DWORD newEdge = senderBase + effectiveWindow;
    if (newEdge > lastReleased) {
        DWORD newSlots = newEdge - lastReleased;
        if (!ReleaseSemaphore(empty, newSlots, NULL)) {
            LeaveCriticalSection(&lock);
            return FAILED_RECV;
        }
        lastReleased = newEdge;
    }
    updateSendEvents();
    LeaveCriticalSection(&lock);
    if (retransmit) {
        if (mode == 2) {
            printf("EC: fast recovery at %lu, cwnd %.2f, ssthresh %.2f\n", seq, lossWindow, lossThreshold);
        }
        int status = sendPacket(seq, 0, false);
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
    sender->workerRun(0);
    return 0;
}
DWORD WINAPI SenderSocket::secondWorkerStart(void* data) {
    SenderSocket* sender = (SenderSocket*)data;
    sender->workerRun(1);
    return 0;
}
DWORD WINAPI SenderSocket::statsStart(void* data) {
    SenderSocket* sender = (SenderSocket*)data;
    sender->statsRun();
    return 0;
}
void SenderSocket::workerRun(int worker) {
    SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_TIME_CRITICAL);
    HANDLE events[4];
    events[0] = eventQuit;
    DWORD eventCount = 2;
    if (worker == 0) {
        events[1] = socketReady;
        events[2] = stateChanged;
        events[3] = sendReady[0];
        eventCount = 4;
    }
    else {
        events[1] = sendReady[1];
    }
    while (true) {
        if (WaitForSingleObject(eventQuit, 0) == WAIT_OBJECT_0) {
            return;
        }
        DWORD waitTime = INFINITE;
        DWORD seq = 0;
        bool expired = false;
        double oldWindow = 0;
        double threshold = 0;
        if (worker == 0) {
            EnterCriticalSection(&lock);
            seq = senderBase;
            if (senderBase < nextToSend) {
                double remaining = timerExpire - getTime();
                if (remaining <= 0) {
                    expired = true;
                    retransmitPending = true;
                    if (mode == 2) {
                        oldWindow = cwnd;
                        Packet* packet = pending + seq % windowSize;
                        if (!packet->timedOut) {
                            ssthresh = (double)(nextToSend - senderBase) / 2;
                            if (ssthresh < 2) {
                                ssthresh = 2;
                            }
                        }
                        packet->timedOut = true;
                        cwnd = 1;
                        cwndLimitedUntil = senderBase;
                        ackCredit = 0;
                        fastRecovery = false;
                        threshold = ssthresh;
                    }
                    updateSendEvents();
                } else {
                    waitTime = (DWORD)ceil(remaining * 1000);
                }
            }
            LeaveCriticalSection(&lock);
        }
        if (expired) {
            if (mode == 2) {
                printf("EC: timeout at %lu, cwnd %.2f -> 1.00, ssthresh %.2f\n", seq, oldWindow, threshold);
            }
            int status = sendPacket(seq, 0, false);
            if (status != STATUS_OK) {
                fail(status);
                return;
            }
            EnterCriticalSection(&lock);
            timeouts++;
            LeaveCriticalSection(&lock);
            continue;
        }
        DWORD result = WaitForMultipleObjects(eventCount, events, FALSE, waitTime);
        if (result == WAIT_OBJECT_0) {
            return;
        }
        if (result == WAIT_TIMEOUT) {
            continue;
        }
        if (worker == 0 && result == WAIT_OBJECT_0 + 1) {
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
        } else if (worker == 0 && result == WAIT_OBJECT_0 + 2) {
            continue;
        } else if ((worker == 0 && result == WAIT_OBJECT_0 + 3) || (worker == 1 && result == WAIT_OBJECT_0 + 1)) {
            EnterCriticalSection(&lock);
            seq = nextToSend;
            LeaveCriticalSection(&lock);
            int status = sendPacket(seq, worker, true);
            if (status != STATUS_OK) {
                fail(status);
                return;
            }
        } else {
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
        double congestionWindow = cwnd;
        double threshold = ssthresh;
        DWORD firstWorker = workerSent[0];
        DWORD secondWorker = workerSent[1];
        LeaveCriticalSection(&lock);
        double speed = (double)(totalBytes - lastBytes) * 8 / (now - lastTime) / 1000000;
        printf("[%3.0f] B %8lu (%6.1f MB) N %8lu T %lu F %lu W %lu S %.3f Mbps RTT %.3f\n", now - startTime, base, ((double)totalBytes + (double)base * sizeof(SenderDataHeader)) / 1000000, next, totalTimeouts, totalFast, win, speed, rtt);
        if (mode != 0) {
            printf("      EC: worker packets %lu / %lu", firstWorker, secondWorker);
            if (mode == 2) {
                printf(", cwnd %.2f, ssthresh %.2f", congestionWindow, threshold);
            }
            printf("\n");
        }
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
    u_long blockingMode = 0;
    if (ioctlsocket(sock, FIONBIO, &blockingMode) == SOCKET_ERROR) {
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
    if (mode != 0 && status == STATUS_OK) {
        printf("EC: final worker packets %lu / %lu\n", workerSent[0], workerSent[1]);
        if (mode == 2) {
            printf("EC: final cwnd %.2f, ssthresh %.2f\n", cwnd, ssthresh);
        }
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