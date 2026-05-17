/*
 * Amalgame Standard Library — Amalgame.Messaging.NATS.JetStream
 * Copyright (c) 2026 Bastien MOUGET
 * https://github.com/amalgame-lang/Amalgame
 *
 * NATS JetStream client speaking the wire protocol directly over
 * TCP. JetStream is NATS's persistence layer (streams + durable
 * consumers + at-least-once delivery); it talks to a NATS server
 * with `-js` enabled by exchanging JSON requests on special
 * subjects ($JS.API.STREAM.*, $JS.API.CONSUMER.*) via the standard
 * NATS Core PUB/SUB protocol.
 *
 * We re-implement the minimum NATS Core wire helpers needed here
 * (Connect / handshake / Publish / Subscribe / WaitMessage / inbox)
 * instead of pulling amalgame-messaging-nats as a dependency, so
 * a project can take JetStream alone.
 *
 * Surface (v0.1):
 *   ── Lifecycle ──
 *   Open(host, port)                  -> AmalgameJetStream*
 *   Close(js)
 *   IsOpen(js)                        -> bool
 *   LastError(js)                     -> string
 *
 *   ── Stream management ──
 *   StreamCreate(js, name, subjects)  -> bool  (file storage)
 *   StreamDelete(js, name)            -> bool
 *   StreamInfo(js, name)              -> string  (raw JSON response)
 *
 *   ── Publishing ──
 *   Publish(js, subject, payload)     -> bool  (blocks until PubAck)
 *   LastSeq(js)                       -> int   (sequence from last PubAck)
 *
 *   ── Pull consumer (durable, AckExplicit) ──
 *   ConsumerCreate(js, stream, name)  -> bool
 *   ConsumerPull(js, stream, name,
 *                batch, timeoutMs)    -> int   (msg count)
 *   MessageSubject(js, idx)           -> string
 *   MessageData(js, idx)              -> string
 *   MessageReply(js, idx)             -> string  (ack inbox)
 *   AckAll(js)                                 (ACK every pulled msg)
 *
 * Wire vocabulary: NATS Core protocol (PUB / SUB / MSG / +OK /
 * -ERR / PING / PONG). All control lines CRLF-terminated. MSG
 * payloads are byte-counted, no in-band terminator.
 *
 *   PUB $JS.API.STREAM.CREATE.foo _INBOX.<x>.<n> <body-len>\r\n
 *   <json body>\r\n
 *   ↑                                                          ↓
 *   ... server processes, replies on _INBOX.<x>.<n> ...        ↓
 *   MSG _INBOX.<x>.<n> 1 <ack-len>\r\n                        ↓
 *   <ack json>\r\n                                            ↓
 *
 * Reuses the cross-platform socket layer from Amalgame_Net.h.
 *
 * Threading: single-owner handle. Concurrent operations on the
 * same handle interleave bytes on the wire and corrupt the
 * protocol. Distinct handles (one per thread) are safe.
 *
 * Deferred to v0.2+: KV / ObjectStore / push consumers /
 * AckNak / AckTerm / JS domain prefix / TLS / AUTH.
 */

#ifndef AMALGAME_MESSAGING_NATS_JETSTREAM_H
#define AMALGAME_MESSAGING_NATS_JETSTREAM_H

#include "_runtime.h"
#include "Amalgame_Collections.h"
#include "Amalgame_Net.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef _WIN32
#  include <sys/time.h>
#  include <unistd.h>
#endif

#ifndef AMALGAME_JS_RECV_BUF
#  define AMALGAME_JS_RECV_BUF 8192
#endif

#ifndef AMALGAME_JS_MAX_PULL
#  define AMALGAME_JS_MAX_PULL 256
#endif

typedef struct {
    char* subject;       /* GC_MALLOC'd */
    char* reply;         /* ack inbox; GC_MALLOC'd */
    char* data;          /* payload as C string; GC_MALLOC'd */
} AmalgameJSMsg;

typedef struct AmalgameJetStream {
    int   fd;                  /* socket fd; -1 = closed */
    char* last_error;          /* GC-strdup'd, or NULL */
    char* inbox_prefix;        /* "_INBOX.<rand-8hex>." per-handle */
    int   inbox_seq;           /* monotonic, appended to inbox_prefix */
    i64   last_seq;            /* sequence from last PubAck */

    unsigned char* rxbuf;      /* GC_MALLOC; doubles on demand */
    size_t         rxlen;
    size_t         rxcap;

    /* Pulled messages from the last ConsumerPull call. Cleared at
     * the start of each pull. Slots own GC strings so the AM side
     * can hand them out via MessageSubject/Data/Reply. */
    AmalgameJSMsg  pulled[AMALGAME_JS_MAX_PULL];
    int            pulled_n;
} AmalgameJetStream;

/* ── Small helpers ──────────────────────────────────── */

static inline code_string _amjs_err_dup(const char* msg) {
    if (!msg) return NULL;
    size_t n = strlen(msg);
    char* p = (char*) code_alloc(n + 1);
    memcpy(p, msg, n + 1);
    return p;
}

static inline char* _amjs_str_dup(const char* s, size_t n) {
    char* p = (char*) code_alloc(n + 1);
    if (n > 0) memcpy(p, s, n);
    p[n] = '\0';
    return p;
}

static inline int _amjs_send_all(int fd, const unsigned char* buf, size_t n) {
    size_t off = 0;
    while (off < n) {
        ssize_t k = send(fd, (const char*) buf + off, n - off, 0);
        if (k <= 0) return -1;
        off += (size_t) k;
    }
    return 0;
}

static inline void _amjs_set_rcvtimeo(int fd, i64 timeout_ms) {
#ifdef _WIN32
    DWORD tv = (timeout_ms > 0) ? (DWORD) timeout_ms : 0;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*) &tv, sizeof(tv));
#else
    struct timeval tv;
    if (timeout_ms > 0) {
        tv.tv_sec  = (time_t) (timeout_ms / 1000);
        tv.tv_usec = (suseconds_t) ((timeout_ms % 1000) * 1000);
    } else {
        tv.tv_sec  = 0;
        tv.tv_usec = 0;
    }
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
#endif
}

/* Append `n` bytes of raw socket data to the rxbuf, growing if
 * needed. Capacity doubles on demand and is tracked explicitly. */
static inline int _amjs_recv_more(AmalgameJetStream* js) {
    if (!js->rxbuf) {
        js->rxbuf = (unsigned char*) GC_MALLOC(AMALGAME_JS_RECV_BUF);
        js->rxcap = AMALGAME_JS_RECV_BUF;
        js->rxlen = 0;
    }
    unsigned char chunk[AMALGAME_JS_RECV_BUF];
    ssize_t k = recv(js->fd, (char*) chunk, AMALGAME_JS_RECV_BUF, 0);
    if (k <= 0) return -1;
    size_t need = js->rxlen + (size_t) k;
    if (need > js->rxcap) {
        size_t newcap = js->rxcap;
        while (newcap < need) newcap *= 2;
        unsigned char* p = (unsigned char*) GC_MALLOC(newcap);
        memcpy(p, js->rxbuf, js->rxlen);
        js->rxbuf = p;
        js->rxcap = newcap;
    }
    memcpy(js->rxbuf + js->rxlen, chunk, (size_t) k);
    js->rxlen += (size_t) k;
    return 0;
}

static inline void _amjs_consume(AmalgameJetStream* js, size_t n) {
    if (n >= js->rxlen) { js->rxlen = 0; return; }
    memmove(js->rxbuf, js->rxbuf + n, js->rxlen - n);
    js->rxlen -= n;
}

static inline ssize_t _amjs_find_crlf(AmalgameJetStream* js) {
    if (js->rxlen < 2) return -1;
    for (size_t i = 0; i + 1 < js->rxlen; i++) {
        if (js->rxbuf[i] == '\r' && js->rxbuf[i+1] == '\n') return (ssize_t) i;
    }
    return -1;
}

/* Read one full control line (excluding CRLF) into `out` (heap-
 * alloc'd, caller takes ownership). Returns 0/-1. */
static inline int _amjs_read_line(AmalgameJetStream* js, char** out, i64 timeout_ms) {
    _amjs_set_rcvtimeo(js->fd, timeout_ms);
    while (1) {
        ssize_t pos = _amjs_find_crlf(js);
        if (pos >= 0) {
            *out = _amjs_str_dup((const char*) js->rxbuf, (size_t) pos);
            _amjs_consume(js, (size_t) pos + 2);
            return 0;
        }
        if (_amjs_recv_more(js) < 0) return -1;
    }
}

/* Read exactly n raw bytes (for MSG payloads). */
static inline int _amjs_read_n(AmalgameJetStream* js, char** out, size_t n, i64 timeout_ms) {
    _amjs_set_rcvtimeo(js->fd, timeout_ms);
    while (js->rxlen < n) {
        if (_amjs_recv_more(js) < 0) return -1;
    }
    *out = _amjs_str_dup((const char*) js->rxbuf, n);
    _amjs_consume(js, n);
    return 0;
}

/* ── NATS Core wire handshake — minimal subset ───────── */

static inline AmalgameJetStream* _amjs_alloc(void) {
    AmalgameJetStream* js = (AmalgameJetStream*) GC_MALLOC(sizeof(AmalgameJetStream));
    js->fd           = -1;
    js->last_error   = NULL;
    js->inbox_prefix = NULL;
    js->inbox_seq    = 0;
    js->last_seq     = 0;
    js->rxbuf        = NULL;
    js->rxlen        = 0;
    js->rxcap        = 0;
    js->pulled_n     = 0;
    return js;
}

/* Build the per-handle inbox prefix once at Open time. NATS Core
 * sends every server-originated reply to whatever subject we put
 * in the PUB's reply-to field; making it unique to this handle
 * keeps cross-handle responses from leaking. */
static inline void _amjs_init_inbox(AmalgameJetStream* js) {
    /* 8 hex chars from clock() ^ getpid() — enough to avoid
     * collisions inside a process; cluster-uniqueness isn't
     * required since the inbox is scoped to this TCP conn. */
    unsigned int seed = (unsigned int) time(NULL) ^ (unsigned int) clock();
    char buf[64];
    snprintf(buf, sizeof(buf), "_INBOX.%08x.", seed);
    js->inbox_prefix = _amjs_str_dup(buf, strlen(buf));
}

/* Generate the next per-request inbox subject — `<prefix><seq>`. */
static inline char* _amjs_next_inbox(AmalgameJetStream* js) {
    js->inbox_seq++;
    char buf[96];
    snprintf(buf, sizeof(buf), "%s%d", js->inbox_prefix, js->inbox_seq);
    return _amjs_str_dup(buf, strlen(buf));
}

static inline AmalgameJetStream* Amalgame_Messaging_NATS_JetStream_Open(
        code_string host, i64 port) {
    AmalgameJetStream* js = _amjs_alloc();
    _amnet_init_once();
    if (!host || port <= 0 || port > 65535) {
        js->last_error = _amjs_err_dup("Open: invalid host/port");
        return js;
    }

    /* getaddrinfo + socket + connect — same pattern as
     * amalgame-messaging-nats so a project can swap one for the
     * other without surprises in the TCP layer. */
    struct addrinfo hints = {0};
    hints.ai_family   = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    char portStr[16];
    snprintf(portStr, sizeof(portStr), "%lld", (long long) port);
    struct addrinfo* res = NULL;
    if (getaddrinfo(host, portStr, &hints, &res) != 0 || !res) {
        js->last_error = _amjs_err_dup("Open: getaddrinfo failed");
        return js;
    }
    int fd = (int) socket(res->ai_family, res->ai_socktype, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        js->last_error = _amjs_err_dup("Open: socket() failed");
        return js;
    }
    if (connect(fd, res->ai_addr, (int) res->ai_addrlen) != 0) {
        _amnet_close_socket(fd);
        freeaddrinfo(res);
        js->last_error = _amjs_err_dup("Open: connect() failed");
        return js;
    }
    freeaddrinfo(res);
    js->fd = fd;
    _amjs_init_inbox(js);

    /* INFO from server is always first. */
    char* line = NULL;
    if (_amjs_read_line(js, &line, 5000) != 0) {
        _amnet_close_socket(fd);
        js->fd = -1;
        js->last_error = _amjs_err_dup("Open: no INFO from server");
        return js;
    }
    /* We don't parse INFO — just send CONNECT. */

    /* CONNECT — `verbose:false` means no per-PUB +OK echoing, which
     * keeps the recv loop simpler. */
    const char* connect_line =
        "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":false,"
        "\"lang\":\"amalgame\",\"version\":\"0.1.0\",\"name\":\"amalgame-js\"}\r\n";
    if (_amjs_send_all(fd, (const unsigned char*) connect_line,
                       strlen(connect_line)) < 0) {
        _amnet_close_socket(fd);
        js->fd = -1;
        js->last_error = _amjs_err_dup("Open: CONNECT write failed");
        return js;
    }

    /* PING / PONG round-trip confirms the server accepted CONNECT. */
    if (_amjs_send_all(fd, (const unsigned char*) "PING\r\n", 6) < 0) {
        _amnet_close_socket(fd);
        js->fd = -1;
        js->last_error = _amjs_err_dup("Open: PING write failed");
        return js;
    }
    int tries = 0;
    while (1) {
        if (_amjs_read_line(js, &line, 5000) != 0) {
            _amnet_close_socket(fd);
            js->fd = -1;
            js->last_error = _amjs_err_dup("Open: handshake never PONG'd");
            return js;
        }
        if (strcmp(line, "PONG") == 0) break;
        if (++tries > 4) {
            _amnet_close_socket(fd);
            js->fd = -1;
            js->last_error = _amjs_err_dup("Open: unexpected handshake reply");
            return js;
        }
    }

    return js;
}

static inline void Amalgame_Messaging_NATS_JetStream_Close(AmalgameJetStream* js) {
    if (!js || js->fd < 0) return;
    _amnet_close_socket(js->fd);
    js->fd = -1;
}

static inline code_bool Amalgame_Messaging_NATS_JetStream_IsOpen(AmalgameJetStream* js) {
    return (js && js->fd >= 0) ? 1 : 0;
}

static inline code_string Amalgame_Messaging_NATS_JetStream_LastError(AmalgameJetStream* js) {
    if (!js || !js->last_error) return (code_string) "";
    return js->last_error;
}

/* ── PUB + SUB primitives used by the JS API request layer ─── */

/* PUB <subject> [reply-to] <bytes>\r\n<payload>\r\n */
static inline int _amjs_pub(AmalgameJetStream* js,
                              const char* subject,
                              const char* reply_to,
                              const char* payload,
                              size_t payload_n) {
    char hdr[512];
    int hl;
    if (reply_to && *reply_to) {
        hl = snprintf(hdr, sizeof(hdr), "PUB %s %s %zu\r\n",
                       subject, reply_to, payload_n);
    } else {
        hl = snprintf(hdr, sizeof(hdr), "PUB %s %zu\r\n",
                       subject, payload_n);
    }
    if (hl <= 0 || (size_t) hl >= sizeof(hdr)) return -1;
    if (_amjs_send_all(js->fd, (const unsigned char*) hdr, (size_t) hl) < 0)
        return -1;
    if (payload_n > 0) {
        if (_amjs_send_all(js->fd, (const unsigned char*) payload, payload_n) < 0)
            return -1;
    }
    if (_amjs_send_all(js->fd, (const unsigned char*) "\r\n", 2) < 0) return -1;
    return 0;
}

/* SUB <subject> <sid>\r\n */
static inline int _amjs_sub(AmalgameJetStream* js, const char* subject, int sid) {
    char buf[256];
    int n = snprintf(buf, sizeof(buf), "SUB %s %d\r\n", subject, sid);
    if (n <= 0 || (size_t) n >= sizeof(buf)) return -1;
    return _amjs_send_all(js->fd, (const unsigned char*) buf, (size_t) n);
}

/* UNSUB <sid>\r\n */
static inline int _amjs_unsub(AmalgameJetStream* js, int sid) {
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "UNSUB %d\r\n", sid);
    if (n <= 0) return -1;
    return _amjs_send_all(js->fd, (const unsigned char*) buf, (size_t) n);
}

/* Parse a MSG control line. Forms:
 *     MSG <subject> <sid> <bytes>
 *     MSG <subject> <sid> <reply> <bytes>
 * On success, fills out_subject / out_reply (may be empty) /
 * out_bytes. Returns 0/-1. */
static inline int _amjs_parse_msg_hdr(const char* line, char** out_subject,
                                        char** out_reply, size_t* out_bytes) {
    if (strncmp(line, "MSG ", 4) != 0) return -1;
    const char* p = line + 4;
    /* subject */
    const char* sp1 = strchr(p, ' '); if (!sp1) return -1;
    *out_subject = _amjs_str_dup(p, (size_t)(sp1 - p));
    p = sp1 + 1;
    /* sid */
    const char* sp2 = strchr(p, ' '); if (!sp2) return -1;
    p = sp2 + 1;
    /* third token = either reply or bytes */
    const char* sp3 = strchr(p, ' ');
    if (sp3) {
        /* reply followed by bytes */
        *out_reply = _amjs_str_dup(p, (size_t)(sp3 - p));
        *out_bytes = (size_t) atoll(sp3 + 1);
    } else {
        *out_reply = _amjs_str_dup("", 0);
        *out_bytes = (size_t) atoll(p);
    }
    return 0;
}

/* Wait for one MSG arriving on the expected sid. Skips +OK,
 * PING (auto-PONG'd), other MSGs (queued in pulled[] for the
 * JS API caller to ignore — JS responses come in fast).
 * Returns 0 on success and fills the AmalgameJSMsg out; -1 on
 * timeout / disconnect. */
static inline int _amjs_recv_msg(AmalgameJetStream* js, AmalgameJSMsg* out, i64 timeout_ms) {
    while (1) {
        char* line = NULL;
        if (_amjs_read_line(js, &line, timeout_ms) != 0) return -1;
        if (!line || !line[0]) continue;
        if (line[0] == '+' || line[0] == '-') continue;  /* +OK / -ERR */
        if (strcmp(line, "PING") == 0) {
            _amjs_send_all(js->fd, (const unsigned char*) "PONG\r\n", 6);
            continue;
        }
        if (strcmp(line, "PONG") == 0) continue;
        if (strncmp(line, "MSG ", 4) == 0) {
            char* subj = NULL; char* reply = NULL; size_t bytes = 0;
            if (_amjs_parse_msg_hdr(line, &subj, &reply, &bytes) != 0) return -1;
            char* data = NULL;
            if (_amjs_read_n(js, &data, bytes, timeout_ms) != 0) return -1;
            /* Strip trailing CRLF that NATS sends after the payload. */
            char* trail = NULL;
            if (_amjs_read_n(js, &trail, 2, timeout_ms) != 0) return -1;
            out->subject = subj;
            out->reply   = reply;
            out->data    = data;
            return 0;
        }
        /* unknown line — try again */
    }
}

/* Send a JS API request: PUB to <api_subject> with reply-to =
 * a fresh inbox, SUB the inbox, wait for the response MSG, UNSUB.
 * Caller owns the returned response body string (may be NULL on
 * error). */
static inline char* _amjs_api_request(AmalgameJetStream* js,
                                       const char* api_subject,
                                       const char* json_body,
                                       i64 timeout_ms) {
    if (js->fd < 0) {
        js->last_error = _amjs_err_dup("api: not open");
        return NULL;
    }
    char* inbox = _amjs_next_inbox(js);
    int sid = js->inbox_seq;   /* reuse seq for sid */

    if (_amjs_sub(js, inbox, sid) < 0) {
        js->last_error = _amjs_err_dup("api: SUB failed"); return NULL;
    }
    size_t body_n = json_body ? strlen(json_body) : 0;
    if (_amjs_pub(js, api_subject, inbox, json_body ? json_body : "", body_n) < 0) {
        js->last_error = _amjs_err_dup("api: PUB failed"); return NULL;
    }

    AmalgameJSMsg m = {0};
    int rc = _amjs_recv_msg(js, &m, timeout_ms);
    _amjs_unsub(js, sid);
    if (rc != 0) {
        js->last_error = _amjs_err_dup("api: timeout / no reply");
        return NULL;
    }
    js->last_error = NULL;
    return m.data;
}

/* ── JSON helpers — minimal, enough for stream/consumer ─── */

/* Look for "key":<number> in s and return the parsed number, or
 * defval if not found. Only scans simple top-level integer values
 * — enough for "seq", "domain" etc. in PubAck and friends. */
static inline i64 _amjs_json_int(const char* s, const char* key, i64 defval) {
    if (!s) return defval;
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char* p = strstr(s, needle);
    if (!p) return defval;
    p += strlen(needle);
    while (*p == ' ' || *p == ':' || *p == '\t') p++;
    if (*p == '"') return defval;  /* string value — caller wanted int */
    return (i64) atoll(p);
}

/* Look for "key":"…" and return a heap-alloc'd copy. NULL if
 * absent. Doesn't handle escape sequences (good enough for stream
 * names + subjects + simple error strings). */
static inline char* _amjs_json_str(const char* s, const char* key) {
    if (!s) return NULL;
    char needle[64];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char* p = strstr(s, needle);
    if (!p) return NULL;
    p += strlen(needle);
    while (*p == ' ' || *p == ':' || *p == '\t') p++;
    if (*p != '"') return NULL;
    p++;
    const char* q = p;
    while (*q && *q != '"') q++;
    if (!*q) return NULL;
    return _amjs_str_dup(p, (size_t)(q - p));
}

/* Returns 1 if the JSON has "error":{...} or "type":"...error..." */
static inline int _amjs_json_has_error(const char* s) {
    if (!s) return 1;
    if (strstr(s, "\"error\":{")) return 1;
    if (strstr(s, "\"err_code\":")) return 1;
    return 0;
}

/* ── Stream management ─────────────────────────────── */

static inline code_bool Amalgame_Messaging_NATS_JetStream_StreamCreate(
        AmalgameJetStream* js, code_string name, AmalgameList* subjects) {
    if (!js || js->fd < 0 || !name) {
        if (js) js->last_error = _amjs_err_dup("StreamCreate: not open");
        return 0;
    }
    /* Build JSON: {"name":"X","subjects":["a","b"],"storage":"file","retention":"limits"} */
    char json[2048];
    int pos = snprintf(json, sizeof(json),
        "{\"name\":\"%s\",\"subjects\":[", name);
    i64 sn = subjects ? AmalgameList_count(subjects) : 0;
    for (i64 i = 0; i < sn; i++) {
        const char* s = (const char*) AmalgameList_get(subjects, i);
        if (!s) continue;
        pos += snprintf(json + pos, sizeof(json) - pos,
            "%s\"%s\"", (i == 0 ? "" : ","), s);
    }
    snprintf(json + pos, sizeof(json) - pos,
        "],\"storage\":\"file\",\"retention\":\"limits\",\"max_consumers\":-1,\"max_msgs\":-1,\"max_bytes\":-1,\"discard\":\"old\",\"num_replicas\":1}");

    char api_subject[256];
    snprintf(api_subject, sizeof(api_subject), "$JS.API.STREAM.CREATE.%s", name);

    char* resp = _amjs_api_request(js, api_subject, json, 5000);
    if (!resp) return 0;
    if (_amjs_json_has_error(resp)) {
        js->last_error = _amjs_err_dup(resp);
        return 0;
    }
    return 1;
}

static inline code_bool Amalgame_Messaging_NATS_JetStream_StreamDelete(
        AmalgameJetStream* js, code_string name) {
    if (!js || js->fd < 0 || !name) {
        if (js) js->last_error = _amjs_err_dup("StreamDelete: not open");
        return 0;
    }
    char api_subject[256];
    snprintf(api_subject, sizeof(api_subject), "$JS.API.STREAM.DELETE.%s", name);
    char* resp = _amjs_api_request(js, api_subject, "", 5000);
    if (!resp) return 0;
    if (_amjs_json_has_error(resp)) {
        js->last_error = _amjs_err_dup(resp);
        return 0;
    }
    return 1;
}

static inline code_string Amalgame_Messaging_NATS_JetStream_StreamInfo(
        AmalgameJetStream* js, code_string name) {
    if (!js || js->fd < 0 || !name) return (code_string) "";
    char api_subject[256];
    snprintf(api_subject, sizeof(api_subject), "$JS.API.STREAM.INFO.%s", name);
    char* resp = _amjs_api_request(js, api_subject, "", 5000);
    if (!resp) return (code_string) "";
    return (code_string) resp;
}

/* ── Publishing — PubAck-aware ─────────────────────── */

static inline code_bool Amalgame_Messaging_NATS_JetStream_Publish(
        AmalgameJetStream* js, code_string subject, code_string payload) {
    if (!js || js->fd < 0 || !subject) {
        if (js) js->last_error = _amjs_err_dup("Publish: not open");
        return 0;
    }
    /* JetStream PUBs return a PubAck JSON on a reply-to subject. */
    char* inbox = _amjs_next_inbox(js);
    int sid = js->inbox_seq;
    if (_amjs_sub(js, inbox, sid) < 0) {
        js->last_error = _amjs_err_dup("Publish: SUB inbox failed");
        return 0;
    }
    size_t pn = payload ? strlen(payload) : 0;
    if (_amjs_pub(js, subject, inbox, payload ? payload : "", pn) < 0) {
        js->last_error = _amjs_err_dup("Publish: PUB failed");
        return 0;
    }
    AmalgameJSMsg m = {0};
    int rc = _amjs_recv_msg(js, &m, 5000);
    _amjs_unsub(js, sid);
    if (rc != 0) {
        js->last_error = _amjs_err_dup("Publish: no PubAck");
        return 0;
    }
    if (_amjs_json_has_error(m.data)) {
        js->last_error = _amjs_err_dup(m.data);
        return 0;
    }
    js->last_seq = _amjs_json_int(m.data, "seq", 0);
    js->last_error = NULL;
    return 1;
}

static inline i64 Amalgame_Messaging_NATS_JetStream_LastSeq(AmalgameJetStream* js) {
    return js ? js->last_seq : 0;
}

/* ── Pull consumer (durable, AckExplicit) ────────────── */

static inline code_bool Amalgame_Messaging_NATS_JetStream_ConsumerCreate(
        AmalgameJetStream* js, code_string stream, code_string name) {
    if (!js || js->fd < 0 || !stream || !name) {
        if (js) js->last_error = _amjs_err_dup("ConsumerCreate: not open");
        return 0;
    }
    /* JSON: {"stream_name":"S","config":{"durable_name":"N","ack_policy":"explicit"}} */
    char json[1024];
    snprintf(json, sizeof(json),
        "{\"stream_name\":\"%s\",\"config\":{\"durable_name\":\"%s\",\"ack_policy\":\"explicit\",\"deliver_policy\":\"all\",\"replay_policy\":\"instant\",\"max_deliver\":-1}}",
        stream, name);

    char api_subject[256];
    snprintf(api_subject, sizeof(api_subject),
        "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s", stream, name);

    char* resp = _amjs_api_request(js, api_subject, json, 5000);
    if (!resp) return 0;
    if (_amjs_json_has_error(resp)) {
        js->last_error = _amjs_err_dup(resp);
        return 0;
    }
    return 1;
}

/* Pull up to `batch` messages, waiting `timeout_ms` for at least
 * one. Returns the count actually received (0 if none arrived in
 * the window). Messages are buffered in js->pulled[] for AM to
 * fetch via MessageSubject/Data/Reply by index. */
static inline i64 Amalgame_Messaging_NATS_JetStream_ConsumerPull(
        AmalgameJetStream* js, code_string stream, code_string name,
        i64 batch, i64 timeout_ms) {
    if (!js || js->fd < 0 || !stream || !name) {
        if (js) js->last_error = _amjs_err_dup("ConsumerPull: not open");
        return 0;
    }
    /* Clear previous batch. */
    js->pulled_n = 0;
    if (batch <= 0) batch = 1;
    if (batch > AMALGAME_JS_MAX_PULL) batch = AMALGAME_JS_MAX_PULL;

    /* Request: PUB $JS.API.CONSUMER.MSG.NEXT.<stream>.<consumer>
     *          reply-to=<inbox> body={"batch":N,"expires":<ns>} */
    char* inbox = _amjs_next_inbox(js);
    int sid = js->inbox_seq;
    if (_amjs_sub(js, inbox, sid) < 0) {
        js->last_error = _amjs_err_dup("ConsumerPull: SUB failed");
        return 0;
    }

    char json[256];
    snprintf(json, sizeof(json),
        "{\"batch\":%lld,\"expires\":%lld}",
        (long long) batch, (long long) (timeout_ms * 1000000LL));
    char api_subject[256];
    snprintf(api_subject, sizeof(api_subject),
        "$JS.API.CONSUMER.MSG.NEXT.%s.%s", stream, name);
    size_t body_n = strlen(json);
    if (_amjs_pub(js, api_subject, inbox, json, body_n) < 0) {
        js->last_error = _amjs_err_dup("ConsumerPull: PUB failed");
        return 0;
    }

    /* Read responses until batch hits or we get a 408 Request Timeout
     * (status message: subject = inbox, payload empty, header begins
     * with "NATS/1.0 408" in HMSG — we don't parse HMSG yet, so just
     * stop on the first 0-byte MSG body). */
    i64 deadline_ms = timeout_ms + 500;
    for (i64 i = 0; i < batch; i++) {
        AmalgameJSMsg m = {0};
        if (_amjs_recv_msg(js, &m, deadline_ms) != 0) break;
        /* Skip status-only responses (empty payload from JetStream
         * means "no more for now"). */
        if (!m.data || m.data[0] == '\0') break;
        js->pulled[js->pulled_n++] = m;
    }
    _amjs_unsub(js, sid);
    js->last_error = NULL;
    return (i64) js->pulled_n;
}

static inline code_string Amalgame_Messaging_NATS_JetStream_MessageSubject(
        AmalgameJetStream* js, i64 idx) {
    if (!js || idx < 0 || idx >= js->pulled_n) return (code_string) "";
    return (code_string) js->pulled[idx].subject;
}

static inline code_string Amalgame_Messaging_NATS_JetStream_MessageData(
        AmalgameJetStream* js, i64 idx) {
    if (!js || idx < 0 || idx >= js->pulled_n) return (code_string) "";
    return (code_string) js->pulled[idx].data;
}

static inline code_string Amalgame_Messaging_NATS_JetStream_MessageReply(
        AmalgameJetStream* js, i64 idx) {
    if (!js || idx < 0 || idx >= js->pulled_n) return (code_string) "";
    return (code_string) js->pulled[idx].reply;
}

/* ACK every pulled message — PUB <reply> empty payload, no
 * reply-to (fire-and-forget; JetStream doesn't ACK an ACK). */
static inline void Amalgame_Messaging_NATS_JetStream_AckAll(AmalgameJetStream* js) {
    if (!js || js->fd < 0) return;
    for (int i = 0; i < js->pulled_n; i++) {
        const char* r = js->pulled[i].reply;
        if (!r || !*r) continue;
        _amjs_pub(js, r, "", "", 0);
    }
}

#endif /* AMALGAME_MESSAGING_NATS_JETSTREAM_H */
