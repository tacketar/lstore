/*
   Copyright 2016 Vanderbilt University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

//*********************************************************************
//*********************************************************************

#define _log_module_index 121

#include <apr_errno.h>
#include <apr_time.h>
#include <assert.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sodium.h>

// Private implementation
#include "debug.h"
#include "network.h"
#include "tbx/assert_result.h"
#include "tbx/atomic_counter.h"
#include "tbx/fmttypes.h"
#include "tbx/log.h"
#include "tbx/network.h"
#include "tbx/type_malloc.h"
#include "transfer_buffer.h"

#define ENCRYPT_STRING      "ENCYRPT:"
#define ENCRYPT_STRING_SIZE 8
#define ENCRYPT_KEY_SIZE    crypto_kx_PUBLICKEYBYTES
#define ENCRYPT_PACKET_SIZE (ENCRYPT_STRING_SIZE+ENCRYPT_KEY_SIZE)

// Accessors
int tbx_ns_getid(tbx_ns_t *ns) {
    return ns->id;
}
void tbx_ns_setid(tbx_ns_t *ns, int id) {
    ns->id = id;
}

void tbx_ns_encrypt_enable(tbx_ns_t *ns) { ns->encrypted = 1; }
int tbx_ns_encrypt_status(tbx_ns_t *ns) { return(ns->encrypted); }
char *tbx_nm_host_get(tbx_ns_monitor_t *nm) {return(nm->address); }
int tbx_nm_port_get(tbx_ns_monitor_t *nm) {return(nm->port); }
tbx_ns_monitor_t *tbx_ns_monitor_get(tbx_ns_t *ns) { return (ns->nm); }

char *tbx_ns_peer_address_get(tbx_ns_t *ns)
{
    return(ns->peer_address);
}

void tbx_ns_chksum_write_set(tbx_ns_t *ns, tbx_ns_chksum_t ncs) {
    ns->write_chksum = ncs;
}
void tbx_ns_chksum_write_disable(tbx_ns_t *ns) {
    ns->write_chksum.is_running = 0;
}
void tbx_ns_chksum_read_set(tbx_ns_t *ns, tbx_ns_chksum_t ncs) {
    ns->read_chksum = ncs;
}
void tbx_ns_chksum_read_clear(tbx_ns_t *ns) { (ns)->read_chksum.is_valid = 0; }
void tbx_ns_chksum_read_disable(tbx_ns_t *ns) {  (ns)->read_chksum.is_running = 0; }
void tbx_ns_chksum_read_enable(tbx_ns_t *ns) { (ns)->read_chksum.is_running = 1; }
void tbx_ns_chksum_write_clear(tbx_ns_t *ns) { (ns)->write_chksum.is_valid = 0; }
void tbx_ns_chksum_write_enable(tbx_ns_t *ns) { (ns)->write_chksum.is_running = 1; }

void tbx_ns_chksum_del(tbx_ns_chksum_t *nsc)  { free(nsc); }

tbx_ns_chksum_t *tbx_ns_chksum_new()
{
  tbx_ns_chksum_t *nsc;

  tbx_type_malloc_clear(nsc, tbx_ns_chksum_t, 1);

  return(nsc);
}

// Forward declarations
int _tbx_ns_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout, int dolock);
int _read_netstream_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, int pos, int size, int dolock);
int _tbx_ns_write(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout, int dolock);
int _write_netstream_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, unsigned int boff, int size, int dolock);

int tcp_bufsize = 0;   //** 0 means use the default TCP buffer sizes for the OS

//*** These are used for counters to track connections
static tbx_atomic_int_t _cuid_counter = 0;
//apr_thread_mutex_t *_net_counter_lock = NULL;
//apr_pool_t *_net_counter_pool = NULL;

//------------------

tbx_ns_t *_get_free_conn(tbx_network_t *net);
void tbx_ns_config_1_ssl(tbx_ns_t *ns, int fd, int tcpsize) { }
void tbx_ns_config_2_ssl(tbx_ns_t *ns, int tcpsize) { }


int tbx_ns_generate_id()
{
    int id;

    id = tbx_atomic_counter(&_cuid_counter);
    log_printf(15, "ns_generate_id: _cuid=%d\n", id);

    return(id);
}

//*********************************************************************
// set/get_network_tcpsize - Sets/gets the default TCP window size.
//     If 0 then the OS defaults are used.
//*********************************************************************

void set_network_tcpsize(int tcpsize)
{
    tcp_bufsize = tcpsize;
}

int get_network_tcpsize(int tcpsize)
{
    return(tcp_bufsize);
}

//*********************************************************************
//  connection_is_pending - Returns if a new connection is needed
//*********************************************************************

int connection_is_pending(tbx_network_t *net)
{

    apr_thread_mutex_lock(net->ns_lock);
    int i = net->accept_pending;
    apr_thread_mutex_unlock(net->ns_lock);

    return(i);
}

//*********************************************************************
//  Locks for R/W
//*********************************************************************

void lock_read_ns(tbx_ns_t *ns)
{
    apr_thread_mutex_lock(ns->read_lock);
}

//*********************************************************************

void unlock_read_ns(tbx_ns_t *ns)
{
    apr_thread_mutex_unlock(ns->read_lock);
}

//*********************************************************************

void lock_write_ns(tbx_ns_t *ns)
{
    apr_thread_mutex_lock(ns->write_lock);
}

//*********************************************************************

void unlock_write_ns(tbx_ns_t *ns)
{
    apr_thread_mutex_unlock(ns->write_lock);
}

//*********************************************************************
// lock_ns - Locks a netstream
//*********************************************************************

void lock_ns(tbx_ns_t *ns)
{
    lock_read_ns(ns);
    lock_write_ns(ns);
}

//*********************************************************************
// unlock_ns - Unlocks a netstream
//*********************************************************************

void unlock_ns(tbx_ns_t *ns)
{
    unlock_write_ns(ns);
    unlock_read_ns(ns);
}

//*********************************************************************
// ns_getid - Returns the ID
//*********************************************************************

#define tbx_ns_getid(ns) ns->id

//*********************************************************************
// ns_set_chksum - Associates a chksum to the stream
//*********************************************************************

int tbx_ns_chksum_set(tbx_ns_chksum_t *ncs, tbx_chksum_t *cks, size_t blocksize)
{
    ncs->blocksize = blocksize;
    ncs->bytesleft = blocksize;
    ncs->chksum = *cks;
    ncs->is_running = 0;
    ncs->is_valid = 1;

    return(0);
}

//*********************************************************************
// ns_chksum_is_valid - Returns if the chksum can be used
//*********************************************************************

int tbx_ns_chksum_is_valid(tbx_ns_chksum_t *ncs)
{
    int i = 0;

    if (ncs->is_valid == 1) {
        if (tbx_chksum_type(&(ncs->chksum)) != CHKSUM_NONE) i = 1;
    }

    return(i);
}


//*********************************************************************
// ns_read_chksum_flush - Assumes the next bytes in the stream are
//   the chksum and reads them and does a comparison
//*********************************************************************

int tbx_ns_chksum_read_flush(tbx_ns_t *ns)
{
    char ns_value[CHKSUM_MAX_SIZE], chksum_value[CHKSUM_MAX_SIZE];
    int err, n;
    tbx_tbuf_t buf;

    log_printf(15, "ns_read_chksum_flush: Reading chksum!  ns=%d type=%d bleft=" I64T " bsize=" I64T " state=%d\n",
               tbx_ns_getid(ns), tbx_chksum_type(&(ns->read_chksum.chksum)), ns->read_chksum.bytesleft, ns->read_chksum.blocksize, ns_read_chksum_state(ns));
    tbx_log_flush();

    if (ns_read_chksum_state(ns) == 0) return(0);
    if (ns->read_chksum.bytesleft == ns->read_chksum.blocksize) return(0);  //** Nothing to do

    n = tbx_chksum_size(&(ns->read_chksum.chksum), CHKSUM_DIGEST_HEX);


    ns->read_chksum.is_running = 0;  //** Don't want to get in an endless loop
    tbx_tbuf_single(&buf, n, ns_value);
    err = _read_netstream_block(ns, apr_time_now() + apr_time_make(5,0), &buf, 0, n, 0);
    ns_value[n] = '\0';
    ns->read_chksum.is_running = 1;

    log_printf(15, "ns_read_chksum_flush: Finished reading chksum!  ns=%d\n", tbx_ns_getid(ns));
    tbx_log_flush();

    if (err != 0) {
        log_printf(10, "ns_read_chksum_flush: ns=%d Error reading chksum! error=%d\n", tbx_ns_getid(ns), err);
        return(err);
    }

    tbx_chksum_get(&(ns->read_chksum.chksum), CHKSUM_DIGEST_HEX, chksum_value);
    log_printf(15, "ns_read_chksum_flush: after tbx_chksum_get!  ns=%d\n", tbx_ns_getid(ns));
    tbx_log_flush();
    err = (strncmp(chksum_value, ns_value, n) == 0) ? 0 : 1;

    log_printf(15, "ns_read_chksum_flush: ns=%d     ns_value=%s  cmp=%d\n", tbx_ns_getid(ns), ns_value, err);
    log_printf(15, "ns_read_chksum_flush: ns=%d chksum_value=%s\n", tbx_ns_getid(ns), chksum_value);
    if (err != 0) {
        log_printf(1, "ns_read_chksum_flush: ns=%d chksum error!\n", tbx_ns_getid(ns));
        log_printf(1, "ns_read_chksum_flush: ns=%d     ns_value=%s  cmp=%d\n", tbx_ns_getid(ns), ns_value, err);
        log_printf(1, "ns_read_chksum_flush: ns=%d chksum_value=%s\n", tbx_ns_getid(ns), chksum_value);
    }

    log_printf(15, "ns_read_chksum_flush: end of routine!  ns=%d\n err=%d", tbx_ns_getid(ns), err);
    tbx_log_flush();

    return(err);
}

//*********************************************************************
// ns_write_chksum_flush - Injects the chksum into the stream
//*********************************************************************

int tbx_ns_chksum_write_flush(tbx_ns_t *ns)
{
    char chksum_value[CHKSUM_MAX_SIZE];
    int err, n;
    tbx_tbuf_t buf;

    log_printf(15, "ns_write_chksum_flush: injecting chksum!  ns=%d type=%d bytesleft=" I64T " bsize=" I64T "\n",
               tbx_ns_getid(ns), tbx_chksum_type(&(ns->write_chksum.chksum)), ns->write_chksum.bytesleft, ns->write_chksum.blocksize);
    tbx_log_flush();

    if (ns_write_chksum_state(ns) == 0) return(0);
    if (ns->write_chksum.bytesleft == ns->write_chksum.blocksize) return(0);  //** Nothing to do

    n = tbx_chksum_size(&(ns->write_chksum.chksum), CHKSUM_DIGEST_HEX);
    tbx_chksum_get(&(ns->write_chksum.chksum), CHKSUM_DIGEST_HEX, chksum_value);

    ns->write_chksum.is_running = 0;  //** Don't want to get in an endless loop
    tbx_tbuf_single(&buf, n, chksum_value);
    err = _write_netstream_block(ns, apr_time_now() + apr_time_make(5,0), &buf, 0, n, 0);
    ns->write_chksum.is_running = 1;

    if (err != 0) {
        log_printf(10, "ns_write_chksum_flush: ns=%d Error writing chksum! error=%d\n", tbx_ns_getid(ns), err);
        return(err);
    }

    chksum_value[n] = '\0';
    log_printf(15, "ns_write_chksum_flush: ns=%d chksum_value=%s\n", tbx_ns_getid(ns), chksum_value);
    log_printf(15, "ns_write_chksum_flush: end of routine!  ns=%d\n err=%d", tbx_ns_getid(ns), err);
    tbx_log_flush();

    return(err);
}

//*********************************************************************
// ns_tbx_chksum_reset - Resets the chksum counter
//*********************************************************************

int tbx_ns_chksum_reset(tbx_ns_chksum_t *ncs)
{
    ncs->bytesleft = ncs->blocksize;
    tbx_chksum_reset(&(ncs->chksum));

    return(0);
}

//*********************************************************************
//  network_counter - Returns the network counter or the number
//     of sockets used so far
//*********************************************************************

int tbx_network_counter(tbx_network_t *net)
{
    int count;

    count = tbx_atomic_get(_cuid_counter);

    return(count);
}

//*********************************************************************
// tbx_ns_timeout_get - Get the timeout
//*********************************************************************

void tbx_ns_timeout_get(tbx_ns_timeout_t tm, int *sec, int *us)
{
    *sec = tm / 1000000;
    *us = tm % 1000000;

    return;;
}

//*********************************************************************
// set_net_timeout - Initializes the timout data structure
//*********************************************************************

tbx_ns_timeout_t *tbx_ns_timeout_set(tbx_ns_timeout_t *tm, int sec, int us)
{
    *tm = (apr_time_t)sec*1000000 + (apr_time_t)us;
//log_printf(0, "set_net_timoeut: tm=" TT " sec=%d us=%d\n", *tm, sec, us);

    return(tm);
}

//*********************************************************************
//  _ns_monitor_create - Creates the NS send/recv monitoring objects
//*********************************************************************

void _ns_monitor_create(tbx_ns_t *ns, int port, char *type)
{
    char *label;

    label = tbx_ns_encrypt_status(ns) ? "encrypted" : "plain";
    tbx_monitor_object_fill(&(ns->mo_send), MON_INDEX_NSSEND, ns->id);
    tbx_monitor_object_fill(&(ns->mo_recv), MON_INDEX_NSRECV, ns->id);
    tbx_monitor_obj_create(&(ns->mo_send), "type=%s host=%s port=%d mode=%s", type, ns->peer_address, port, label);
    tbx_monitor_obj_create(&(ns->mo_recv), "type=%s host=%s port=%d mode=%s", type, ns->peer_address, port, label);
}

//*********************************************************************
//  _ns_monitor_destroy - Destroys the NS send/recv monitoring objects
//*********************************************************************

void _ns_monitor_destroy(tbx_ns_t *ns)
{
    tbx_monitor_obj_destroy(&(ns->mo_send));
    tbx_monitor_obj_destroy(&(ns->mo_recv));
}

//*********************************************************************
// _ns_init - Inits a NetStream data structure assuming a connected state
//*********************************************************************

void _ns_init(tbx_ns_t *ns, int incid)
{
    //** Initialize the socket type information **
    ns->sock_type = NS_TYPE_UNKNOWN;
    ns->sock = NULL;
    ns->close = NULL;
    ns->read = NULL;
    ns->write = NULL;
    ns->sock_status = NULL;
    ns->set_peer = NULL;
    ns->connect = NULL;
    ns->nm = NULL;

    ns->last_read = apr_time_now();
    ns->last_write = apr_time_now();
    ns->start = 0;
    ns->end = -1;
    ns->encrypted = 0;
    ns->enc = NULL;
    memset(ns->peer_address, 0, sizeof(ns->peer_address));

    memset(&(ns->write_chksum), 0, sizeof(tbx_ns_chksum_t));
    ns->write_chksum.is_valid = 0;
    memset(&(ns->read_chksum), 0, sizeof(tbx_ns_chksum_t));
    ns->read_chksum.is_valid = 0;

    if (incid == 1)  ns->id = tbx_ns_generate_id();

//  log_printf(15, "_ns_init: incid=%d ns=%d\n", incid, ns->id);

}


//*********************************************************************
// ns_init - inits a tbx_ns_t data structure
//*********************************************************************

void ns_init(tbx_ns_t *ns)
{
    _ns_init(ns, 1);
}

//*********************************************************************
// ns_clone - Clones the ns settings from one ns to another.
//     The sock is also copied but it can lead to problems if
//     not used properly.  Normally this field should be set to NULL
//*********************************************************************

void ns_clone(tbx_ns_t *dest_ns, tbx_ns_t *src_ns)
{
    apr_thread_mutex_t *rl, *wl;
    apr_pool_t *mpool;

    //** Need to preserve the locks and pool they came from
    rl = dest_ns->read_lock;
    wl = dest_ns->write_lock;
    mpool = dest_ns->mpool;

    lock_ns(src_ns);
    memcpy(dest_ns, src_ns, sizeof(tbx_ns_t));
    unlock_ns(src_ns);

    dest_ns->read_lock = rl;
    dest_ns->write_lock = wl;
    dest_ns->mpool = mpool;
}

//*********************************************************************
// _ns_encrypt_client - Performs the handshake for doing encryption
//*********************************************************************

int _ns_encrypt_client(tbx_ns_t *ns)
{
    tbx_tbuf_t ns_tb;
    tbx_ns_timeout_t to;
    int nbytes = 0;

    ns->encrypted = 0;  //** We disable if for the handshake

    //** Make the encryption object
    tbx_type_malloc_clear(ns->enc, tbx_ns_encrypt_t, 1);

    //** Make my keys and send my public key to the server
    //** Make the keys
    crypto_kx_keypair(ns->enc->my_key_pk, ns->enc->my_key_sk);

    //** Send them my public key
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_BUFSIZE, ns->buffer);
    memcpy(ns->buffer, ENCRYPT_STRING, ENCRYPT_STRING_SIZE);
    memcpy(ns->buffer + ENCRYPT_STRING_SIZE, ns->enc->my_key_pk, crypto_kx_PUBLICKEYBYTES);
    nbytes = _tbx_ns_write(ns, &ns_tb, 0, ENCRYPT_PACKET_SIZE, to, 0);
    if (nbytes != ENCRYPT_PACKET_SIZE) {
        log_printf(0, "ERROR: Failed sending encrypt initial packet! ns=%d\n", ns->id);
        return(1);
    }

    //** Attempt to Read the server's public key
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, crypto_kx_PUBLICKEYBYTES, (char *)ns->enc->remote_key_pk);
    nbytes = _tbx_ns_read(ns, &ns_tb, 0, crypto_kx_PUBLICKEYBYTES, to, 0);  //**there should be 0 bytes in buffer now since this si the 1st read
    if (nbytes != crypto_kx_PUBLICKEYBYTES) {
        log_printf(0, "ERROR: Failed getting server public key! ns=%d\n", ns->id);
        return(1);
    }

    //** Make the session keys
    if (crypto_kx_client_session_keys(ns->enc->my_key_rx, ns->enc->my_key_tx, ns->enc->my_key_pk, ns->enc->my_key_sk, ns->enc->remote_key_pk) != 0) {
        log_printf(0, "ERROR: Failed generating session keys! ns=%d\n", ns->id);
        return(1);
    }

    //** Initialize the 2 separate encryption streams. We have to exchange the TX header/nonces
    crypto_secretstream_xchacha20poly1305_init_push(&(ns->enc->state_write), ns->enc->header_write, ns->enc->my_key_tx);
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_HEADER_SIZE , (char *)ns->enc->header_write);
    nbytes = _tbx_ns_write(ns, &ns_tb, 0, N_ENCRYPT_PACKET_HEADER_SIZE , to, 0);
    if (nbytes != N_ENCRYPT_PACKET_HEADER_SIZE ) {
        log_printf(0, "ERROR: Failed sending header nonce packet! ns=%d\n", ns->id);
        return(1);
    }

    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_HEADER_SIZE , (char *)ns->enc->header_read);
    nbytes = _tbx_ns_read(ns, &ns_tb, 0, N_ENCRYPT_PACKET_HEADER_SIZE , to, 0);
    if (nbytes != N_ENCRYPT_PACKET_HEADER_SIZE) {
        log_printf(0, "ERROR: Failed getting reading header nonce packet! ns=%d\n", ns->id);
        return(1);
    }
    crypto_secretstream_xchacha20poly1305_init_pull(&(ns->enc->state_read), ns->enc->header_read, ns->enc->my_key_rx);

    ns->encrypted = 1;  //** Flag the connection as encrypted
    log_printf(10, "ENCRYPTED: ns=%d\n", ns->id);

    return(0);
}

//*********************************************************************
// _ns_encrypt_server_handshake - Performs the handshake for doing encryption on the server
//    if the client requests it.  This is accomplished by checking for the handshake
//    encrypt command in the first few seconds of the connections
//*********************************************************************

int _ns_encrypt_server_handshake(tbx_ns_t *ns)
{
    tbx_tbuf_t ns_tb;
    tbx_ns_timeout_t to;
    int nbytes = 0;
    int err = 0;

    //** Attempt to Read the first packet and dump it into the stream buffer
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_BUFSIZE, ns->buffer);
    nbytes = _tbx_ns_read(ns, &ns_tb, 0, ENCRYPT_PACKET_SIZE, to, 0);  //**there should be 0 bytes in buffer now since this si the 1st read
    if (nbytes != ENCRYPT_PACKET_SIZE) goto done; //** Not enough characters to enable encryption

    //** check if they are requesting encryption
    if (memcmp(ns->buffer, ENCRYPT_STRING, ENCRYPT_STRING_SIZE) != 0) goto done;  //** No match so kick out

    //** If we made it here then we're going to try and use encyption
    nbytes = 0;  //** We've consumed the initial packet
    tbx_type_malloc_clear(ns->enc, tbx_ns_encrypt_t, 1);

    //** Store the public key of the client
    memcpy(ns->enc->remote_key_pk, ns->buffer + ENCRYPT_STRING_SIZE, crypto_kx_PUBLICKEYBYTES);

    //** Make the keys
    crypto_kx_keypair(ns->enc->my_key_pk, ns->enc->my_key_sk);

    //** Send them my public key
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_BUFSIZE, (char *)ns->enc->my_key_pk);
    nbytes = _tbx_ns_write(ns, &ns_tb, 0, crypto_kx_PUBLICKEYBYTES, to, 0);
    err = (nbytes == crypto_kx_PUBLICKEYBYTES) ? 0 : 1;
    nbytes = 0;

    //** Make the session keys
    memcpy(ns->enc->remote_key_pk, ns->buffer + ENCRYPT_STRING_SIZE, crypto_kx_PUBLICKEYBYTES);
    if (crypto_kx_server_session_keys(ns->enc->my_key_rx, ns->enc->my_key_tx, ns->enc->my_key_pk, ns->enc->my_key_sk, ns->enc->remote_key_pk) != 0) {
        log_printf(0, "ERROR: Failed generating session keys! ns=%d\n", ns->id);
        err = 1;
        goto done;

    }

    //** Initialize the 2 separate encryption streams. We have to exchange the TX header/nonces
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_HEADER_SIZE , (char *)ns->enc->header_read);
    nbytes = _tbx_ns_read(ns, &ns_tb, 0, N_ENCRYPT_PACKET_HEADER_SIZE , to, 0);
    if (nbytes != N_ENCRYPT_PACKET_HEADER_SIZE) {
        log_printf(0, "ERROR: Failed getting reading header nonce packet! ns=%d\n", ns->id);
        return(1);
    }
    crypto_secretstream_xchacha20poly1305_init_pull(&(ns->enc->state_read), ns->enc->header_read, ns->enc->my_key_rx);

    crypto_secretstream_xchacha20poly1305_init_push(&(ns->enc->state_write), ns->enc->header_write, ns->enc->my_key_tx);
    tbx_ns_timeout_set(&to, 5, 0);
    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_HEADER_SIZE , (char *)ns->enc->header_write);
    nbytes = _tbx_ns_write(ns, &ns_tb, 0, N_ENCRYPT_PACKET_HEADER_SIZE , to, 0);
    if (nbytes != N_ENCRYPT_PACKET_HEADER_SIZE ) {
        log_printf(0, "ERROR: Failed sending header nonce packet! ns=%d\n", ns->id);
        return(1);
    }

    ns->encrypted = 1;  //** Flag the connection as encrypted
    log_printf(10, "ENCRYPTED: ns=%d\n", ns->id);

done:
    if (ns->encrypted == 0) {
        if (nbytes > 0) {
            ns->start = 0;
            ns->end = nbytes-1;
        }
    }

    return(err);
}

//*********************************************************************
// net_connect - Creates a connection to a remote host.
//*********************************************************************

int tbx_ns_connect(tbx_ns_t *ns, const char *hostname, int port, tbx_ns_timeout_t timeout)
{
    int err, encrypted;

    lock_ns(ns);

    //** Simple check on the connection type **
    switch (ns->sock_type) {
    case NS_TYPE_SOCK:
    case NS_TYPE_PHOEBUS:
    case NS_TYPE_1_SSL:
    case NS_TYPE_ZSOCK:
        break;
    default:
        log_printf(0, "net_connect: Invalid ns_type=%d Exiting!\n", ns->sock_type);
        unlock_ns(ns);
        return(1);
    }

    encrypted = ns->encrypted; //** This get's cleared in the following call
    err = ns->connect(ns->sock, hostname, port, timeout);
    if (err != 0) {
        log_printf(5, "net_connect: select failed.  Hostname: %s  Port: %d select=%d errno: %d error: %s\n", hostname, port, err, errno, strerror(errno));
        unlock_ns(ns);
        return(1);
    }

    ns->set_peer(ns->sock, ns->peer_address, sizeof(ns->peer_address));

    ns->id = tbx_ns_generate_id();
    ns->encrypted = encrypted; //** Restore it

    log_printf(10, "net_connect:  Made connection to %s:%d on ns=%d address=%s\n", hostname, port, ns->id, ns->peer_address);

    err = (ns->encrypted) ? _ns_encrypt_client(ns) : 0;

    _ns_monitor_create(ns, port, "connect");

    log_printf(10, "net_connect: final ns=%d\n", ns->id);
    unlock_ns(ns);

    return(err);
}

//*********************************************************************
//  monitor_thread - Thread for monitoring a network connection for
//     incoming connection requests.
//*********************************************************************

void *monitor_thread(apr_thread_t *th, void *data)
{
    tbx_ns_monitor_t *nm = (tbx_ns_monitor_t *)data;
    tbx_ns_t *ns = nm->ns;
    int i;

    log_printf(15, "monitor_thread: Monitoring port %d\n", nm->port);

    apr_thread_mutex_lock(nm->lock);
    while (nm->shutdown_request == 0) {
        apr_thread_mutex_unlock(nm->lock);

        i = ns->connection_request(ns->sock, 1);

        if (i == 1) {  //** Got a request
            log_printf(15, "monitor_thread: port=%d ns=%d Got a connection request time=" TT "\n", nm->port, tbx_ns_getid(ns), apr_time_now());

            //** Mark that I have a connection pending
            apr_thread_mutex_lock(nm->lock);
            nm->is_pending = 1;
            apr_thread_mutex_unlock(nm->lock);

            //** Wake up the calling thread
            apr_thread_mutex_lock(nm->trigger_lock);
            (*(nm->trigger_count))++;
            apr_thread_cond_signal(nm->trigger_cond);
            apr_thread_mutex_unlock(nm->trigger_lock);

            log_printf(15, "monitor_thread: port=%d ns=%d waiting for accept\n", nm->port, tbx_ns_getid(ns));

            //** Sleep until my connection is accepted
            apr_thread_mutex_lock(nm->lock);
            while ((nm->is_pending == 1) && (nm->shutdown_request == 0)) {
                apr_thread_cond_wait(nm->cond, nm->lock);
                log_printf(15, "monitor_thread: port=%d ns=%d Cond triggered=" TT " trigger_count=%d\n", nm->port, tbx_ns_getid(ns), apr_time_now(), *(nm->trigger_count));
            }
            apr_thread_mutex_unlock(nm->lock);
            log_printf(15, "monitor_thread: port=%d ns=%d Connection accepted time=" TT "\n", nm->port, tbx_ns_getid(ns), apr_time_now());

            //** Update pending count
//         apr_thread_mutex_lock(nm->trigger_lock);
//         *(nm->trigger_count)--;
//         apr_thread_mutex_unlock(nm->trigger_lock);
        }

        apr_thread_mutex_lock(nm->lock);
    }

    apr_thread_mutex_unlock(nm->lock);

    //** Lastly shutdown my socket
    tbx_ns_close(ns);

    log_printf(15, "monitor_thread: Closing port %d\n", nm->port);

    apr_thread_exit(th, 0);

    return(NULL);
}

//*********************************************************************
// tbx_network_bind - Creates the main port for listening
//*********************************************************************

int tbx_network_bind(tbx_network_t *net, tbx_ns_t *ns, char *address, int port, int max_pending)
{
    int err, slot;
    tbx_ns_monitor_t *nm;

    apr_thread_mutex_lock(net->ns_lock);

    slot = net->used_ports;
    nm = &(net->nm[slot]);

    log_printf(15, "bind_server_port: connection=%s:%d being stored in slot=%d\n", address, port, slot);

    err = ns->bind(ns->sock, address, port);
    if (err != APR_SUCCESS) {
        log_printf(0, "bind_server_port: Error with bind address=%s port=%d err=%d\n", address, port, err);
        goto error1;
    }

    err = ns->listen(ns->sock, max_pending);
    if (err != APR_SUCCESS) {
        log_printf(0, "bind_server_port: Error with listen address=%s port=%d err=%d\n", address, port, err);
        goto error2;
    }

    if (apr_pool_create(&(nm->mpool), NULL) != APR_SUCCESS) {
        err = -1;
        log_printf(0, "bind_server_port: Failed to create pool\n");
        goto error3;
    }

    if (apr_thread_mutex_create(&(nm->lock),
                                APR_THREAD_MUTEX_DEFAULT,
                                nm->mpool) != APR_SUCCESS) {
        err = -1;
        log_printf(0, "bind_server_port: Failed to create mutex\n");
        goto error4;
    }
    if (apr_thread_cond_create(&(nm->cond), nm->mpool) != APR_SUCCESS) {
        err = -1;
        log_printf(0, "bind_server_port: Failed to create cond\n");
        goto error5;
    }

    nm->shutdown_request = 0;
    nm->is_pending = 0;
    nm->ns = ns;
    nm->address = strdup(address);
    if (!nm->address) {
        err = errno;
        log_printf(0, "bind_server_port: couldn't strdup\n");
        goto error6;
    }
    nm->port = port;
    nm->trigger_cond = net->cond;
    nm->trigger_lock = net->ns_lock;
    nm->trigger_count = &(net->accept_pending);
    ns->id = tbx_ns_generate_id();
    _ns_monitor_create(ns, port, "bind");

    if (apr_thread_create(&(nm->thread),
                          NULL,
                          monitor_thread,
                          (void *)nm, nm->mpool) != APR_SUCCESS) {
        err = -1;
        log_printf(0, "bind_server_port: couldn't make worker thread\n");
        goto error7;
    }

    net->used_ports++;
    apr_thread_mutex_unlock(net->ns_lock);

    return(0);
error7:
    free(nm->address);
error6:
    apr_thread_cond_destroy(nm->cond);
error5:
    apr_thread_mutex_destroy(nm->lock);
error4:
    apr_pool_destroy(nm->mpool);
error3:
    // ns->unlisten()
error2:
    ns->close(ns->sock);
error1:
    apr_thread_mutex_unlock(net->ns_lock);
    return err;
}

//*********************************************************************
// close_server_port - Closes a server port
//*********************************************************************

void close_server_port(tbx_ns_monitor_t *nm)
{
    apr_status_t dummy;

    //** Trigger a port shutdown
    apr_thread_mutex_lock(nm->lock);
    nm->shutdown_request = 1;
    log_printf(15, "close_server_port: port=%d Before cond_signal\n", nm->port);
    tbx_log_flush();
    apr_thread_cond_signal(nm->cond);
    log_printf(15, "close_server_port: port=%d After cond_signal\n", nm->port);
    tbx_log_flush();
    apr_thread_mutex_unlock(nm->lock);

    log_printf(15, "close_server_port: port=%d After unlock\n", nm->port);
    tbx_log_flush();

    //** Wait until the thread closes
    apr_thread_join(&dummy, nm->thread);

    log_printf(15, "close_server_port: port=%d After join\n", nm->port);
    tbx_log_flush();

    //** Destroy the actual connection
    tbx_ns_destroy(nm->ns);
    //** Free the actual struct
    free(nm->address);
    apr_thread_mutex_destroy(nm->lock);
    apr_thread_cond_destroy(nm->cond);

    apr_pool_destroy(nm->mpool);

    nm->port = -1;
}


//*********************************************************************
// tbx_network_new - Creates a new network for use
//*********************************************************************

tbx_network_t *tbx_network_new()
{
    int i;
    tbx_network_t *net;

    //**** Allocate space for the data structures ***
    net = (tbx_network_t *)malloc(sizeof(tbx_network_t));FATAL_UNLESS(net != NULL);


    net->used_ports = 0;
    net->accept_pending = 0;
    net->monitor_index = 0;
    assert_result(apr_pool_create(&(net->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(net->ns_lock), APR_THREAD_MUTEX_DEFAULT,net->mpool);
    apr_thread_cond_create(&(net->cond), net->mpool);

    net->used_ports = 0;
    for (i=0; i<NETWORK_MON_MAX; i++) {
        net->nm[i].port = -1;
    }

    return(net);
}

//*********************************************************************
// _close_ns - Close a network connection
//*********************************************************************

void _close_ns(tbx_ns_t *ns)
{

    log_printf(10, "tbx_ns_close:  Closing stream ns=%d type=%d\n", ns->id, ns->sock_type);
    tbx_log_flush();

    ns->cuid = -1;
    if (ns->sock == NULL) return;

    if (ns->sock_status(ns->sock) != 1) return;

    ns->close(ns->sock);

    ns->sock = NULL;

    return;
}

//*********************************************************************
// tbx_ns_close - Close a network connection
//*********************************************************************

void tbx_ns_close(tbx_ns_t *ns)
{
    lock_ns(ns);
    _close_ns(ns);
    unlock_ns(ns);
}

//*********************************************************************
// teardown_netstream - closes an NS and also frees the mutex
//*********************************************************************

void teardown_netstream(tbx_ns_t *ns)
{
    tbx_ns_close(ns);
    if (ns->enc) {
        free(ns->enc);
        ns->enc = NULL;
    }
    if (ns->id > 0) _ns_monitor_destroy(ns);

    apr_thread_mutex_destroy(ns->read_lock);
    apr_thread_mutex_destroy(ns->write_lock);
    apr_pool_destroy(ns->mpool);
}

//*********************************************************************
// destroy_netstream - Completely destroys a netstream created with new_netstream
//*********************************************************************

void tbx_ns_destroy(tbx_ns_t *ns)
{
    teardown_netstream(ns);
    free(ns);
}

//*********************************************************************
// new_netstream - Creates a new NS
//*********************************************************************

tbx_ns_t *tbx_ns_new()
{
    tbx_ns_t *ns = (tbx_ns_t *)malloc(sizeof(tbx_ns_t));

    if (ns == NULL) {
        log_printf(0, "new_netstream: Failed malloc!!\n");
        abort();
    }

    assert_result(apr_pool_create(&(ns->mpool), NULL), APR_SUCCESS);
    apr_thread_mutex_create(&(ns->read_lock), APR_THREAD_MUTEX_DEFAULT,ns->mpool);
    apr_thread_mutex_create(&(ns->write_lock), APR_THREAD_MUTEX_DEFAULT,ns->mpool);

    _ns_init(ns, 0);
    ns->id = ns->cuid = -1;

    return(ns);
}


//*********************************************************************
// tbx_network_close - Closes down all the network connections
//*********************************************************************

void tbx_network_close(tbx_network_t *net)
{
    int i;

    //** Close the attached server ports
    for (i=0; i<NETWORK_MON_MAX; i++) {
        if (net->nm[i].port > 0) {
            close_server_port(&(net->nm[i]));
        }
    }
}

//*********************************************************************
// tbx_network_destroy - Closes and destroys the network struct
//*********************************************************************

void tbx_network_destroy(tbx_network_t *net)
{
    tbx_network_close(net);

    //** Free the main net variables
    apr_thread_mutex_destroy(net->ns_lock);
    apr_thread_cond_destroy(net->cond);
    apr_pool_destroy(net->mpool);

    free(net);
}

//*********************************************************************
// _ns_encrypt_packet_write - Writes an encrypted packet
//*********************************************************************

int _ns_encrypt_packet_write(tbx_ns_t *ns, int bsize, tbx_ns_timeout_t timeout)
{
    int nbytes, n, nleft, err;
    unsigned char *c = ns->enc->enc_packet_write_cipher;
    tbx_tbuf_t tb;

    if (bsize == 0) return(0);

    if (ns->sock_status(ns->sock) != 1) {
        log_printf(15, "write_netstream: connection closed!  ns=%d\n", ns->id);
        return(-1);
    }

    //** Store the size in the buffer
    n = bsize;
    c[0] = n % 256; n = n / 256;
    c[1] = n % 256; n = n / 256;
    c[2] = n;

    //** Make the ciphertext
    crypto_secretstream_xchacha20poly1305_push(&(ns->enc->state_write), c + N_ENCRYPT_INT_SIZE, NULL, ns->enc->enc_packet_write_buf, bsize, NULL, 0, 0);

    //** Send it
    nleft = N_ENCRYPT_INT_SIZE + N_ENCRYPT_PACKET_ENV_SIZE + bsize;
    n = 0;
    tbx_tbuf_single(&tb, nleft, (char *)c);
    do {
        nbytes = ns->write(ns->sock, &tb, n, nleft, timeout);
        if (nbytes > 0) {
            nleft -= nbytes;
            n += nbytes;
        }
    } while ((nleft > 0) && (nbytes >= 0));

    err = (nbytes >= 0) ? bsize : nbytes;

    return(err);
}

//*********************************************************************
// _ns_encrypt_write - Writes data to an encrypted stream
//*********************************************************************

int _ns_encrypt_write(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout)
{
    int i, n, dn, nbytes, err;
    tbx_tbuf_t ns_tb;

    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_DATA_SIZE, (char *)ns->enc->enc_packet_write_buf);
    nbytes = 0;
    for (i=0; i<bsize; i = i + N_ENCRYPT_PACKET_DATA_SIZE) {
        dn = ((i+N_ENCRYPT_PACKET_DATA_SIZE) < bsize) ? N_ENCRYPT_PACKET_DATA_SIZE : bsize - i;
        err = tbx_tbuf_copy(buffer, boff + i, &ns_tb, 0, dn, 0);
        if (err) return(-1);
        n = _ns_encrypt_packet_write(ns, dn, timeout);
        if (n < 0) return(n);  //** Kick out on error
        nbytes += n;
    }

    return(nbytes);
}

//*********************************************************************
// _tbx_ns_write - Writes characters to the stream with a max wait
//*********************************************************************

int _tbx_ns_write(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout, int dolock)
{
    int total_bytes, i;

    if (dolock == 1) lock_write_ns(ns);


    if (ns->sock_status(ns->sock) != 1) {
        log_printf(15, "write_netstream: connection closed!  ns=%d\n", ns->id);
        if (dolock == 1) unlock_write_ns(ns);
        return(-1);
    }

    if (bsize == 0) {
        if (dolock == 1) unlock_write_ns(ns);
        return(0);
    }

    if (ns_write_chksum_state(ns) == 1) {  //** We have chksumming enabled
        if (bsize > ns->write_chksum.bytesleft) {
            bsize = ns->write_chksum.bytesleft;  //** Truncate at the block
        }
    }

    total_bytes = (ns->encrypted == 0) ? ns->write(ns->sock, buffer, boff, bsize, timeout) : _ns_encrypt_write(ns, buffer, boff, bsize, timeout);

    if (total_bytes == -1) {
        log_printf(10, "write_netstream:  Dead connection! ns=%d\n", tbx_ns_getid(ns));
    }

    ns->last_write = apr_time_now();

    if ((ns_write_chksum_state(ns) == 1) && (total_bytes > 0)) {  //** We have chksumming enabled
        tbx_chksum_add(&(ns->write_chksum.chksum), total_bytes, buffer, boff);  //** Chksum it
        ns->write_chksum.bytesleft -= total_bytes;
        if (ns->write_chksum.bytesleft <= 0) { //** Reached the block size so inject the chksum
            i = tbx_ns_chksum_write_flush(ns);
            if (i != 0) total_bytes = NS_CHKSUM;

            //** Reset the chksum
            ns->write_chksum.bytesleft = ns->write_chksum.blocksize;
            tbx_chksum_reset(&(ns->write_chksum.chksum));
        }
    }

    if (dolock == 1) unlock_write_ns(ns);

    tbx_monitor_obj_integer2(&(ns->mo_send), total_bytes, bsize);

    return(total_bytes);
}

//*********************************************************************
// tbx_ns_write - Writes characters to the stream with a max wait
//*********************************************************************

int tbx_ns_write(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout)
{
    return(_tbx_ns_write(ns, buffer, boff, bsize, timeout, 1));
}

//*********************************************************************
//  _write_netstream_block - Same as write_netstream but blocks until the
//     data is sent or end_time is reached
//*********************************************************************

int _write_netstream_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, unsigned int boff, int size, int dolock)
{
    int pos, nleft, nbytes, err;

    tbx_ns_timeout_t dt;

    tbx_ns_timeout_set(&dt, 1, 0);
    pos = boff;
    nleft = size;
    nbytes = -100;
    err = NS_OK;
    while ((nleft > 0) && (err == NS_OK)) {
        nbytes = _tbx_ns_write(ns, buffer, pos, nleft, dt, dolock);
        log_printf(15, "write_netstream_block: ns=%d size=%d nleft=%d nbytes=%d pos=%d time=" TT "\n",
                   tbx_ns_getid(ns), size, nleft, nbytes, pos, apr_time_now());

        if (apr_time_now() > end_time) {
            log_printf(15, "write_netstream_block: ns=%d Command timed out! to=" TT " ct=" TT " \n", tbx_ns_getid(ns), end_time, apr_time_now());
            err = NS_TIMEOUT;
        }

        if (nbytes < 0) {
            err = nbytes;   //** Error with write
        } else if (nbytes > 0) {   //** Normal write
            pos = pos + nbytes;
            nleft = nleft - nbytes;
            err = NS_OK;
        }
    }

    log_printf(15, "write_netstream_block: END ns=%d size=%d nleft=%d nbytes=%d pos=%d\n", tbx_ns_getid(ns), size, nleft, nbytes, pos);

    return(err);
}

//*********************************************************************
//  tbx_ns_write_block - Same as write_netstream but blocks until the
//     data is sent or end_time is reached
//*********************************************************************

int tbx_ns_write_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, unsigned int boff, int bsize)
{
    return(_write_netstream_block(ns, end_time, buffer, boff, bsize, 1));
}

//*********************************************************************
//  _read_netstream_block - Same as read_netstream but blocks until the
//     data is sent or end_time is reached
//*********************************************************************

int _read_netstream_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, int pos, int size, int dolock)
{
    int nleft, nbytes, err;

    tbx_ns_timeout_t dt;

    tbx_ns_timeout_set(&dt, 1, 0);
    nleft = size;
    nbytes = -100;
    err = NS_OK;
    while ((nleft > 0) && (err == NS_OK)) {
        nbytes = _tbx_ns_read(ns, buffer, pos, nleft, dt, dolock);
        log_printf(15, "read_netstream_block: ns=%d size=%d nleft=%d nbytes=%d pos=%d time=" TT "\n",
                   tbx_ns_getid(ns), size, nleft, nbytes, pos, apr_time_now());

        if (apr_time_now() > end_time) {
            log_printf(15, "read_netstream_block: ns=%d Command timed out! to=" TT " ct=" TT " \n", tbx_ns_getid(ns), end_time, apr_time_now());
            err = NS_TIMEOUT;
        }

        if (nbytes < 0) {
            err = nbytes;   //** Error with write
        } else if (nbytes > 0) {   //** Normal write
            pos = pos + nbytes;
            nleft = nleft - nbytes;
            err = NS_OK;
        }
    }

    log_printf(15, "read_netstream_block: END ns=%d size=%d nleft=%d nbytes=%d pos=%d\n", tbx_ns_getid(ns), size, nleft, nbytes, pos);

    return(err);
}

//*********************************************************************
//  tbx_ns_read_block - Same as read_netstream but blocks until the
//     data is sent or end_time is reached
//*********************************************************************

int tbx_ns_read_block(tbx_ns_t *ns, apr_time_t end_time, tbx_tbuf_t *buffer, unsigned int boff, int bsize)
{
    return(_read_netstream_block(ns, end_time, buffer, boff, bsize, 1));
}

//*********************************************************************
//  scan_and_copy_netstream - Scans the input stream for "\n" or "\r"
//*********************************************************************

int scan_and_copy_stream(char *inbuf, int insize, char *outbuf, int outsize, int *finished)
{
    int max_char;
    int nbytes;

    *finished = 0;

    if (outsize > insize) {
        max_char = insize - 1;
    } else {
        max_char = outsize - 1;
    }

    if (max_char < 0) return(0);  //** Nothing to parse
    if (insize == 0) {
        return(0);
    }

    nbytes = -1;
    do {
        nbytes++;
        outbuf[nbytes] = inbuf[nbytes];
    } while ((outbuf[nbytes] != '\r') && (outbuf[nbytes] != '\n') && (nbytes < max_char));

    if ((outbuf[nbytes] == '\r') || (outbuf[nbytes] == '\n')) {
        *finished = 1;
    }

    log_printf(15, "scan_and_copy_stream: insize=%d outsize=%d  max_char=%d nbytes=%d finished=%d\n", insize, outsize, max_char, nbytes+1, *finished);

//   log_printf(0, "scan_and_copy_stream: insize=%d nbytes=%d buffer=!", insize, nbytes+1);
//   int i;
//   if (insize > nbytes+2) insize = nbytes+2;
//   for (i=0; i<insize; i++) log_printf(0, "%c", inbuf[i]);
//   log_printf(0, "!\n");

    return(nbytes+1);
}

//*********************************************************************

int _ns_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout)
{
    int nbytes, nleft, n;

    nleft = bsize;
    n = 0;
    do {
        nbytes = ns->read(ns->sock, buffer, boff+n, nleft, timeout);
        if (nbytes > 0) {
            nleft -= nbytes;
            n += nbytes;
        }
    } while ((nleft > 0) && (nbytes >= 0));

    return(((n==nbytes) ? n : nbytes));
}

//*********************************************************************
// _ns_encrypt_packet_read - Reads an encrypted packet
//*********************************************************************

int _ns_encrypt_packet_read(tbx_ns_t *ns, tbx_ns_timeout_t timeout)
{
    int nbytes, n;
    unsigned char tag;
    unsigned char *c = ns->enc->enc_packet_read_cipher;
    tbx_tbuf_t tb;

    //** Make sure the socket is working
    if (ns->sock_status(ns->sock) != 1) {
        log_printf(15, "connection closed!  ns=%d\n", ns->id);
        return(-1);
    }

    //** Get a fresh packet
    //** Read the size
    tbx_tbuf_single(&tb, N_ENCRYPT_PACKET_ENV_SIZE + N_ENCRYPT_PACKET_DATA_SIZE, (char *)c);
    n = _ns_read(ns, &tb, 0, N_ENCRYPT_INT_SIZE, timeout);
    if (n <= 0) return(n);  //** Kick out early if nothing is available or an error

    //** If we made it here we're commited to reading a full packet
    nbytes = c[0] + 256*c[1] + 256*256*c[2];
    if (nbytes > N_ENCRYPT_PACKET_DATA_SIZE) return(-1);

    //** And the packet
    n = _ns_read(ns, &tb, 0, nbytes + N_ENCRYPT_PACKET_ENV_SIZE, timeout);
    if (n <= 0) return(-1);

    //** Decrypt it
    if (crypto_secretstream_xchacha20poly1305_pull(&(ns->enc->state_read), ns->enc->enc_packet_read_buf, NULL, &tag, c, nbytes+N_ENCRYPT_PACKET_ENV_SIZE, NULL, 0) != 0) {
        log_printf(0, "ERROR: Bad encrypted packet! ns=%d\n", ns->id);
        return(-1);
    }

    return(nbytes);
}

//*********************************************************************
// _ns_encrypt_read - Reads data from an ecrypted stream
//*********************************************************************

int _ns_encrypt_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout)
{
    int n, err;
    tbx_tbuf_t ns_tb;

    //** See if there is data in the buffer already
    if (ns->enc->end > 0) {
        n = ns->enc->end - ns->enc->start + 1;
        tbx_tbuf_single(&ns_tb, n, (char *)&(ns->enc->enc_packet_read_buf[ns->enc->start]));
        if (n > bsize) {  //** Got plenty in the buffer
            tbx_tbuf_copy(&ns_tb, 0, buffer, boff, bsize, 1);
            ns->enc->start += bsize;
            n = bsize;
        } else {   //** Copy what we have and return
            tbx_tbuf_copy(&ns_tb, 0, buffer, boff, n, 1);
            ns->enc->start = 0;
            ns->enc->end = -1;
        }
        return(n);
    }

    //** See if we can get a packet
    n = _ns_encrypt_packet_read(ns, timeout);
    if (n <= 0) return(n);  //** Kick out on error or nothing to read

    //** Copy over some data
    tbx_tbuf_single(&ns_tb, N_ENCRYPT_PACKET_DATA_SIZE, (char *)ns->enc->enc_packet_read_buf);
    if (n > bsize) {
        err = tbx_tbuf_copy(&ns_tb, 0, buffer, boff, bsize, 0);
        if (err) return(-1);

        if (n > bsize) {   //** Anything else stays in the buffer for next time
            ns->enc->start = bsize;
            ns->enc->end = n - 1;
            n = bsize;
        }
    } else {
        err = tbx_tbuf_copy(&ns_tb, 0, buffer, boff, n, 0);
        if (err) return(-1);
    }

    return(n);
}

//*********************************************************************
// read_netstream - Reads characters from the stream with a max wait
//*********************************************************************

int _tbx_ns_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout, int dolock)
{
    int total_bytes, i;
    tbx_tbuf_t ns_tb;

    if (size == 0) return(0);

    if (dolock == 1) lock_read_ns(ns);

    if (ns->sock_status(ns->sock) != 1) {
        log_printf(15, "read_netstream: Dead connection!  ns=%d\n", ns->id);
        if (dolock == 1) unlock_read_ns(ns);
        return(-1);
    }

    if (ns_read_chksum_state(ns) == 1) {  //** We have chksumming enabled
        if (size > ns->read_chksum.bytesleft) {
            size = ns->read_chksum.bytesleft;  //** Truncate at the block
        }
    }

    //*** 1st grab anything currently in the network buffer ***
    if (ns->end >= ns->start) {
        i = ns->end - ns->start + 1;
        if (i>size) {
            total_bytes = size;
            tbx_tbuf_single(&ns_tb, size, &(ns->buffer[ns->start]));
            tbx_tbuf_copy(&ns_tb, 0, buffer, boff, size, 1);
            ns->start = ns->start + total_bytes;
        } else {
            total_bytes = i;
            tbx_tbuf_single(&ns_tb, i, &(ns->buffer[ns->start]));
            tbx_tbuf_copy(&ns_tb, 0, buffer, boff, i, 1);
            ns->start = 0;
            ns->end = -1;
        }
    } else {  //*** Now grab some data off the network port ****
        total_bytes = (ns->encrypted == 0) ? ns->read(ns->sock, buffer, boff, size, timeout) : _ns_encrypt_read(ns, buffer, boff, size, timeout);
    }

    ns->last_read = apr_time_now();

    if ((ns_read_chksum_state(ns) == 1) && (total_bytes > 0)) {  //** We have chksumming enabled
        tbx_chksum_add(&(ns->read_chksum.chksum), total_bytes, buffer, boff);

        ns->read_chksum.bytesleft -= total_bytes;
        if (ns->read_chksum.bytesleft <= 0) { //** Compare the chksums
            i = tbx_ns_chksum_read_flush(ns);
            if (i != 0) total_bytes = NS_CHKSUM;

            //** Reset the chksum
            ns->read_chksum.bytesleft = ns->read_chksum.blocksize;
            tbx_chksum_reset(&(ns->read_chksum.chksum));
        }
    }

    if (dolock == 1) unlock_read_ns(ns);

    tbx_monitor_obj_integer2(&(ns->mo_recv), total_bytes, size);

    return(total_bytes);
}

//*********************************************************************
// read_netstream - Reads characters fomr the stream with a max wait
//*********************************************************************

int tbx_ns_read(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout)
{
    return(_tbx_ns_read(ns, buffer, boff, size, timeout, 1));
}

//*********************************************************************
// tbx_ns_readline_raw - Performs an attempt to read a complete line
//    if it fails it returns the partial read
//*********************************************************************

int tbx_ns_readline_raw(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int size, tbx_ns_timeout_t timeout, int *status)
{
    tbx_tbuf_t ns_tb;
    int nbytes, total_bytes, i;
    int finished = 0;
    char *buf;
    *status = 0;

    if (boff > buffer->buf.iov[0].iov_len) {
        log_printf(0, "ERROR boff>iov_len!  boff=%d iov_len=" ST "\n", boff, buffer->buf.iov[0].iov_len);
        fprintf(stderr, "ERROR boff>iov_len!  boff=%d iov_len=" ST "\n", boff, buffer->buf.iov[0].iov_len);
        fprintf(stdout, "ERROR boff>iov_len!  boff=%d iov_len=" ST "\n", boff, buffer->buf.iov[0].iov_len);
        *status = -1;
        return(0);
    }

    buf = (buffer->buf.iov[0].iov_base + boff);

    //*** 1st grab anything currently in the network buffer ***
    total_bytes = 0;
    lock_read_ns(ns);
    i = ns->end - ns->start + 1;
    if (i > 0) {
        //** Assumes buffer has a single iovec element
        total_bytes = scan_and_copy_stream(&(ns->buffer[ns->start]), i, buf, size, &finished);
        ns->start = ns->start + total_bytes;
        if (ns->start > ns->end) {
            ns->start = 0;
            ns->end = -1;
        }

        if (finished == 1) {
            *status = 1;
            total_bytes--;
            buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string remove the \n
            debug_printf(15, "BUFFER ns=%d Command : %s * nbytes=%d\n", ns->id, buffer,total_bytes);
            flush_debug();
            unlock_read_ns(ns);
            return(total_bytes);
        }
    }
    unlock_read_ns(ns);

    //*** Now grab the data off the network port ****
    nbytes = 0;
    if (finished == 0) {
        tbx_tbuf_single(&ns_tb, N_BUFSIZE, ns->buffer);
        nbytes = tbx_ns_read(ns, &ns_tb, 0, N_BUFSIZE, timeout);  //**there should be 0 bytes in buffer now
        debug_printf(15, "ns=%d Command : !", ns->id);
        for (i=0; i< nbytes; i++) debug_printf(15, "%c", ns->buffer[i]);
        debug_printf(15, "! * nbytes =%d\n", nbytes);
        flush_debug();

        if (nbytes > 0) {
            //** Assumes buffer has a single iovec element
            lock_read_ns(ns);
            i = scan_and_copy_stream(ns->buffer, nbytes, &(buf[total_bytes]), size-total_bytes, &finished);
            unlock_read_ns(ns);
            total_bytes += i;
        }
    }

    buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string

    if (finished == 1) {  //*** Push the unprocessed characters back onto the stream buffer ****
        *status = 1;
        total_bytes--;
        buf[total_bytes] = '\0';   //** Make sure and NULL terminate the string remove the \n
        lock_read_ns(ns);
        ns->start = i;
        ns->end = nbytes-1;
        if (ns->start > ns->end) {
            ns->start = 0;
            ns->end = -1;
        }
        unlock_read_ns(ns);

        debug_printf(15, "readline_stream_raw: ns=%d Command : %s * nbytes=%d\n", ns->id, buffer,total_bytes);
        flush_debug();
    } else if (nbytes == -1) {  //** Socket error
        *status = -1;
        debug_printf(15, "readline_stream_raw: Socket error! ns=%d nbytes=%d  buffer=%s\n", ns->id, total_bytes, buffer);
        flush_debug();
        return(0);
    } else {       //*** Not enough space in input buffer
        *status = 0;
        lock_read_ns(ns);
        ns->start = i;
        ns->end = nbytes-1;
        if (ns->start > ns->end) {
            ns->start = 0;
            ns->end = -1;
        }
        unlock_read_ns(ns);
        debug_printf(15, "Out of buffer space or nothing read! ns=%d nbytes=%d  buffer=%s\n", ns->id, total_bytes, buffer);
        flush_debug();
    }

    return(total_bytes);
}

//*********************************************************************
// tbx_ns_readline - Reads a line of text from the stream
//*********************************************************************

int tbx_ns_readline(tbx_ns_t *ns, tbx_tbuf_t *buffer, unsigned int boff, int bsize, tbx_ns_timeout_t timeout)
{
    int status;
    int n = tbx_ns_readline_raw(ns, buffer, boff, bsize, timeout, &status);

//log_printf(15, "readline_netstream: ns=%d status=%d\n", tbx_ns_getid(ns), status);
    if (status == 1) {
        n = 0;
    } else if (status == -1) {
        n = -1;
    } else if (status == 0) {
        n = 1;
    }

    return(n);
}

//*********************************************************************
//  tbx_network_accept_pending_connection - Accepts a pending connection and stores
//    it in the provided ns.  The ns should be uninitialize, ie closed
//    since the sock structure is inherited from the server ports
//    ns type
//*********************************************************************

int tbx_network_accept_pending_connection(tbx_network_t *net, tbx_ns_t *ns)
{
    int i, j, k, err;
    tbx_ns_monitor_t *nm = NULL;

    //** Get the global settings
    apr_thread_mutex_lock(net->ns_lock);

    err = 0;
    //** Find the port.  Make sure and use the next port in the list
    j = -1;
    k = net->monitor_index % net->used_ports;
    for (i=0; i<net->used_ports; i++) {
        k = (i + net->monitor_index) % net->used_ports;
        nm = &(net->nm[k]);
        apr_thread_mutex_lock(nm->lock);
        if (nm->is_pending == 1) {   //** Found a slot
            j = k;
            break;
        }
        apr_thread_mutex_unlock(nm->lock);
    }

    net->monitor_index = (k + 1) % net->used_ports;

    //** Check if there is nothing to do.
    if (j == -1) {
        apr_thread_mutex_unlock(net->ns_lock);
        return(1);
    }

    ns_clone(ns, nm->ns);  //** Clone the settings
    ns->nm = nm;           //** Specify the bind accepted

    ns->sock = nm->ns->accept(nm->ns->sock);   //** Accept the connection
    if (ns->sock == NULL) err = 1;

    nm->is_pending = 0;                  //** Clear the pending flag
    net->accept_pending--;
    if (net->accept_pending < 0) net->accept_pending = 0;
    apr_thread_mutex_unlock(net->ns_lock);

    apr_thread_cond_signal(nm->cond);    //** Wake up the pending monitor thread
    apr_thread_mutex_unlock(nm->lock);   //** This was locked in the fop loop above

    if (err == 0) {
        ns->id = tbx_ns_generate_id();
        ns->set_peer(ns->sock, ns->peer_address, sizeof(ns->peer_address));

        log_printf(10, "accept_pending_connection: Got a new connection from %s! Storing in ns=%d \n", ns->peer_address, ns->id);

        err = _ns_encrypt_server_handshake(ns);  //** See if we need to encrypt the channel
        _ns_monitor_create(ns, 0, "accept");
        tbx_monitor_obj_message(&ns->mo_recv, "accepted nsid=%d", ns->id);
    } else {
        log_printf(10, "accept_pending_connection: Failed getting a new connection\n");
    }

    return(err);
}


//*********************************************************************
// wait_for_connection - Waits for a new connection
//*********************************************************************

int tbx_network_wait_for_connection(tbx_network_t *net, int max_wait)
{
    apr_time_t t;
    apr_time_t end_time = apr_time_now() + apr_time_make(max_wait, 0);
    int n;

    log_printf(15, "wait_for_connection: max_wait=%d starttime=" TT " endtime=" TT "\n", max_wait, apr_time_now(), end_time);
    apr_thread_mutex_lock(net->ns_lock);

    log_printf(15, "wait_for_connection: accept_pending=%d\n", net->accept_pending);

    while ((end_time > apr_time_now()) && (net->accept_pending == 0)) {
//    log_printf(15, "wait_for_connection: accept_pending=%d time=" TT "\n", net->accept_pending, apr_time_now());
        tbx_ns_timeout_set(&t, 1, 0);  //** Wait for at least 1 second
        apr_thread_cond_timedwait(net->cond, net->ns_lock, t);
    }

    log_printf(15, "wait_for_connection: exiting loop accept_pending=%d time=" TT "\n", net->accept_pending, apr_time_now());

    n = net->accept_pending;
//  net->accept_pending--;
//  if (net->accept_pending < 0) net->accept_pending = 0;
    apr_thread_mutex_unlock(net->ns_lock);

    return(n);
}

//*********************************************************************
// tbx_network_wakeup - Wakes up the network monitor thread
//*********************************************************************

void tbx_network_wakeup(tbx_network_t *net)
{
    apr_thread_mutex_lock(net->ns_lock);
    net->accept_pending++;
    apr_thread_cond_signal(net->cond);
    apr_thread_mutex_unlock(net->ns_lock);
}

