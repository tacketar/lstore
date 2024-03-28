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

//***********************************************************************
//  tcmu_lstore - TCMU module supporting the use of LStore files as targets
//***********************************************************************

#define _GNU_SOURCE
#include <stdio.h>
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <endian.h>
#include <errno.h>
#include <scsi/scsi.h>

#include "libtcmu.h"
#include "tcmur_device.h"
#include "tcmu-runner.h"

//#include <gop/gop.h>
#include <lio/lio.h>
#include <lio/fs.h>
#include <tbx/atomic_counter.h>
#include <tbx/fmttypes.h>
#include <tbx/type_malloc.h>
#include "visibility.h"


// **SCSI additional sense codes.  from file_zbc.c in tcmu-runner source
#define ASC_READ_ERROR                          0x1100
#define ASC_WRITE_ERROR                         0x0C00

TCMUL_API int handler_init(void);

typedef struct {
   lio_fd_t *fd;
} tcmu_lstore_t;

lio_fs_t *fs = NULL;  //** This is the FS handle used for all devices

//***********************************************************************
//  lstore_activate - Start LIO subsystem when loading the shared library
//***********************************************************************

static int lstore_activate()
{
    int argc = 1;
    char **argv = malloc(sizeof(char *)*argc);
    argv[0] = "lio_tcmu";

fprintf(stderr, "AFTER apr_init\n"); fflush(stderr);
    //** Do the normal init stuff
    lio_init(&argc, &argv);
    free(argv);
    if (!lio_gc) {
        log_printf(-1,"Failed to load LStore\n");
        tcmu_err("Failed to load LStore\n");
        exit(1);
    }


    //** Create the default file system handler
    fs = lio_fs_create(lio_gc->ifd, "lfs", lio_gc, getuid(), getgid());
    if (!fs) {
        fprintf(stderr, "ERROR: Unable to create the default fs!\n");
        exit(2);
    }

    log_printf(0,"Loaded\n");

    tbx_log_flush();

    return(0);
}

//***********************************************************************
// lstore_deactivate - Shutdown LStore
//***********************************************************************

static void lstore_deactivate()
{

fprintf(stderr, "lstore_deactivate: START\n"); fflush(stderr);

    if (fs) lio_fs_destroy(fs);

fprintf(stderr, "lstore_deactivate: BEFORE shutdown\n"); fflush(stderr);
    lio_shutdown();

fprintf(stderr, "lstore_deactivate: END\n"); fflush(stderr);

//    apr_terminate();

}

//***********************************************************************
//  cfgstring2fname - Takes the TCMU config string converts it to a filename
//***********************************************************************

const char *cfgstring2fname(const char *cfgstring, char **reason)
{
	char *fname;

    fname = strchr(cfgstring, '/');
    if (!fname) {
        if (reason != NULL) {
            if (asprintf(reason, "No path found") == -1) *reason = NULL;
        }
        return(NULL);
    }
    fname += 1; //** get past '/'

    return(fname);
}

//***********************************************************************
//  lstore_check_config - Verify config is valid
//***********************************************************************

static bool lstore_check_config(const char *cfgstring, char **reason)
{
	const char *fname;
    int ftype;

    fname = cfgstring2fname(cfgstring, reason);
    if (!fname) {
        return false;
    }

    ftype = lio_fs_exists(fs, fname);
    if (ftype == 0) {
        asprintf(reason, "Target file does not exists. target:%s\n", fname);
        return false;
    } else if ((ftype & OS_OBJECT_FILE_FLAG) == 0) {
        asprintf(reason, "Target file is not a faile. target:%s\n", fname);
        return false;
    }

    return true;
}

//***********************************************************************
//  lstore_open - Opens an existing target file
//***********************************************************************

static int lstore_open(struct tcmu_device *dev, bool reopen)
{
    tcmu_lstore_t *ctx;
    int64_t size;
    const char *fname;
    int block_size, lbas;
    int return_code;

    tbx_type_malloc_clear(ctx, tcmu_lstore_t, 1);
    return_code = 0;

    tcmur_dev_set_private(dev, ctx);

    fname = cfgstring2fname(tcmu_dev_get_cfgstring(dev), NULL);
    if (!fname) {
        tcmu_err("no configuration found in cfgstring\n");
        return_code = -EINVAL;
        goto failed;
    }

    ctx->fd = lio_fs_open(fs, NULL, fname, lio_fopen_flags("r+"));
    if (ctx->fd == NULL) {
        tcmu_err("Failed opening target!\n");
        return_code = -EREMOTEIO;
        goto failed;
    }

    return(return_code);  //** Exit

    //** If we make it here there was an error
failed:
    free(ctx);
    return(return_code);
}

//***********************************************************************
// lstore_close - Close the target
//***********************************************************************

static void lstore_close(struct tcmu_device *dev)
{
    tcmu_lstore_t *ctx = tcmur_dev_get_private(dev);

    lio_fs_close(fs, ctx->fd);
    free(ctx);
}

//***********************************************************************
//  lstore_readv - Read data from the target
//***********************************************************************

static int lstore_readv(struct tcmu_device *dev, struct tcmur_cmd *rcmd,
               struct iovec *iov, size_t iov_cnt, size_t length, off_t offset)
{
    tcmu_lstore_t *ctx = tcmur_dev_get_private(dev);
    struct tcmulib_cmd *cmd = rcmd->lib_cmd;
    int ret, nbytes;

    nbytes = lio_fs_readv(fs, ctx->fd, iov, iov_cnt, offset);
    if ((unsigned int)nbytes != length) {
        log_printf(1, "ERROR READ n_iov=" ST " offset=" OT " len=" ST " nbytes=%d\n", iov_cnt, offset, length, nbytes);
        tcmu_err("read failed: %m\n");
        ret = tcmu_sense_set_data(cmd->sense_buf, MEDIUM_ERROR, ASC_READ_ERROR);
    } else {
        ret = TCMU_STS_OK;
    }

    return(ret);
}

//***********************************************************************
//  lstore_writev - Read data from the target
//***********************************************************************

static int lstore_writev(struct tcmu_device *dev, struct tcmur_cmd *rcmd,
		     struct iovec *iov, size_t iov_cnt, size_t length,
		     off_t offset)
{
    tcmu_lstore_t *ctx = tcmur_dev_get_private(dev);
    struct tcmulib_cmd *cmd = rcmd->lib_cmd;
    int nbytes, ret;

    nbytes = lio_fs_writev(fs, ctx->fd, iov, iov_cnt, offset);

    if ((unsigned int)nbytes != length) {
        log_printf(1, "ERROR WRITE n_iov=" ST " offset=" OT " len=" ST " nbytes=%d\n", iov_cnt, offset, length, nbytes);
        tcmu_err("read failed: %m\n");
        ret = tcmu_sense_set_data(cmd->sense_buf, MEDIUM_ERROR, ASC_READ_ERROR);
    } else {
        ret = TCMU_STS_OK;
    }

    return(ret);
}

//***********************************************************************
// lstore_flush - Flush data to backing store
//***********************************************************************

static int lstore_flush(struct tcmu_device *dev, struct tcmur_cmd *rcmd)
{
    tcmu_lstore_t *ctx = tcmur_dev_get_private(dev);
    struct tcmulib_cmd *cmd = rcmd->lib_cmd;
    int err, ret;

    err = lio_fs_flush(fs, ctx->fd);
    if (err != 0) {
        tcmu_err("Flush failed\n");
        ret = tcmu_sense_set_data(cmd->sense_buf, MEDIUM_ERROR, ASC_WRITE_ERROR);
    } else {
    	ret = TCMU_STS_OK;
    }

    return(ret);
}

//***********************************************************************

static const char lstore_cfg_desc[] =
    "The path for the LStore file to use as a backstore.";

static struct tcmur_handler lstore_handler = {
    .cfg_desc = lstore_cfg_desc,

    .check_config = lstore_check_config,

    .open = lstore_open,
    .close = lstore_close,
    .read = lstore_readv,
    .write = lstore_writev,
    .flush = lstore_flush,
    .name = "LStore Handler",
    .subtype = "lstore",
    .nr_threads = 128,
    .init = lstore_activate,
    .destroy = lstore_deactivate
};

//***********************************************************************
// Entry point must be named "handler_init"
//***********************************************************************

int handler_init(void)
{
    return tcmur_register_handler(&lstore_handler);
}
