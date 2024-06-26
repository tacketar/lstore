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


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include <tbx/fmttypes.h>
#include <tbx/direct_io.h>
#include <tbx/log.h>
#include <tbx/io.h>


//***********************************************************************
// tbx_dio_init - Preps the FD for direct I/O.  It returns the initial
//    for restoring if you want
//***********************************************************************

int tbx_dio_init(FILE *fd)
{
    int flags, flags_new;

    flags = tbx_io_fcntl(fileno(fd), F_GETFL);
    flags_new = flags |  O_DIRECT;
    tbx_io_fcntl(fileno(fd), F_SETFL, flags_new);  //** Enable direct I/O

    return(flags);
}

//***********************************************************************
// tbx_dio_finish - Unsets the O_DIRECT flag and adjusts the FD position
//    If flags == 0 they aren't  changed.  Just O_DIRECT is disabled
//***********************************************************************

void tbx_dio_finish(FILE *fd, int flags)
{
    if (flags == 0) {
        flags = tbx_io_fcntl(fileno(fd), F_GETFL);
        flags |=  O_DIRECT;
    }

    tbx_io_fcntl(fileno(fd), F_SETFL, flags);  //** Restore the original flags
}

//***********************************************************************
// _base_read - Uses either normal or O_DIRECT if possible to read data
//    if offset == -1 then command acts like fread
//***********************************************************************

ssize_t _base_read(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset, int dio_enable)
{
    ssize_t n, ntotal, nleft;
    int nfd, flags, old_flags;

    nfd = fileno(fd);

    ntotal = 0;
    nleft = nbytes;
    do {
        //** Try and do the I/O with direct I/O
        n = (offset != -1) ? tbx_io_pread(nfd, buf + ntotal, nleft, offset + ntotal) : tbx_io_read(nfd, buf + ntotal, nleft);
        log_printf(5, "nfd=%d nleft=" SST " got=" SST " ntotal=" SST " lseek=" SST " offset=" SST " errno=%d\n", nfd, nleft, n, ntotal, tbx_io_lseek(nfd, 0L, SEEK_CUR), offset, errno);
        if ((n == -1) && (dio_enable == 1)) {  //** Got an error so try without Direct I/O
            old_flags = tbx_io_fcntl(nfd, F_GETFL);
            flags = old_flags ^ O_DIRECT;
            tbx_io_fcntl(nfd, F_SETFL, flags);

            n = (offset != -1) ? tbx_io_pread(nfd, buf + ntotal, nleft, offset + ntotal) : tbx_io_read(nfd, buf + ntotal, nleft);
            log_printf(5, "RETRY nfd=%d nleft=" SST " got=" SST " ntotal=" SST " lseek=" SST " offset=" SST " errno=%d\n", nfd, nleft, n, ntotal, tbx_io_lseek(nfd, 0L, SEEK_CUR), offset, errno);

            tbx_io_fcntl(nfd, F_SETFL, old_flags);
        }

        if (n > 0) {
            ntotal += n;
            nleft -=n;
        }
    } while ((n > 0) && (nleft > 0));

    if (ntotal == 0) ntotal = n;  //** Got an error

    log_printf(5, "END nfd=%d ntotal=" SST "\n", nfd, ntotal);
    return(ntotal);
}


//***********************************************************************
// tbx_dio_read - Uses O_DIRECT if possible to read data
//    if offset == -1 then command acts like fread
//***********************************************************************

ssize_t tbx_dio_read(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    return(_base_read(fd, buf, nbytes, offset, 1));
}

//***********************************************************************
// tbx_normal_read - Does a normal read op
//    if offset == -1 then command acts like fread
//***********************************************************************

ssize_t tbx_normal_read(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    return(_base_read(fd, buf, nbytes, offset, 0));
}

//***********************************************************************
// _base_write - Uses either a normal or O_DIRECT if possible op to write data
//    if offset == -1 then command acts like fwrite
//***********************************************************************

ssize_t _base_write(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset, int dio_enable)
{
    ssize_t n, ntotal, nleft;
    int nfd, flags, old_flags;

    nfd = fileno(fd);

    ntotal = 0;
    nleft = nbytes;
    do {
        //** Try and do the I/O with direct I/O
        n = (offset != -1) ? tbx_io_pwrite(nfd, buf + ntotal, nleft, offset + ntotal) : tbx_io_write(nfd, buf + ntotal, nleft);
        log_printf(5, "nfd=%d nleft=" SST " got=" SST " ntotal=" SST " lseek=" SST " offset=" SST " errno=%d\n", nfd, nleft, n, ntotal, tbx_io_lseek(nfd, 0L, SEEK_CUR), offset, errno);
        if ((n == -1) && (dio_enable == 1)) {  //** Got an error so try without Direct I/O
            old_flags = tbx_io_fcntl(nfd, F_GETFL);
            flags = old_flags ^ O_DIRECT;
            tbx_io_fcntl(nfd, F_SETFL, flags);

            n = (offset != -1) ? tbx_io_pwrite(nfd, buf + ntotal, nleft, offset + ntotal) : tbx_io_write(nfd, buf + ntotal, nleft);
            log_printf(5, "RETRY nfd=%d nleft=" SST " got=" SST " ntotal=" SST " lseek=" SST " offset=" SST " errno=%d\n", nfd, nleft, n, ntotal, tbx_io_lseek(nfd, 0L, SEEK_CUR), offset, errno);

            tbx_io_fcntl(nfd, F_SETFL, old_flags);
        }

        if (n > 0) {
            ntotal += n;
            nleft -=n;
        }
    } while ((n > 0) && (nleft > 0));

    if (ntotal == 0) ntotal = n;  //** Got an error

    log_printf(5, "END nfd=%d ntotal=" SST "\n", nfd, ntotal);
    return(ntotal);
}

//***********************************************************************
// tbx_dio_write - Uses O_DIRECT if possible to write data
//    if offset == -1 then command acts like fwrite
//***********************************************************************

ssize_t tbx_dio_write(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    return(_base_write(fd, buf, nbytes, offset, 1));
}

//***********************************************************************
// tbx_normal_write - Does a normal write op
//    if offset == -1 then command acts like fwrite
//***********************************************************************

ssize_t tbx_normal_write(FILE *fd, char *buf, ssize_t nbytes, ssize_t offset)
{
    return(_base_write(fd, buf, nbytes, offset, 0));
}
