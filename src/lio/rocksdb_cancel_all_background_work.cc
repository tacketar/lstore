//**************************************************************************
//  Simple throwaway shim to make the CancelAllBackgroundWork() call
//  available to C.  In newer version of RockDB this is already done
//**************************************************************************


#include <rocksdb/c.h>
#include <rocksdb/db.h>
#include <rocksdb/convenience.h>

using rocksdb::DB;

extern "C" {

struct rocksdb_t { DB* rep; };

void rocksdb_cancel_all_background_work(rocksdb_t* db, unsigned char wait)
{
    CancelAllBackgroundWork(db->rep, wait);
}

}
