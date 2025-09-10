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

#define _log_module_index 211

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include <lio/lio.h>
#include <lio/lio_fuse.h>

// *************************************************************************

int launch_fuse_lowlevel(int fuse_argc, char **fuse_argv, struct fuse_lowlevel_ops *ll_ops, lio_fuse_init_args_t *lfs_args)
{
    int err;
	struct fuse_session *se;
	struct fuse_args args = FUSE_ARGS_INIT(fuse_argc, fuse_argv);
	struct fuse_cmdline_opts opts;
	struct fuse_loop_config *config;

	if (fuse_parse_cmdline(&args, &opts) != 0) return(1);

	se = fuse_session_new(&args, ll_ops, sizeof(struct fuse_lowlevel_ops), lfs_args);
	if (se == NULL) goto err_out1;

	if (fuse_set_signal_handlers(se) != 0) goto err_out2;

	if (fuse_session_mount(se, opts.mountpoint) != 0) goto err_out3;

	fuse_daemonize(opts.foreground);

	/* Block until ctrl+c or fusermount -u */
	if (opts.singlethread)
		err = fuse_session_loop(se);
	else {
		config = fuse_loop_cfg_create();
		fuse_loop_cfg_set_max_threads(config, opts.max_threads);
		err = fuse_session_loop_mt(se, config);
		fuse_loop_cfg_destroy(config);
		config = NULL;
	}

	fuse_session_unmount(se);
err_out3:
	fuse_remove_signal_handlers(se);
err_out2:
	fuse_session_destroy(se);
err_out1:
	free(opts.mountpoint);
	fuse_opt_free_args(&args);

	return(err);
}

// *************************************************************************
// *************************************************************************

void print_usage(void)
{
    printf("\n"
           "lio_fuse mount_point [FUSE_OPTIONS] [--lio LIO_COMMON_OPTIONS] [--disable-flock]\n"
           "    --disable-flock           Disable LStore integrated flock() functionality\n"
           "    --api-low                 Use the FUSE low-level API. Default if LServer supported\n"
           "    --api-high                Use the FUSE high-level API\n");
    lio_print_options(stdout);
    printf("    FUSE_OPTIONS:\n"
           "       -h   --help            print this help\n"
           "       -ho                    print FUSE mount options help\n"
           "       -V   --version         print FUSE version\n"
           "       -d   -o debug          enable debug output (implies -f)\n"
           "       -s                     disable multi-threaded operation\n"
           "       -f                     foreground operation\n"
           "                                (REQUIRED unless  '-c /absolute/path/lio.cfg' is specified and all included files are absolute paths)\n"
           "       -o OPT[,OPT...]        mount options\n"
           "                                (for possible values of OPT see 'man mount.fuse' or see 'lio_fuse -o -h')\n");
}

// *************************************************************************

int main(int argc, char **argv)
{
    int err = -1;
    int idx;
    lio_fuse_init_args_t lio_args;
    int fuse_argc;
    char **fuse_argv;

    if (argc < 2 || strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
        print_usage();
        return(1);
    }

    // ** split lio and fuse arguments **


    // these defaults hold if --lio is not used on the commandline
    fuse_argc = argc;
    fuse_argv = argv;
    lio_args.use_lowlevel_api = 1;   // ** Defail to useing the Low-Level FUSE API if LServer supports it
    lio_args.lio_argc = 1;
    lio_args.lio_argv = argv;
    lio_args.mount_point = argv[1];

    for (idx=1; idx<argc; idx++) {
        if (strcmp(argv[idx], "--disable-flock") == 0) {
            lfs_fops.flock = NULL;  // ** Have to disable that before fuse_main since it gets copied before lfs_init() is called
            lfs_ll_ops.flock = NULL;
            lfs_ll_ops.getlk = NULL;
            lfs_ll_ops.setlk = NULL;
        } else if (strcmp(argv[idx], "--api-high") == 0) {
            lio_args.use_lowlevel_api = 0;
        }
        if (strcmp(argv[idx], "--lio") == 0) {
            fuse_argc = idx;
            lio_args.mount_point = argv[fuse_argc-1];  // ** The last FUSE argument is the mount point
            lio_args.lio_argc = argc - idx;
            lio_args.lio_argv = &argv[idx];
            lio_args.lio_argv[0] = argv[0]; //replace "--lio" with the executable name because the parser may reasonably expect the zeroth argument to be the program name
        }
    }

    umask(0);

    if (lio_args.use_lowlevel_api == 1) {
        err = launch_fuse_lowlevel(fuse_argc, fuse_argv, &lfs_ll_ops, &lio_args);
    } else {
        err = fuse_main(fuse_argc, fuse_argv, &lfs_fops, &lio_args /* <- stored to fuse's ctx->private_data*/);
    }

    return(err);
}
