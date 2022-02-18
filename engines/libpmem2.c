/*
 * libpmem2: IO engine that uses PMDK libpmem2 to read and write data
 *
 * Copyright (C) 2017 Nippon Telegraph and Telephone Corporation.
 * Copyright 2018-2022, Intel Corporation
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License,
 * version 2 as published by the Free Software Foundation..
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 */

/*
 * libpmem2 engine
 *
 * IO engine that uses libpmem2 to write data (and memcpy to read).
 * It requires PMDK >= 1.12
 * Additionally, in order to use asynchronous features libminiasync
 * has to be installed.
 *
 * To use:
 *   ioengine=libpmem2
 *
 * Other relevant settings:
 *   iodepth=n - no major impact on synchronous mode, in async mode
 *   defines maximum count of memcpy operations to be run simultaneously.
 *
 *   direct=1 - PMEM2_F_MEM_NONTEMPORAL flag is set in pmem2_memcpy_fn call.
 *   direct=0 - PMEM2_F_MEM_TEMPORAL flag is set in pmem2_memcpy_fn call.
 *
 *   sync=0 - PMEM2_F_MEM_NODRAIN flag is set in pmem2_memcpy_fn call.
 *
 *   directory=/mnt/pmem0/ - directory for benchmark files, should be
 *   on a dax filesystem.
 *
 *   bs=4k - size of memcpy operation.
 *
 *   sync_file_range=write:n - pmem_persist_fn is executed
 *   after n sequential write operations.
 *
 *   async=libpmem2-mover|libminiasync-dml|libminiasync-threads - specify asynchronous
 *   mode of performing operations. If not specified, the engine works in synchronous
 *   mode. All modes require libminiasync to work properly and
 *   libminiasync-dml also requires libminiasync-dml library.
 *
 *   iodepth_batch=n - how many operations should be submitted at once.
 *
 *   iodepth_batch_complete_min=n - minimum number of operations that has to be
 *   completed in any getevents call. That is, the operations will be polled
 *   in a loop as long as count of finished operations will be less than n.
 *
 *   mover_threads=n - option for libminiasync-threads mover specifying how many
 *   threads should be created of the instance of libminiasync vdm.
 *
 *   ringbuf_size=m - option for libminiasync-threads mover specifying the
 *   size of ringbuffer observed by worker threads where the memcpy operations
 *   to be performed go.
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include "../fio.h"
#include "../verify.h"
#include "../optgroup.h"

#ifdef CONFIG_LIBMINIASYNC
	#include <libminiasync.h>
	#define PMEM2_USE_MINIASYNC 1
	#ifdef CONFIG_VDM_DML
		#include <libminiasync-vdm-dml.h>
	#endif
#endif

#include <libpmem2.h>

struct libpmem2_fio_options_values {
    void *pad;
    char *async;
    unsigned int thread_count;
    unsigned int ringbuf_size;
};

struct fio_option libpmem2_fio_options[] = {
	{
		.name	= "async",
		.lname	= "asynchronous_data_write",
		.type	= FIO_OPT_STR_STORE,
		.off1	= offsetof(struct libpmem2_fio_options_values, async),
		.def 	= "",
		.help	= "Which engine should be used for libpmem2 asynchronous operations"
				"libpmem2-mover|libminiasync-dml|libminiasync-threads",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBPMEM2,
	},
	{
		.name	= "mover_threads",
		.lname	= "data_mover_threads thread count",
		.type	= FIO_OPT_INT,
		.off1	= offsetof(struct libpmem2_fio_options_values, thread_count),
		.def 	= "2",
		.help	= "How many threads should be created by data_mover_threads"
				"(async=libminiasync-threads)",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBPMEM2,
	},
	{
		.name	= "ringbuf_size",
		.lname	= "data_mover_threads ringbuffer size",
		.type	= FIO_OPT_INT,
		.off1	= offsetof(struct libpmem2_fio_options_values, ringbuf_size),
		.def 	= "32768",
		.help	= "How big ringbuffer should be created by data_mover_threads(async=3)"
				"(async=libminiasync-threads)",
		.category = FIO_OPT_C_ENGINE,
		.group	= FIO_OPT_G_LIBPMEM2,
	},
	{
		.name	= NULL,
	},
};

struct fio_libpmem2_data {
	void *libpmem2_ptr;
	size_t libpmem2_sz;
	off_t libpmem2_off;
	pmem2_memcpy_fn memcpy;
	pmem2_persist_fn persist;
	struct pmem2_config *cfg;
	struct pmem2_map *map;
	struct pmem2_source *src;

	/* Data for async operations */
	unsigned int async;
#ifdef CONFIG_LIBMINIASYNC
	struct vdm *vdm;
	struct runtime *r;
	/*
	 * Array of futures that were not synchronised yet.
	 * It is limited by MAX_FUTURES_COUNT, so in fio job parameter
	 * sync_file_range=str:n, the n must not exceed MAX_FUTURES_COUNT
	 */
	struct pmem2_future *futs;
	size_t futs_count;

	/* Data for fio events */
	/*
	 * Array of pointers to queued io_u that may not be completed yet
	 */
	struct io_u **queued_io_us;
	/*
	 * Array of event pointers filled by fio_libpmem2_async_getevents with
	 * committed io_u events pointers.
	 */
	struct io_u **event_io_us;
#endif
};

static int fio_libpmem2_init(struct thread_data *td) {
	struct thread_options *o = &td->o;
	struct fio_libpmem2_data *fdd;
	struct libpmem2_fio_options_values *lo = td->eo;

	dprint(FD_IO, "o->rw_min_bs %llu\n o->fsync_blocks %u\n o->fdatasync_blocks %u\n",
		o->rw_min_bs, o->fsync_blocks, o->fdatasync_blocks);
	dprint(FD_IO, "DEBUG fio_libpmem2_async_init\n");

	fdd = calloc(1, sizeof(*fdd));
	if (fdd == NULL)
		return 1;

	/*
	 * If async is set, we must prepare virtual data mover instance
	 */
	if(lo->async!=NULL&&strcmp(lo->async,"")!=0) {
		dprint(FD_IO, "DEBUG using async operations with libpmem2\n");

        if(strcmp(lo->async,"libpmem2-mover")==0) {
            fdd->async = 1;
        } else if (strcmp(lo->async,"libminiasync-dml")==0) {
            fdd->async = 2;
        } else if (strcmp(lo->async,"libminiasync-threads")==0) {
            fdd->async = 3;
        }

#ifdef CONFIG_LIBMINIASYNC
		fdd->futs_count = 0;
		fdd->futs = malloc(o->iodepth * sizeof(*fdd->futs));
		fdd->queued_io_us = calloc(o->iodepth, sizeof(*fdd->queued_io_us));
		fdd->event_io_us = calloc(o->iodepth, sizeof(*fdd->event_io_us));

		if(fdd->async == 3) {
			dprint(FD_IO, "DEBUG using vdm threads miniasync mover\n");
			fdd->vdm = data_mover_threads_get_vdm(
				data_mover_threads_new(lo->thread_count, lo->ringbuf_size, FUTURE_NOTIFIER_NONE));
		} else if(fdd->async == 2) {
#ifdef CONFIG_VDM_DML
			dprint(FD_IO, "DEBUG using vdm dml miniasync mover\n");
			fdd->vdm = data_mover_dml_get_vdm(
				data_mover_dml_new(DATA_MOVER_DML_HARDWARE));
#endif
		} else if(fdd->async == 1) {
			dprint(FD_IO, "DEBUG using pmem2_memcpy_async\n");
		}
#endif
	}

	td->io_ops_data = fdd;
	return 0;
}

static int fio_libpmem2_unmap(struct fio_libpmem2_data *fdd)
{
	return
		pmem2_map_delete(&fdd->map) |
		pmem2_source_delete(&fdd->src) |
		pmem2_config_delete(&fdd->cfg);
}

static void fio_libpmem2_fini(struct thread_data *td) {
	/*
	 * Perform cleanup of data created with init
	 */
	struct fio_libpmem2_data *fdd = td->io_ops_data;
	dprint(FD_IO, "DEBUG fio_libpmem_fini\n");
	if (fdd->libpmem2_ptr) {
		fio_libpmem2_unmap(fdd);
	}
	if(fdd->async != 0) {
#ifdef CONFIG_LIBMINIASYNC
		free(fdd->futs);
		free(fdd->queued_io_us);
		free(fdd->event_io_us);
		if(fdd->async == 3) {
			data_mover_threads_delete((struct data_mover_threads *) fdd->vdm);
		} else if(fdd->async == 2) {
#ifdef CONFIG_VDM_DML
			data_mover_dml_delete((struct data_mover_dml *) fdd->vdm);
#endif
		}

		free(fdd);
#endif
	}
}

static int fio_libpmem2_open_file(struct thread_data *td, struct fio_file *f)
{
	int ret = 0;

	dprint(FD_IO, "DEBUG fio_libpmem_open_file\n");

	ret &= generic_open_file(td, f);

	FILE_SET_ENG_DATA(f, td->io_ops_data);

	return ret;
}

static int fio_libpmem2_close_file(struct thread_data *td, struct fio_file *f)
{
	struct fio_libpmem2_data *fdd = FILE_ENG_DATA(f);
	int ret = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_close_file\n");

	if (fdd->libpmem2_ptr)
		ret = fio_libpmem2_unmap(fdd);
	if (fio_file_open(f))
		ret &= generic_close_file(td, f);

	/*
	 * Clear old data from fdd, because fdd is created once for all files
	 */
	fdd->libpmem2_ptr = NULL;
	fdd->libpmem2_sz = 0;
	fdd->libpmem2_off = 0;
	if(fdd->async) {
#ifdef CONFIG_LIBMINIASYNC
		fdd->futs_count = 0;
		memset(fdd->queued_io_us, 0, td->o.iodepth * sizeof(*fdd->queued_io_us));
#endif
	}
	return ret;
}

static int fio_libpmem2_map_file_completely(struct thread_data *td,
		struct fio_file *f, struct fio_libpmem2_data *fdd)
{
	int ret = 0;
	int flags = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_map_file_completely\n");
	/*
	 * If there was any mapping before, we unmap it
	 */
	if (fdd->libpmem2_ptr) {
		if (fio_libpmem2_unmap(fdd) != 0) {
			ret = 1;
			goto failed_no_allocations;
		}
		fdd->libpmem2_ptr = NULL;
	}
	/*
	 * Prepare configuration for a new mapping
	 */
	if (pmem2_config_new(&fdd->cfg)) {
		ret = 1;
		goto failed_no_allocations;
	}
	if (pmem2_source_from_fd(&fdd->src, f->fd)) {
		ret = 1;
		goto failed_config_allocated;
	}

	/*
	 * Set flags for mmap
	 */

	if (td_rw(td) || td_write(td)) {
		flags = PMEM2_PROT_READ | PMEM2_PROT_WRITE;
	} else {
		flags = PMEM2_PROT_READ;
	}

	if(pmem2_config_set_protection(fdd->cfg,flags) != 0) {
		dprint(FD_IO, "DEBUG failed set protection\n");
		ret = 1;
		goto failed_source_allocated;
	}
	/*
	 * Depending on the hardware, the granularity might be better
	 */
	if (pmem2_config_set_required_store_granularity(fdd->cfg,
		PMEM2_GRANULARITY_PAGE)) {
		ret = 1;
		goto failed_source_allocated;
	}

#ifdef CONFIG_LIBMINIASYNC
	if(fdd->vdm != NULL) {
		pmem2_config_set_vdm(fdd->cfg, fdd->vdm);
	}
#endif

	if ((ret = pmem2_map_new(&fdd->map, fdd->cfg, fdd->src)) != 0) {
		dprint(FD_IO, "DEBUG failed map %d\n",ret);
		goto failed_source_allocated;
	}

	return ret;
failed_source_allocated:
	pmem2_source_delete(&fdd->src);
failed_config_allocated:
	pmem2_config_delete(&fdd->cfg);
failed_no_allocations:
	return ret;
}

static int fio_libpmem2_prep(struct thread_data *td, struct io_u *io_u)
{
	int ret = 0;
	struct fio_file *f = io_u->file;
	struct fio_libpmem2_data *fdd = FILE_ENG_DATA(f);

	dprint(FD_IO, "DEBUG fio_libpmem2_prep\n");
	dprint(FD_IO, "io_u->offset %llu : fdd->libpmem2_off %ld : "
			"io_u->buflen %llu : fdd->libpmem2_sz %ld\n",
			io_u->offset, fdd->libpmem2_off,
			io_u->buflen, fdd->libpmem2_sz);

	if (io_u->buflen > f->real_file_size) {
		log_err("libpmem2: bs bigger than the file size\n");
		return EIO;
	}

	/*
	 * Check if the mapping is sufficient, so we might not have to create
	 * a new one.
	 */
	if (io_u->offset >= fdd->libpmem2_off &&
	    io_u->offset + io_u->buflen <= fdd->libpmem2_off + fdd->libpmem2_sz) {
		goto done;
	}

	if((ret = fio_libpmem2_map_file_completely(td,f,fdd)) != 0) {
		/*
		 * We could not map the file completely. Then ret is 1
		 * or appropriate error code returned from pmem2_map_new.
		 */
		fio_libpmem2_unmap(fdd);
		return ret;
	}
	/*
	 * We only map files completely, so the offset is always 0.
	 * We do not take into account a case where address space is too small
	 * for a file to map it.
	 */
	fdd->libpmem2_off = 0;
	fdd->libpmem2_ptr = pmem2_map_get_address(fdd->map);
	fdd->libpmem2_sz  = pmem2_map_get_size(fdd->map);
	fdd->memcpy = pmem2_get_memcpy_fn(fdd->map);
	fdd->persist = pmem2_get_persist_fn(fdd->map);
	/*
 	 * If async is set, we must prepare virtual data mover instance
 	 */
	if(fdd->async == 3) {
#ifdef CONFIG_LIBMINIASYNC
		data_mover_threads_set_memcpy_fn(
			(struct data_mover_threads*)fdd->vdm,
			fdd->memcpy);
#endif
	}

done:
	io_u->mmap_data = fdd->libpmem2_ptr + io_u->offset - fdd->libpmem2_off
				- f->file_offset;
	return 0;
}

static enum fio_q_status fio_libpmem2_queue(struct thread_data *td,
					   struct io_u *io_u)
{
	unsigned flags = 0;
	struct fio_file *f = io_u->file;
	struct fio_libpmem2_data *fdd = FILE_ENG_DATA(f);

	fio_ro_check(td, io_u);
	io_u->error = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_queue\n");
	dprint(FD_IO, "td->o.odirect %d td->o.sync_io %d\n",
			td->o.odirect, td->o.sync_io);
	/* map both O_SYNC / DSYNC to not use NODRAIN */
	flags = td->o.sync_io ? 0 : PMEM2_F_MEM_NODRAIN;
	flags |= td->o.odirect ? PMEM2_F_MEM_NONTEMPORAL : PMEM2_F_MEM_TEMPORAL;

	switch (io_u->ddir) {
	case DDIR_READ:
		dprint(FD_IO, "DEBUG std memcpy\n");
		memcpy(io_u->xfer_buf, io_u->mmap_data, io_u->xfer_buflen);
		return FIO_Q_COMPLETED;
	case DDIR_WRITE:
		dprint(FD_IO, "DEBUG mmap_data=%p, xfer_buf=%p, numberio=%d\n",
				io_u->mmap_data, io_u->xfer_buf, io_u->numberio);
		if(!fdd->async) {
			fdd->memcpy(io_u->mmap_data,
				io_u->xfer_buf,
				io_u->xfer_buflen,
				flags);
			return FIO_Q_COMPLETED;
		} else {
            /*
             * Asynchronous queue
             */
#ifdef CONFIG_LIBMINIASYNC
			if (fdd->futs_count==td->o.iodepth) {
				return FIO_Q_BUSY;
			}
			for(int i=0;i<td->o.iodepth;i++) {
				/*
				 * Search for a free spot
				 */
				if(fdd->queued_io_us[i]==NULL) {
					fdd->queued_io_us[i] = io_u;
					fdd->futs[i] = pmem2_memcpy_async(
						fdd->map, io_u->mmap_data, io_u->xfer_buf,
						io_u->xfer_buflen, flags);
					return FIO_Q_QUEUED;
				}
			}
#endif
		}
		return FIO_Q_BUSY;
	case DDIR_SYNC:
	case DDIR_DATASYNC:
	case DDIR_SYNC_FILE_RANGE:
		dprint(FD_IO, "DEBUG sync\n");
		/*
		 * Only for sequential write
		 */
		fdd->persist(fdd->libpmem2_ptr + f->first_write,f->last_write - f->first_write);
		break;
	default:
		io_u->error = EINVAL;
		break;
	}

	return FIO_Q_COMPLETED;
}

static int fio_libpmem2_async_commit(struct thread_data *td) {
#ifdef CONFIG_LIBMINIASYNC
	unsigned nstarted = 0;
	struct fio_libpmem2_data *fdd = td->io_ops_data;
	dprint(FD_IO, "DEBUG fio_libpmem2_async_commit\n");

	for (int i = 0; i < td->o.iodepth; i++) {
		if(fdd->queued_io_us[i] == NULL ||
		   fdd->futs[i].base.context.state != FUTURE_STATE_IDLE) {
			continue;
		}
		/*
		 * Here we poll the futures for the first time, so we know
		 * they are not complete yet
		 */
		do {
			future_poll(FUTURE_AS_RUNNABLE(&fdd->futs[i]), NULL);
		} while (fdd->futs[i].base.context.state == FUTURE_STATE_IDLE);

		nstarted++;
		io_u_queued(td, fdd->queued_io_us[i]);
		io_u_mark_submit(td, 1);
	}

	dprint(FD_IO, "DEBUG nstarted=%u \n", nstarted);
#endif
	return 0;
}

static int fio_libpmem2_async_getevents(struct thread_data *td, unsigned int min,
	unsigned int max, const struct timespec *t) {
	int events = 0;
#ifdef CONFIG_LIBMINIASYNC
	struct fio_libpmem2_data *fdd = td->io_ops_data;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_getevents\n");
	while(events < min) {
		for (int i = 0; i < td->o.iodepth; i++) {
			if (fdd->queued_io_us[i] != NULL) {
				if(fdd->futs[i].base.context.state == FUTURE_STATE_COMPLETE) {
					/*
					 * The future is complete, so we can put it into events
					 * array and remove it from queue
					 */
					dprint(FD_IO, "futs[%d] done\n",i);

					fdd->event_io_us[events++] = fdd->queued_io_us[i];
					fdd->queued_io_us[i] = NULL;
					//io_u_mark_complete(td, 1);
					if(events == max) {
						goto max_events;
					}
				} else {
					dprint(FD_IO, "futs[%d] poll\n", i);
					future_poll(FUTURE_AS_RUNNABLE(&fdd->futs[i]), NULL);
				}
			}
		}
	}
max_events:
	fdd->futs_count -= events;
#endif
	dprint(FD_IO, "DEBUG events=%d\n", events);
	return events;
}

static struct io_u *fio_libpmem2_async_event(struct thread_data *td, int event) {
#ifdef CONFIG_LIBMINIASYNC
	struct fio_libpmem2_data *fdd = td->io_ops_data;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_event=%d\n", event);
	return fdd->event_io_us[event];
#else
	return NULL;
#endif
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name			= "libpmem2",
	.version		= FIO_IOOPS_VERSION,
	.init			= fio_libpmem2_init,
	.cleanup		= fio_libpmem2_fini,
	.prep			= fio_libpmem2_prep,
	.queue			= fio_libpmem2_queue,
	.open_file		= fio_libpmem2_open_file,
	.close_file		= fio_libpmem2_close_file,
	.get_file_size		= generic_get_file_size,
	.prepopulate_file 	= generic_prepopulate_file,
	.flags			= FIO_SYNCIO | FIO_RAWIO | FIO_NOEXTEND |
				FIO_BARRIER | FIO_MEMALIGN,
	.commit			= fio_libpmem2_async_commit,
	.getevents		= fio_libpmem2_async_getevents,
	.event			= fio_libpmem2_async_event,
	.options		= libpmem2_fio_options,
	.option_struct_size	= sizeof(struct libpmem2_fio_options_values),
};

static void fio_init fio_libpmem2_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_libpmem2_unregister(void)
{
	unregister_ioengine(&ioengine);
}
