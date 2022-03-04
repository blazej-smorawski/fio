/*
 * libpmem2_async: IO engine that uses PMDK libpmem2_async to read and write data
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
 * libpmem2_async engine
 *
 * IO engine that uses libpmem2_async to write data (and memcpy to read)
 *
 * To use:
 *   ioengine=libpmem2_async
 *
 * Other relevant settings:
 *   iodepth=1
 *   direct=1
 *   sync=1
 *   directory=/mnt/pmem0/
 *   bs=4k
 *
 *   sync=1 means that pmem2_persist_fn is executed for each write operation.
 *   Otherwise is not and should be called on demand.
 *
 *   sync_file_range=write:n means that pmem_persist_fn is executed
 *   after n write operations.
 *
 *   direct=1 means PMEM_F_MEM_NONTEMPORAL flag is set in memcpy.
 */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <libpmem2.h>
#include <libminiasync.h>
#include <libminiasync-dml.h>
#include "../fio.h"
#include "../verify.h"

struct fio_libpmem2_async_data {
    void *libpmem2_async_ptr;
    size_t libpmem2_async_sz;
    off_t libpmem2_async_off;
    pmem2_memcpy_fn memcpy;
    pmem2_persist_fn persist;
    struct pmem2_config *cfg;
    struct pmem2_map *map;
    struct pmem2_source *src;

    /* Data for miniasync */
    struct vdm *vdm;
    struct runtime *r;
    /*
     * Array of futures that were not synchronised yet.
     * It is limited by MAX_FUTURES_COUNT, so in fio job parameter
     * sync_file_range=str:n, the n must not exceed MAX_FUTURES_COUNT
     */
    struct vdm_operation_future *futs;
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
};

static int fio_libpmem2_async_init(struct thread_data *td) {
	struct thread_options *o = &td->o;
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;

	dprint(FD_IO, "o->rw_min_bs %llu\n o->fsync_blocks %u\n o->fdatasync_blocks %u\n",
		o->rw_min_bs, o->fsync_blocks, o->fdatasync_blocks);
	dprint(FD_IO, "DEBUG fio_libpmem2_async_init\n");

	fdd = calloc(1, sizeof(*fdd));
	if (fdd == NULL)
		return 1;

	fdd->vdm = NULL;
	fdd->futs_count = 0;
	//fdd->vdm = vdm_new(vdm_descriptor_threads(), NULL);
	/*
	 * All these arrays' elements count do not exceed iodepth
	 * because we will never create a new future if there is no place
	 * and all io_us are stored only for a one batch of futures.
	 */
	fdd->futs = malloc(o->iodepth * sizeof(*fdd->futs));
	fdd->queued_io_us = calloc(o->iodepth,sizeof(*fdd->queued_io_us));
	fdd->event_io_us = calloc(o->iodepth,sizeof(*fdd->event_io_us));

	if ((o->rw_min_bs & page_mask) &&
	    (o->fsync_blocks || o->fdatasync_blocks)) {
		log_err("libpmem2_async: mmap options dictate a minimum block size of "
			"%llu bytes\n", (unsigned long long) page_size);
		return 1;
	}

	td->io_ops_data = fdd;
	return 0;
}

static int fio_libpmem2_async_unmap(struct fio_libpmem2_async_data *fdd) {
	return
		pmem2_map_delete(&fdd->map) |
		pmem2_source_delete(&fdd->src) |
		pmem2_config_delete(&fdd->cfg);
}

static int fio_libpmem2_async_map_file_completely(struct thread_data *td,
	struct fio_file *f, struct fio_libpmem2_async_data *fdd) {
	int ret = 0;
	int flags = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_map_file_completely\n");
	/*
	 * If there was any mapping before, we unmap it
	 */
	if (fdd->libpmem2_async_ptr) {
		if (fio_libpmem2_async_unmap(fdd) != 0) {
			ret = 1;
			goto failed_no_allocations;
		}
		fdd->libpmem2_async_ptr = NULL;
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
	if (td_rw(td) && !td->o.verify_only)
		flags = PMEM2_PROT_READ | PMEM2_PROT_WRITE;
	else if (td_write(td) && !td->o.verify_only) {
		flags = PMEM2_PROT_WRITE;

		if (td->o.verify != VERIFY_NONE)
			flags |= PMEM2_PROT_READ;
	} else
		flags = PMEM2_PROT_READ;

	if (pmem2_config_set_protection(fdd->cfg, flags) != 0) {
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
	if ((ret = pmem2_map_new(&fdd->map, fdd->cfg, fdd->src)) != 0) {
		dprint(FD_IO, "DEBUG failed map %d\n", ret);
		goto failed_source_allocated;
	}

	if (fdd->vdm == NULL) {
		//data_mover_threads_delete(fdd->vdm);
		struct data_mover_dml *dmd = data_mover_dml_new();
		fdd->vdm = data_mover_dml_get_vdm(dmd);
	}

	//struct data_mover_threads *dmt = data_mover_threads_new(2,32768,FUTURE_NOTIFIER_NONE);
	//data_mover_threads_set_memcpy_fn(dmt,pmem2_get_memcpy_fn(fdd->map));
	//struct data_mover_dml *dmd = data_mover_dml_new();

	//fdd->vdm = data_mover_threads_get_vdm(dmt);fdd->vdm = data_mover_dml_get_vdm(dmd);

	return ret;
	failed_source_allocated:
	pmem2_source_delete(&fdd->src);
	failed_config_allocated:
	pmem2_config_delete(&fdd->cfg);
	failed_no_allocations:
	return ret;
}

static int fio_libpmem2_async_prep(struct thread_data *td, struct io_u *io_u) {
	int ret = 0;
	struct fio_file *f = io_u->file;
	struct fio_libpmem2_async_data *fdd = FILE_ENG_DATA(f);

	dprint(FD_IO, "DEBUG fio_libpmem2_async_prep\n");
	dprint(FD_IO, "io_u->offset %llu : fdd->libpmem2_async_off %ld : "
		      "io_u->buflen %llu : fdd->libpmem2_async_sz %ld\n",
		io_u->offset, fdd->libpmem2_async_off,
		io_u->buflen, fdd->libpmem2_async_sz);

	if (io_u->buflen > f->real_file_size) {
		log_err("libpmem2_async: bs bigger than the file size\n");
		return EIO;
	}

	/*
	 * Check if the mapping is sufficient, so we might not have to create
	 * a new one.
	 */
	if (io_u->offset >= fdd->libpmem2_async_off &&
	    io_u->offset + io_u->buflen <= fdd->libpmem2_async_off + fdd->libpmem2_async_sz) {
		dprint(FD_IO, "Good mapping\n");
		goto done;
	}

	if ((ret = fio_libpmem2_async_map_file_completely(td, f, fdd)) != 0) {
		/*
		 * We could not map the file completely. Then ret is 1
		 * or appropriate error code returned from pmem2_map_new.
		 */
		fio_libpmem2_async_unmap(fdd);
		dprint(FD_IO, "Error map %d\n",ret);
		return ret;
	}
	/*
	 * We only map files completely, so the offset is always 0.
	 * We do not take into account a case where address space is too small
	 * for a file to map it.
	 */
	fdd->libpmem2_async_off = 0;
	fdd->libpmem2_async_ptr = pmem2_map_get_address(fdd->map);
	fdd->libpmem2_async_sz = pmem2_map_get_size(fdd->map);
	fdd->memcpy = pmem2_get_memcpy_fn(fdd->map);
	fdd->persist = pmem2_get_persist_fn(fdd->map);
	/*
	 * We do not do any checks of libpmem2_async_ptr or so, because if the mapping
	 * was successful, pmem2_map_get_address cannot fail.
	 */

	done:
	io_u->mmap_data = fdd->libpmem2_async_ptr + io_u->offset - fdd->libpmem2_async_off
			  - f->file_offset;
	return 0;
}

static enum fio_q_status fio_libpmem2_async_queue(struct thread_data *td,
	struct io_u *io_u) {
	unsigned flags = 0;
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;
	dprint(FD_IO, "DEBUG queue iodepth=%u futs_count=%d\n",td->o.iodepth,fdd->futs_count);
	if (fdd->futs_count == td->o.iodepth) {
		/*
		 * We reched the limit of io depth, so we cannot queue any
		 * more operations until they are commited and getevents is
		 * called.
		 */
		dprint(FD_IO, "DEBUG FIO_Q_BUSY\n");
		return FIO_Q_BUSY;
	}

	fio_ro_check(td, io_u);
	io_u->error = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_queue\n");
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
			dprint(FD_IO, "DEBUG mmap_data=%p, xfer_buf=%p\n",
				io_u->mmap_data, io_u->xfer_buf);

			for(int i=0;i<td->o.iodepth;i++) {
				/*
				 * Search for a free spot
				 */
				if(fdd->queued_io_us[i]==NULL) {
					fdd->queued_io_us[i]=io_u;
					fdd->futs[fdd->futs_count] = vdm_memcpy(
						fdd->vdm, io_u->mmap_data, io_u->xfer_buf,
						io_u->xfer_buflen, 0);
					fdd->futs_count++;
					return FIO_Q_QUEUED;
				}
			}
			break;
		case DDIR_SYNC:
		case DDIR_DATASYNC:
		case DDIR_SYNC_FILE_RANGE:
			break;
		default:
			io_u->error = EINVAL;
			break;
	}

	return FIO_Q_BUSY;
}

static int fio_libpmem2_async_commit(struct thread_data *td) {
	unsigned ndone = 0;
	unsigned nstarted = 0;
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;
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
			dprint(FD_IO, "DEBUG fio_libpmem2_async_commit [%d]-state-[%d]\n",i,fdd->futs[i].base.context.state);
			future_poll(FUTURE_AS_RUNNABLE(&fdd->futs[i]), NULL);
			dprint(FD_IO, "DEBUG fio_libpmem2_async_commit [%d]-state-[%d]\n",i,fdd->futs[i].base.context.state);
		} while (fdd->futs[i].base.context.state == FUTURE_STATE_IDLE);
		
		nstarted++;
		io_u_queued(td, fdd->queued_io_us[i]);
		io_u_mark_submit(td, 1);
		if (fdd->futs[i].base.context.state == FUTURE_STATE_COMPLETE) {
			ndone++;
			//io_u_mark_complete(td, 1);
		}
	}

	dprint(FD_IO, "DEBUG nstarted=%u ndone=%u\n", nstarted, ndone);
	return 0;
}

static int fio_libpmem2_async_getevents(struct thread_data *td, unsigned int min,
	unsigned int max, const struct timespec *t) {
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;
	int events = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_getevents\n");

	while(events < min) {
		for (int i = 0; i < td->o.iodepth; i++) {
			if (fdd->queued_io_us[i] != NULL) {
				dprint(FD_IO, "DEBUG fio_libpmem2_async_getevents [%d]-state-[%d]\n",i,fdd->futs[i].base.context.state);
				//dprint(FD_IO, "futs[%d] ptr not null\n", i);
				if(fdd->futs[i].base.context.state == FUTURE_STATE_COMPLETE) {
					/*
					 * The future is complete, so we can put it into events
					 * array and remove it from queue
					 */
					//dprint(FD_IO, "futs[%d] done\n",i);

					fdd->event_io_us[events++] = fdd->queued_io_us[i];
					fdd->queued_io_us[i] = NULL;
					io_u_mark_complete(td, 1);
				} else {
					//dprint(FD_IO, "futs[%d] poll\n", i);
					future_poll(FUTURE_AS_RUNNABLE(&fdd->futs[i]), NULL);
				}
			}
			if(events == max) {
				break;
			}
		}
	}

	fdd->futs_count -= events;
	dprint(FD_IO, "DEBUG events=%d\n", events);
	return events;
}

static struct io_u *fio_libpmem2_async_event(struct thread_data *td, int event) {
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_event=%d\n", event);
	return fdd->event_io_us[event];
}

static void fio_libpmem2_async_fini(struct thread_data *td) {
	/*
	 * Perform cleanup of data created with init
	 */
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;
	if (fdd->libpmem2_async_ptr) {
		fio_libpmem2_async_unmap(fdd);
	}
	free(fdd->futs);
	free(fdd->queued_io_us);
	free(fdd->event_io_us);
	free(fdd);
}

static int fio_libpmem2_async_open_file(struct thread_data *td, struct fio_file *f) {
	struct fio_libpmem2_async_data *fdd = td->io_ops_data;
	int ret;

	ret = generic_open_file(td, f);
	if (ret)
		return ret;

	FILE_SET_ENG_DATA(f, fdd);
	return 0;
}

static int fio_libpmem2_async_close_file(struct thread_data *td, struct fio_file *f) {
	struct fio_libpmem2_async_data *fdd = FILE_ENG_DATA(f);
	int ret = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_async_close_file\n");

	if (fio_file_open(f)) {
		ret &= generic_close_file(td, f);
	}

	return ret;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name           = "libpmem2_async",
	.version        = FIO_IOOPS_VERSION,
	.init           = fio_libpmem2_async_init,
	.prep           = fio_libpmem2_async_prep,
	.queue          = fio_libpmem2_async_queue,
	.commit         = fio_libpmem2_async_commit,
	.getevents      = fio_libpmem2_async_getevents,
	.event          = fio_libpmem2_async_event,
	.cleanup        = fio_libpmem2_async_fini,
	.open_file      = fio_libpmem2_async_open_file,
	.close_file     = fio_libpmem2_async_close_file,
	.get_file_size  = generic_get_file_size,
	.prepopulate_file = generic_prepopulate_file,
	.flags                = FIO_RAWIO | FIO_NOEXTEND | FIO_MEMALIGN,
};

static void fio_init fio_libpmem2_async_register(void) {
	register_ioengine(&ioengine);
}

static void fio_exit fio_libpmem2_async_unregister(void) {
	unregister_ioengine(&ioengine);
}
