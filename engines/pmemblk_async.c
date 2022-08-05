/*
 * pmemblk: IO engine that uses PMDK libpmemblk to read and write data
 *
 * Copyright (C) 2016 Hewlett Packard Enterprise Development LP
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
 * You should have received a copy of the GNU General Public
 * License along with this program; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

/*
 * pmemblk engine
 *
 * IO engine that uses libpmemblk to read and write data
 *
 * To use:
 *   ioengine=pmemblk
 *
 * Other relevant settings:
 *   iodepth=1
 *   direct=1
 *   unlink=1
 *   filename=/mnt/pmem0/fiotestfile
 *
 *   thread must be set to 1 for pmemblk as multiple processes cannot
 *     open the same block pool file.
 *
 *   iodepth should be set to 1 as pmemblk is always synchronous.
 *   Use numjobs to scale up.
 *
 *   direct=1 is implied as pmemblk is always direct. A warning message
 *   is printed if this is not specified.
 *
 *   unlink=1 removes the block pool file after testing, and is optional.
 *
 *   The pmem device must have a DAX-capable filesystem and be mounted
 *   with DAX enabled.  filename must point to a file on that filesystem.
 *
 *   Example:
 *     mkfs.xfs /dev/pmem0
 *     mkdir /mnt/pmem0
 *     mount -o dax /dev/pmem0 /mnt/pmem0
 *
 *   When specifying the filename, if the block pool file does not already
 *   exist, then the pmemblk engine creates the pool file if you specify
 *   the block and file sizes.  BSIZE is the block size in bytes.
 *   FSIZEMB is the pool file size in MiB.
 *
 *   See examples/pmemblk.fio for more.
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/uio.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <libpmem.h>

#define PMEMBLK_USE_MINIASYNC

#include <libpmemblk.h>
#include <libminiasync.h>
#include "../fio.h"
#include "../optgroup.h"

/*
 * libpmemblk
 */
#define PMB_CREATE (0x0001) /* should create file */
#define PMB_SIZE 1 << 30 /*1 GiB */

static pthread_mutex_t shared_pool_lock = PTHREAD_MUTEX_INITIALIZER;

struct fio_pmemblk_options_values {
	void *pad;
	int *async;
};

struct fio_option fio_pmemblk_options[] = {
	{
		.name        = "async",
		.lname        = "asynchronous_mode",
		.type        = FIO_OPT_STR_STORE,
		.off1        = offsetof(struct fio_pmemblk_options_values,
			async),
		.def        = 0,
		.help        = "Use asynchronous operations",
		.category = FIO_OPT_C_ENGINE,
		.group        = FIO_OPT_G_PMEMBLK,
	},
	{
		.name        = NULL,
	},
};

union pmemblk_async_future {
	struct pmemblk_read_async_future read;
	struct pmemblk_write_async_future write;
};

static PMEMblkpool *pool;

struct fio_pmemblk_data {
	size_t nblocks;
	size_t bsize;

	/* Data for asynchronous operations */
	int async;
	struct vdm *vdm;
	int queued;
	union pmemblk_async_future *futures;
	struct io_u **queued_events;
	struct io_u **completed_events;
};

static int pmb_get_flags(struct thread_data *td, uint64_t *pflags)
{
	static int odirect_warned = 0;

	uint64_t flags = 0;

	if (!td->o.odirect && !odirect_warned) {
		odirect_warned = 1;
		log_info(
			"pmemblk: direct == 0, but pmemblk is always direct\n");
	}

	if (td->o.allow_create)
		flags |= PMB_CREATE;

	(*pflags) = flags;
	return 0;
}

static int fio_pmemblk_init(struct thread_data *td)
{

	return 0;
}

static int fio_pmemblk_cleanup(struct thread_data *td)
{

	return 0;
}

static struct fio_pmemblk_data *pmb_open(struct thread_data *td, struct fio_file *f)
{
	struct fio_pmemblk_options_values *options = td->eo;
	struct thread_options *thread_options = &td->o;
	struct fio_pmemblk_data *pmb = td->io_ops_data;
	struct thread_options *o = &td->o;
	uint64_t flags;
	pmb_get_flags(td, &flags);

	dprint(FD_IO, "pmemblk: open\n");

	pthread_mutex_lock(&shared_pool_lock);

	if(pmb == NULL) {
		pmb = malloc(sizeof(*pmb));
		if (options->async) {
			pmb->async = 1;
			pmb->futures =
				calloc(thread_options->iodepth, sizeof(*pmb->futures));
			pmb->queued_events =
				calloc(thread_options->iodepth, sizeof(*pmb->queued_events));
			pmb->completed_events =
				calloc(thread_options->iodepth, sizeof(*pmb->completed_events));

			//pmb->vdm = data_mover_sync_get_vdm(data_mover_sync_new());
			pmb->vdm = data_mover_threads_get_vdm(data_mover_threads_default());
			if(!pool) {
				pool = pmemblk_xopen(f->file_name, o->rw_min_bs, pmb->vdm);
				if (!pool && (flags & PMB_CREATE) && (errno == ENOENT) && (0 < f->real_file_size) && (0 < o->rw_min_bs)) {
					dprint(FD_IO, "pmemblk: create pool\n");
					pool = pmemblk_xcreate(f->file_name, o->rw_min_bs, PMB_SIZE, 0644, pmb->vdm);
				}
			}
		} else {
			pmb->async = 0;
			/* try opening existing first, create it if needed */
			if(!pool) {
				pool = pmemblk_open(f->file_name, o->rw_min_bs);
				if (!pool && (flags & PMB_CREATE) && (errno == ENOENT) && (0 < f->real_file_size) && (0 < o->rw_min_bs)) {
					dprint(FD_IO, "pmemblk: create pool\n");
					pool = pmemblk_create(f->file_name, o->rw_min_bs, PMB_SIZE, 0644);
				}
			}
		}
		pmb->bsize = pmemblk_bsize(pool);
		pmb->nblocks = pmemblk_nblock(pool);
	}

	if (!pool) {
		log_err("pmemblk: unable to open pmemblk pool file %s (%s)\n",
			f->file_name, strerror(errno));
		goto error;
	}

	td->io_ops_data = pmb;
	dprint(FD_IO,
		"pmemblk: created pool->bsize=%zu, nblocks=%zu\n",
		pmb->bsize, pmb->nblocks);


	pthread_mutex_unlock(&shared_pool_lock);
	return pmb;

	error:
	if (pmb) {
		if (pool)
			pmemblk_close(pool);
		pool = NULL;
		free(pmb);
	}

	pthread_mutex_unlock(&shared_pool_lock);
	return NULL;
}

static int fio_pmemblk_open_file(struct thread_data *td, struct fio_file *f)
{
	struct fio_pmemblk_data *pmb;

	pmb = pmb_open(td, f);
	if (!pmb)
		return 1;

	FILE_SET_ENG_DATA(f, pmb);
	return 0;
}

static void fio_pmblk_close(struct fio_pmemblk_data *pmb)
{
	pthread_mutex_lock(&shared_pool_lock);

	if(pool)
		pmemblk_close(pool);
	pool = NULL;
	free(pmb);

	pthread_mutex_unlock(&shared_pool_lock);
}

static int fio_pmemblk_close_file(struct thread_data fio_unused *td,
	struct fio_file *f)
{
	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);

	if (pmb)
		//fio_pmblk_close(pmb);

	FILE_SET_ENG_DATA(f, NULL);
	return 0;
}

static int fio_pmemblk_get_file_size(struct thread_data *td, struct fio_file *f)
{
	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);

	dprint(FD_IO, "pmemblk: get file size\n");

	if (fio_file_size_known(f))
		return 0;

	/*
	 * In case the pmemblk pool was not opened yet
	 */
	if (!pmb) {
		/*
		 * To see the size, we must open the file, so we do it
		 * using the fio callback. Later on, if the engine needs
		 * to open the file it will be already open and there will be
		 * nothing left to do.
		 */
		if (fio_pmemblk_open_file(td, f))
			return 1;
		pmb = FILE_ENG_DATA(f);
	}

	f->real_file_size = pmb->bsize * pmb->nblocks;

	fio_file_set_size_known(f);

	return 0;
}

static int fio_pmemblk_commit(struct thread_data *td)
{
	unsigned nstarted = 0;
	struct fio_pmemblk_data *pmb = td->io_ops_data;
	dprint(FD_IO, "pmemblk: commit\n");

	for (int i = 0; i < td->o.iodepth; i++) {
		if (pmb->queued_events[i] == NULL ||
			FUTURE_STATE(&pmb->futures[i].write) !=
				FUTURE_STATE_IDLE) {
			continue;
		}
		/*
		 * Here we poll the futures for the first time, so we know
		 * they are not complete yet
		 */
		do {
			future_poll(FUTURE_AS_RUNNABLE(&pmb->futures[i].write),
				NULL);
		} while (FUTURE_STATE(&pmb->futures[i].write) ==
			FUTURE_STATE_IDLE);

		/*
		 * Leaving the loop, we know that the future is complete or running
		 */
		nstarted++;
		io_u_queued(td, pmb->queued_events[i]);
	}

	dprint(FD_IO, "DEBUG nstarted=%u\n", nstarted);
	return 0;
}

/*
 * The ->event() hook is called to match an event number with an io_u.
 * After the core has called ->getevents() and it has returned eg 3,
 * the ->event() hook must return the 3 events that have completed for
 * subsequent calls to ->event() with [0-2]. Required.
 */
static struct io_u *fio_pmblk_event(struct thread_data *td, int event)
{
	struct fio_pmemblk_data *pmb = td->io_ops_data;

	dprint(FD_IO, "DEBUG fio_pmemblk_event=%d\n", event);
	return pmb->completed_events[event];
}

/*
 * The ->getevents() hook is used to reap completion events from an async
 * io engine. It returns the number of completed events since the last call,
 * which may then be retrieved by calling the ->event() hook with the event
 * numbers. Required.
 */
static int fio_pmblk_getevents(struct thread_data *td, unsigned int min,
	unsigned int max, const struct timespec *t)
{
	int events = 0;
	struct fio_pmemblk_data *pmb = td->io_ops_data;

	dprint(FD_IO, "DEBUG fio_pmblk_getevents\n");
	while (events < min) {
		for (int i = 0; i < td->o.iodepth; i++) {
			/*
			 * Queued operations that were not collected into event_io_us
			 * have a not NULL pointer in queued_io_us
			 */
			if (pmb->queued_events[i] != NULL) {
				if (FUTURE_STATE(&pmb->futures[i].write) ==
					FUTURE_STATE_COMPLETE) {
					/*
					 * The future is complete, so we can put it into events
					 * array and remove it from queue
					 */
					pmb->completed_events[events++] = pmb->queued_events[i];

					/*
					 * We set value at the index to NULL, so
					 * the queued io will not be added to events_io_us
					 * again.
					 */
					pmb->queued_events[i] = NULL;
					if (events == max) {
						goto max_events;
					}
				} else {
					/*
					 * Future was not complete yet so, we poll it to get
					 * it's state set if it changed by the time of a previous
					 * future_poll.
					 */
					future_poll(FUTURE_AS_RUNNABLE(
						&pmb->futures[i].write), NULL);
				}
			}
		}
	}
	max_events:
	pmb->queued -= events;
	dprint(FD_IO, "DEBUG events=%d\n", events);
	return events;
}

static enum fio_q_status fio_pmemblk_queue(struct thread_data *td,
	struct io_u *io_u)
{
	struct fio_file *f = io_u->file;
	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);

	unsigned long long off;
	unsigned long len;
	void *buf;

	fio_ro_check(td, io_u);

	dprint(FD_IO, "pmemblk: off->%llu, len->%llu, off%%bs->%llu\n", io_u->offset, io_u->buflen, io_u->offset%pmb->bsize);

	switch (io_u->ddir) {
		case DDIR_READ:
		case DDIR_WRITE:
			off = io_u->offset;
			len = io_u->xfer_buflen;

			/*
			 * I'm not a fan of this part, because we should guarantee
			 * those parameters differently.
			 */
			io_u->error = EINVAL;
			if (off % pmb->bsize) {
				dprint(FD_IO,"pmemblk: 1\n");
				break;
			}
			if (len % pmb->bsize) {
				dprint(FD_IO,"pmemblk: 2\n");
				break;
			}
			if ((off + len) / pmb->bsize > pmb->nblocks) {
				dprint(FD_IO,"pmemblk: 3\n");
				break;
			}

			io_u->error = 0;
			buf = io_u->xfer_buf;
			off /= pmb->bsize;
			len /= pmb->bsize;

			if (!pmb->async) {
				if (io_u->ddir == DDIR_READ) {
					/*
					 * We assume that all io operations use same block size as
					 * block size in pmemblk pool.
					 */
					if (0 != pmemblk_read(pool, buf,off)) {
						io_u->error = errno;
						break;
					}
				} else if (0 != pmemblk_write(pool, buf, off)) {
					io_u->error = errno;
					break;
				}

				//off *= pmb->bsize;
				//len *= pmb->bsize;
				//io_u->resid = io_u->xfer_buflen - (off - io_u->offset);
				return FIO_Q_COMPLETED;
			}

			/*
			 * Handle asynchronous operations
			 */
			if (pmb->queued == td->o.iodepth) {
				return FIO_Q_BUSY;
			}
			for (int i = 0; i < td->o.iodepth; i++) {
				/*
				 * Search for a free spot in futures/events array
				 */
				if (pmb->queued_events[i] == NULL) {
					pmb->queued_events[i] = io_u;
					if (io_u->ddir == DDIR_WRITE) {
						pmb->futures[i].write = pmemblk_write_async(pool, buf, off);
					} else if (io_u->ddir == DDIR_READ) {
						pmb->futures[i].read = pmemblk_read_async(pool, buf, off);
					}
					break;
				}
			}

			pmb->queued++;
			//io_u->resid = io_u->xfer_buflen - (off - io_u->offset);
			return FIO_Q_QUEUED;
		case DDIR_SYNC:
		case DDIR_DATASYNC:
		case DDIR_SYNC_FILE_RANGE:
			/* we're always sync'd */
			io_u->error = 0;
			break;
		default:
			io_u->error = EINVAL;
			break;
	}

	return FIO_Q_COMPLETED;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name = "pmemblk_async",
	.version = FIO_IOOPS_VERSION,
	.init = fio_pmemblk_init,
	.cleanup = fio_pmemblk_cleanup,
	.commit = fio_pmemblk_commit,
	.getevents = fio_pmblk_getevents,
	.event = fio_pmblk_event,
	.queue = fio_pmemblk_queue,
	.open_file = fio_pmemblk_open_file,
	.close_file = fio_pmemblk_close_file,
	.get_file_size = fio_pmemblk_get_file_size,
	.options = fio_pmemblk_options,
	.option_struct_size = sizeof(struct fio_pmemblk_options_values),
	.flags = FIO_DISKLESSIO | FIO_NOEXTEND | FIO_NODISKUTIL | FIO_MEMALIGN,
};

static void fio_init fio_pmemblk_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_pmemblk_unregister(void)
{
	unregister_ioengine(&ioengine);
}
