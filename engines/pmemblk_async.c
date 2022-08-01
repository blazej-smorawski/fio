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

#include "../fio.h"

/*
 * libpmemblk
 */
#define PMB_CREATE (0x0001) /* should create file */

pthread_mutex_t shared_pool_lock = PTHREAD_MUTEX_INITIALIZER;

struct fio_pmemblk_data {
	PMEMblkpool *pool;
	size_t nblocks;
	size_t bsize;
};

static int pmb_get_flags(struct thread_data *td, uint64_t *pflags)
{
	static int odirect_warned = 0;

	uint64_t flags = 0;

	if (!td->o.odirect && !odirect_warned) {
		odirect_warned = 1;
		log_info("pmemblk: direct == 0, but pmemblk is always direct\n");
	}

	if (td->o.allow_create)
		flags |= PMB_CREATE;

	(*pflags) = flags;
	return 0;
}

static struct fio_pmemblk_data* pmb_open(struct thread_data *td, struct fio_file *f)
{

	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);
	struct thread_options *o = &td->o;
	uint64_t flags;
	pmb_get_flags(td, &flags);

	pthread_mutex_lock(&shared_pool_lock);

	if (!pmb) {
		pmb = malloc(sizeof(*pmb));
		if (!pmb)
			goto error;

		/* try opening existing first, create it if needed */
		pmb->pool = pmemblk_open(f->file_name, o->rw_min_bs);
		if (!pmb->pool && (errno == ENOENT) &&
			(flags & PMB_CREATE) && (0 < f->real_file_size) &&
			(0 < o->rw_min_bs)) {
			pmb->pool =
				pmemblk_create(f->file_name, o->rw_min_bs,
					f->io_size, 0644);
		}
		if (!pmb->pool) {
			log_err("pmemblk: unable to open pmemblk pool file %s (%s)\n",
				f->file_name, strerror(errno));
			goto error;
		}

		pmb->bsize = pmemblk_bsize(pmb->pool);
		pmb->nblocks = pmemblk_nblock(pmb->pool);
		dprint(FD_IO, "pmemblk: created pool->bsize=%zu, nblocks=%zu\n",
			pmb->bsize, pmb->nblocks);
	}

	pthread_mutex_unlock(&shared_pool_lock);

	return pmb;

error:
	if (pmb) {
		if (pmb->pool)
			pmemblk_close(pmb->pool);
		pmb->pool = NULL;
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

static void pmb_close(struct fio_pmemblk_data *pmb)
{
	pthread_mutex_lock(&shared_pool_lock);

	pmemblk_close(pmb->pool);
	pmb->pool = NULL;
	free(pmb);

	pthread_mutex_unlock(&shared_pool_lock);
}

static int fio_pmemblk_close_file(struct thread_data fio_unused *td,
				  struct fio_file *f)
{
	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);

	if (pmb)
		pmb_close(pmb);

	FILE_SET_ENG_DATA(f, NULL);
	return 0;
}

static int fio_pmemblk_get_file_size(struct thread_data *td, struct fio_file *f)
{
	struct fio_pmemblk_data *pmb = FILE_ENG_DATA(f);

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
	}

	f->real_file_size = pmb->bsize * pmb->nblocks;

	fio_file_set_size_known(f);

	return 0;
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
		if (off % pmb->bsize)
			break;
		if (len % pmb->bsize)
			break;
		if ((off + len) / pmb->bsize > pmb->nblocks)
			break;

		io_u->error = 0;
		buf = io_u->xfer_buf;
		off /= pmb->bsize;
		len /= pmb->bsize;
		while (0 < len) {
			if (io_u->ddir == DDIR_READ) {
				if (0 != pmemblk_read(pmb->pool, buf, off)) {
					io_u->error = errno;
					break;
				}
			} else if (0 != pmemblk_write(pmb->pool, buf, off)) {
				io_u->error = errno;
				break;
			}
			buf += pmb->bsize;
			off++;
			len--;
		}
		off *= pmb->bsize;
		len *= pmb->bsize;
		io_u->resid = io_u->xfer_buflen - (off - io_u->offset);
		break;
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
	.queue = fio_pmemblk_queue,
	.open_file = fio_pmemblk_open_file,
	.close_file = fio_pmemblk_close_file,
	.get_file_size = fio_pmemblk_get_file_size,
	.flags = FIO_SYNCIO | FIO_DISKLESSIO | FIO_NOEXTEND | FIO_NODISKUTIL,
};

static void fio_init fio_pmemblk_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_pmemblk_unregister(void)
{
	unregister_ioengine(&ioengine);
}
