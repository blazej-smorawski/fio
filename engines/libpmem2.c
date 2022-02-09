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
 * IO engine that uses libpmem2 to write data (and memcpy to read)
 *
 * To use:
 *   ioengine=libpmem2
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
#include <unistd.h>
#include <errno.h>
#include <libpmem2.h>

#include "../fio.h"
#include "../verify.h"

struct fio_libpmem2_data {
	void *libpmem2_ptr;
	size_t libpmem2_sz;
	off_t libpmem2_off;
	pmem2_memcpy_fn memcpy;
	pmem2_persist_fn persist;
	struct pmem2_config *cfg;
	struct pmem2_map *map;
	struct pmem2_source *src;
};

static int fio_libpmem2_init(struct thread_data *td)
{
	struct thread_options *o = &td->o;

	dprint(FD_IO, "o->rw_min_bs %llu\n o->fsync_blocks %u\n o->fdatasync_blocks %u\n",
			o->rw_min_bs, o->fsync_blocks, o->fdatasync_blocks);
	dprint(FD_IO, "DEBUG fio_libpmem2_init\n");

	if ((o->rw_min_bs & page_mask) &&
	    (o->fsync_blocks || o->fdatasync_blocks)) {
		log_err("libpmem2: mmap options dictate a minimum block size of "
				"%llu bytes\n",	(unsigned long long) page_size);
		return 1;
	}

	return 0;
}

static int fio_libpmem2_unmap(struct fio_libpmem2_data *fdd)
{
	return
		pmem2_map_delete(&fdd->map) |
		pmem2_source_delete(&fdd->src) |
		pmem2_config_delete(&fdd->cfg);
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
	if (td_rw(td) && !td->o.verify_only)
		flags = PMEM2_PROT_READ | PMEM2_PROT_WRITE;
	else if (td_write(td) && !td->o.verify_only) {
		flags = PMEM2_PROT_WRITE;

		if (td->o.verify != VERIFY_NONE)
			flags |= PMEM2_PROT_READ;
	} else
		flags = PMEM2_PROT_READ;

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
	 * We do not do any checks of libpmem2_ptr or so, because if the mapping
	 * was successful, pmem2_map_get_address cannot fail.
	 */

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
		break;
	case DDIR_WRITE:
		dprint(FD_IO, "DEBUG mmap_data=%p, xfer_buf=%p\n",
				io_u->mmap_data, io_u->xfer_buf);
		fdd->memcpy(io_u->mmap_data,
				io_u->xfer_buf,
				io_u->xfer_buflen,
				flags);
		break;
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

static int fio_libpmem2_open_file(struct thread_data *td, struct fio_file *f)
{
	struct fio_libpmem2_data *fdd;
	int ret;

	ret = generic_open_file(td, f);
	if (ret)
		return ret;

	fdd = calloc(1, sizeof(*fdd));
	if (fdd == NULL)
		return 1;


	FILE_SET_ENG_DATA(f, fdd);
	return 0;
}

static int fio_libpmem2_close_file(struct thread_data *td, struct fio_file *f)
{
	struct fio_libpmem2_data *fdd = FILE_ENG_DATA(f);
	int ret = 0;

	dprint(FD_IO, "DEBUG fio_libpmem2_close_file\n");

	if (fdd->libpmem2_ptr) {
		ret = fio_libpmem2_unmap(fdd);
	}
	if (fio_file_open(f)) {
		ret &= generic_close_file(td, f);
	}

	FILE_SET_ENG_DATA(f, NULL);
	free(fdd);

	return ret;
}

FIO_STATIC struct ioengine_ops ioengine = {
	.name		= "libpmem2",
	.version	= FIO_IOOPS_VERSION,
	.init		= fio_libpmem2_init,
	.prep		= fio_libpmem2_prep,
	.queue		= fio_libpmem2_queue,
	.open_file	= fio_libpmem2_open_file,
	.close_file	= fio_libpmem2_close_file,
	.get_file_size	= generic_get_file_size,
	.prepopulate_file = generic_prepopulate_file,
	.flags		= FIO_SYNCIO | FIO_RAWIO | FIO_NOEXTEND |
				FIO_BARRIER | FIO_MEMALIGN,
};

static void fio_init fio_libpmem2_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_libpmem2_unregister(void)
{
	unregister_ioengine(&ioengine);
}
