// SPDX-License-Identifier: Apache-2.0
/*
 * test_nrht_race.c - NRHT concurrent insert/delete CAS race test
 *
 * Multiple child processes simultaneously perform NRHT operations
 * (insert, lookup, delete) on shared region files to exercise the
 * CAS-based lock-free hash table under contention.
 *
 * Test 1: N processes insert the SAME name → exactly 1 succeeds
 * Test 2: N processes insert DIFFERENT names → all succeed, all findable
 * Test 3: Concurrent insert + delete → lookup returns either found or ENOENT
 *
 * Usage: ./test_nrht_race [--rounds N] [--strategy order|request|both]
 *                         <mount1> <mount2> [... up to 8 mounts]
 *   Accepts 2..8 mount points on the same CXL device (different node_ids).
 *   Workers round-robin across mounts to simulate cross-node contention.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

/* --- Test helpers --- */

static int pass_count;
static int fail_count;

#define TEST(name, expr)                                                   \
	do {                                                               \
		if (expr) {                                                \
			printf("  PASS: %s\n", name);                      \
			pass_count++;                                      \
		} else {                                                   \
			printf("  FAIL: %s (errno=%d: %s)\n", name, errno, \
			       strerror(errno));                           \
			fail_count++;                                      \
		}                                                          \
	} while (0)

#define SLOT_SIZE (4ULL * 1024 * 1024)
#define NUM_WORKERS 8
#define DEFAULT_ROUNDS 10

/* --- Sync helpers --- */

static void sync_signal(int fd)
{
	char c = 'G';
	if (write(fd, &c, 1) != 1) {
		perror("sync_signal");
		_exit(1);
	}
}

static void sync_wait(int fd)
{
	char c;
	if (read(fd, &c, 1) != 1) {
		perror("sync_wait");
		_exit(1);
	}
}

/* --- IOCTL helpers --- */

static int do_set_default(int fd, unsigned perms)
{
	struct marufs_perm_req preq;

	memset(&preq, 0, sizeof(preq));
	preq.perms = (__u32)perms;
	return ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, &preq);
}

static int do_nrht_init(int fd, __u32 max_entries, __u32 num_shards,
			__u32 num_buckets, __u32 me_strategy)
{
	struct marufs_nrht_init_req req;

	memset(&req, 0, sizeof(req));
	req.max_entries = max_entries;
	req.num_shards = num_shards;
	req.num_buckets = num_buckets;
	req.me_strategy = me_strategy;
	return ioctl(fd, MARUFS_IOC_NRHT_INIT, &req);
}

static const char *strategy_name(__u32 s)
{
	return s == MARUFS_ME_REQUEST ? "request" : "order";
}

/* Explicit pre-warm: trigger ME instance create+join for this sbi. */
static int do_nrht_join(int fd)
{
	return ioctl(fd, MARUFS_IOC_NRHT_JOIN);
}

static int do_name_offset(int fd, const char *name, __u64 offset, int target_fd)
{
	struct marufs_name_offset_req req;

	memset(&req, 0, sizeof(req));
	strncpy(req.name, name, MARUFS_NAME_MAX);
	req.offset = offset;
	req.target_region_fd = target_fd;
	return ioctl(fd, MARUFS_IOC_NAME_OFFSET, &req);
}

static int do_find_name(int fd, const char *name, char *out_region,
			__u64 *out_offset)
{
	struct marufs_find_name_req req;
	int ret;

	memset(&req, 0, sizeof(req));
	strncpy(req.name, name, MARUFS_NAME_MAX);
	ret = ioctl(fd, MARUFS_IOC_FIND_NAME, &req);
	if (ret == 0) {
		if (out_region)
			strncpy(out_region, req.region_name,
				MARUFS_NAME_MAX + 1);
		if (out_offset)
			*out_offset = req.offset;
	}
	return ret;
}

static int do_clear_name(int fd, const char *name)
{
	struct marufs_name_offset_req req;

	memset(&req, 0, sizeof(req));
	strncpy(req.name, name, MARUFS_NAME_MAX);
	return ioctl(fd, MARUFS_IOC_CLEAR_NAME, &req);
}

/*
 * setup_nrht_region - create region file, ftruncate, init NRHT, grant perms.
 * Returns open fd on success, -1 on failure.
 */
/*
 * setup_nrht_region_sized - create region file of @slot_size and format
 * NRHT with the supplied (@max_entries, @num_shards, @num_buckets).
 * Caller picks all sizing explicitly — kernel defaults (64 shards) are
 * too fragmented for small worker counts in the benchmark.
 * Pass 0 for any param to use the kernel default for that one field.
 */
static int setup_nrht_region_sized(const char *mount, const char *filename,
				   __u32 me_strategy, __u64 slot_size,
				   __u32 max_entries, __u32 num_shards,
				   __u32 num_buckets)
{
	char path[512];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", mount, filename);
	unlink(path); /* remove stale */

	fd = open(path, O_CREAT | O_RDWR, 0644);
	if (fd < 0) {
		perror("open region");
		return -1;
	}
	if (ftruncate(fd, slot_size) < 0) {
		perror("ftruncate");
		close(fd);
		return -1;
	}
	if (do_nrht_init(fd, max_entries, num_shards, num_buckets,
			 me_strategy) < 0) {
		perror("NRHT_INIT");
		close(fd);
		return -1;
	}
	if (do_set_default(fd, MARUFS_PERM_ALL) < 0) {
		perror("PERM_SET_DEFAULT");
		close(fd);
		return -1;
	}
	return fd;
}

/*
 * setup_nrht_region - correctness-test variant. Uses small custom NRHT
 * params (1024 entries, 4 shards, 256 buckets/shard) so each round's
 * region fits in SLOT_SIZE (4 MiB). Defaults would not fit.
 */
static int setup_nrht_region(const char *mount, const char *filename,
			     __u32 me_strategy)
{
	char path[512];
	int fd;

	snprintf(path, sizeof(path), "%s/%s", mount, filename);
	unlink(path);

	fd = open(path, O_CREAT | O_RDWR, 0644);
	if (fd < 0) {
		perror("open region");
		return -1;
	}
	if (ftruncate(fd, SLOT_SIZE) < 0) {
		perror("ftruncate");
		close(fd);
		return -1;
	}
	if (do_nrht_init(fd, 1024, 4, 256, me_strategy) < 0) {
		perror("NRHT_INIT");
		close(fd);
		return -1;
	}
	if (do_set_default(fd, MARUFS_PERM_ALL) < 0) {
		perror("PERM_SET_DEFAULT");
		close(fd);
		return -1;
	}
	return fd;
}

/*
 * open_region - open an existing region file from a given mount point.
 * Returns open fd on success, -1 on failure.
 */
static int open_region(const char *mount, const char *filename)
{
	char path[512];

	snprintf(path, sizeof(path), "%s/%s", mount, filename);
	return open(path, O_RDWR);
}

/*
 * unlink_region - remove a region file from a mount point.
 */
static void unlink_region(const char *mount, const char *filename)
{
	char path[512];

	snprintf(path, sizeof(path), "%s/%s", mount, filename);
	unlink(path);
}

/* ======================================================================
 * Test 1: Same-name insert race
 *
 * NUM_WORKERS children all try to insert the same name into the NRHT.
 * Exactly one must succeed (ret==0); the rest must fail with EEXIST.
 * ====================================================================== */
/*
 * worker_mount - map worker index @wi onto a mount point.
 * Worker 0 stays on mounts[0] (the setup mount); others round-robin across
 * the remaining mounts. With num_mounts == 1, everyone uses mounts[0].
 */
static const char *worker_mount(char **mounts, int num_mounts, int wi)
{
	if (num_mounts <= 1)
		return mounts[0];
	return mounts[wi % num_mounts];
}

static void run_test1(char **mounts, int num_mounts, int round,
		      __u32 me_strategy)
{
	char region_name[64];
	char race_name[64];
	char label[128];
	int region_fd;
	/* ready_pipes[i][0..1]: child[i] signals parent it's ready */
	int ready_pipes[NUM_WORKERS][2];
	/* go_pipes[i][0..1]: parent signals child[i] to go */
	int go_pipes[NUM_WORKERS][2];
	/* result_pipe[0..1]: children write 1 byte result each */
	int result_pipe[2];
	pid_t pids[NUM_WORKERS];
	int i;
	int success_count = 0;
	int eexist_count = 0;
	__u64 found_offset = 0;
	int lookup_ok;

	snprintf(region_name, sizeof(region_name), "t1_%s_r%d_region",
		 strategy_name(me_strategy), round);
	snprintf(race_name, sizeof(race_name), "race_same_%s_r%d",
		 strategy_name(me_strategy), round);

	region_fd = setup_nrht_region(mounts[0], region_name, me_strategy);
	if (region_fd < 0) {
		fprintf(stderr, "Test1: setup failed\n");
		fail_count++;
		return;
	}

	if (pipe(result_pipe) < 0) {
		perror("pipe result");
		close(region_fd);
		unlink_region(mounts[0], region_name);
		fail_count++;
		return;
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
			perror("pipe");
			close(region_fd);
			unlink_region(mounts[0], region_name);
			fail_count++;
			return;
		}
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		pids[i] = fork();
		if (pids[i] < 0) {
			perror("fork");
			_exit(1);
		}

		if (pids[i] == 0) {
			/* Child: open region via mount2 for cross-node simulation */
			int child_fd;
			int ret;
			unsigned char result;
			int j;

			/* Close unused pipe ends */
			for (j = 0; j < NUM_WORKERS; j++) {
				close(ready_pipes[j][0]);
				close(go_pipes[j][1]);
				if (j != i) {
					close(ready_pipes[j][1]);
					close(go_pipes[j][0]);
				}
			}
			close(result_pipe[0]);
			close(region_fd);

			child_fd =
				open_region(worker_mount(mounts, num_mounts, i),
					    region_name);
			if (child_fd < 0) {
				/* fallback to mount1 if mount2 is same path */
				child_fd = open_region(mounts[0], region_name);
			}
			if (child_fd < 0) {
				result = 0xFF; /* signal error */
				write(result_pipe[1], &result, 1);
				_exit(1);
			}

			/* Signal ready */
			sync_signal(ready_pipes[i][1]);
			/* Wait for go */
			sync_wait(go_pipes[i][0]);

			/*
             * All workers race to insert the same name.
             * offset is child index * 4096 so parent can verify which one won.
             */
			ret = do_name_offset(child_fd, race_name,
					     (__u64)i * 4096, child_fd);

			/* Encode result: 0 = success, errno value on failure */
			if (ret == 0)
				result = 0;
			else
				result = (unsigned char)(errno & 0xFF);

			write(result_pipe[1], &result, 1);
			close(child_fd);
			close(ready_pipes[i][1]);
			close(go_pipes[i][0]);
			close(result_pipe[1]);
			_exit(0);
		}
	}

	/* Parent: close child ends */
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][1]);
		close(go_pipes[i][0]);
	}
	close(result_pipe[1]);

	/* Wait for all children to signal ready */
	for (i = 0; i < NUM_WORKERS; i++)
		sync_wait(ready_pipes[i][0]);

	/* Release all children simultaneously */
	for (i = 0; i < NUM_WORKERS; i++)
		sync_signal(go_pipes[i][1]);

	/* Collect results */
	for (i = 0; i < NUM_WORKERS; i++) {
		unsigned char result;

		if (read(result_pipe[0], &result, 1) == 1) {
			if (result == 0)
				success_count++;
			else if (result == (unsigned char)(EEXIST & 0xFF))
				eexist_count++;
			/* else: other error, counted implicitly */
		}
	}

	/* Wait for all children */
	for (i = 0; i < NUM_WORKERS; i++)
		waitpid(pids[i], NULL, 0);

	close(result_pipe[0]);
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][0]);
		close(go_pipes[i][1]);
	}

	/* Verify: exactly 1 winner */
	snprintf(label, sizeof(label), "Test1 [%s] r%d: exactly 1 insert wins",
		 strategy_name(me_strategy), round);
	TEST(label, success_count == 1);

	snprintf(
		label, sizeof(label),
		"Test1 [%s] r%d: others get EEXIST (got %d eexist, %d success)",
		strategy_name(me_strategy), round, eexist_count, success_count);
	TEST(label, eexist_count == NUM_WORKERS - 1);

	/* Lookup must succeed */
	lookup_ok = do_find_name(region_fd, race_name, NULL, &found_offset);
	snprintf(label, sizeof(label),
		 "Test1 [%s] r%d: name is findable after race",
		 strategy_name(me_strategy), round);
	TEST(label, lookup_ok == 0);

	/* offset must be one of the valid child offsets (multiple of 4096) */
	if (lookup_ok == 0) {
		snprintf(label, sizeof(label),
			 "Test1 [%s] r%d: found offset is valid child offset",
			 strategy_name(me_strategy), round);
		TEST(label, found_offset % 4096 == 0 &&
				    found_offset < (__u64)NUM_WORKERS * 4096);
	}

	/* Cleanup */
	do_clear_name(region_fd, race_name);
	close(region_fd);
	unlink_region(mounts[0], region_name);
}

/* ======================================================================
 * Test 2: Different-name insert race
 *
 * NUM_WORKERS children each insert a UNIQUE name.
 * All must succeed; parent verifies each name is findable.
 * ====================================================================== */
static void run_test2(char **mounts, int num_mounts, int round,
		      __u32 me_strategy)
{
	char region_name[64];
	char label[128];
	int region_fd;
	int ready_pipes[NUM_WORKERS][2];
	int go_pipes[NUM_WORKERS][2];
	int result_pipe[2];
	pid_t pids[NUM_WORKERS];
	int i;
	int success_count = 0;

	snprintf(region_name, sizeof(region_name), "t2_%s_r%d_region",
		 strategy_name(me_strategy), round);

	region_fd = setup_nrht_region(mounts[0], region_name, me_strategy);
	if (region_fd < 0) {
		fprintf(stderr, "Test2: setup failed\n");
		fail_count++;
		return;
	}

	if (pipe(result_pipe) < 0) {
		perror("pipe result");
		close(region_fd);
		unlink_region(mounts[0], region_name);
		fail_count++;
		return;
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
			perror("pipe");
			close(region_fd);
			unlink_region(mounts[0], region_name);
			fail_count++;
			return;
		}
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		pids[i] = fork();
		if (pids[i] < 0) {
			perror("fork");
			_exit(1);
		}

		if (pids[i] == 0) {
			int child_fd;
			int ret;
			unsigned char result;
			char unique_name[64];
			int j;

			for (j = 0; j < NUM_WORKERS; j++) {
				close(ready_pipes[j][0]);
				close(go_pipes[j][1]);
				if (j != i) {
					close(ready_pipes[j][1]);
					close(go_pipes[j][0]);
				}
			}
			close(result_pipe[0]);
			close(region_fd);

			child_fd =
				open_region(worker_mount(mounts, num_mounts, i),
					    region_name);
			if (child_fd < 0)
				child_fd = open_region(mounts[0], region_name);
			if (child_fd < 0) {
				result = 0xFF;
				write(result_pipe[1], &result, 1);
				_exit(1);
			}

			snprintf(unique_name, sizeof(unique_name),
				 "race_diff_%s_r%d_w%d",
				 strategy_name(me_strategy), round, i);

			sync_signal(ready_pipes[i][1]);
			sync_wait(go_pipes[i][0]);

			ret = do_name_offset(child_fd, unique_name,
					     (__u64)i * 4096, child_fd);
			result = (ret == 0) ? 0 : (unsigned char)(errno & 0xFF);

			write(result_pipe[1], &result, 1);
			close(child_fd);
			close(ready_pipes[i][1]);
			close(go_pipes[i][0]);
			close(result_pipe[1]);
			_exit(0);
		}
	}

	/* Parent: close child ends */
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][1]);
		close(go_pipes[i][0]);
	}
	close(result_pipe[1]);

	for (i = 0; i < NUM_WORKERS; i++)
		sync_wait(ready_pipes[i][0]);

	for (i = 0; i < NUM_WORKERS; i++)
		sync_signal(go_pipes[i][1]);

	for (i = 0; i < NUM_WORKERS; i++) {
		unsigned char result;

		if (read(result_pipe[0], &result, 1) == 1 && result == 0)
			success_count++;
	}

	for (i = 0; i < NUM_WORKERS; i++)
		waitpid(pids[i], NULL, 0);

	close(result_pipe[0]);
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][0]);
		close(go_pipes[i][1]);
	}

	snprintf(label, sizeof(label),
		 "Test2 [%s] r%d: all %d unique inserts succeed (got %d)",
		 strategy_name(me_strategy), round, NUM_WORKERS, success_count);
	TEST(label, success_count == NUM_WORKERS);

	/* Verify each name is findable */
	for (i = 0; i < NUM_WORKERS; i++) {
		char unique_name[64];
		int found;

		snprintf(unique_name, sizeof(unique_name),
			 "race_diff_%s_r%d_w%d", strategy_name(me_strategy),
			 round, i);
		found = do_find_name(region_fd, unique_name, NULL, NULL);
		snprintf(label, sizeof(label),
			 "Test2 [%s] r%d: name '%s' findable",
			 strategy_name(me_strategy), round, unique_name);
		TEST(label, found == 0);

		do_clear_name(region_fd, unique_name);
	}

	close(region_fd);
	unlink_region(mounts[0], region_name);
}

/* ======================================================================
 * Test 3: Concurrent insert + delete
 *
 * Half the workers insert "race_mix_rN", half delete it.
 * After all complete: lookup returns either 0 or ENOENT — no corruption.
 * ====================================================================== */
static void run_test3(char **mounts, int num_mounts, int round,
		      __u32 me_strategy)
{
	char region_name[64];
	char mix_name[64];
	char label[128];
	int region_fd;
	int ready_pipes[NUM_WORKERS][2];
	int go_pipes[NUM_WORKERS][2];
	int result_pipe[2];
	pid_t pids[NUM_WORKERS];
	int i;
	int inserters = NUM_WORKERS / 2;
	__u64 found_offset = 0;
	int lookup_ret;
	int lookup_errno;

	snprintf(region_name, sizeof(region_name), "t3_%s_r%d_region",
		 strategy_name(me_strategy), round);
	snprintf(mix_name, sizeof(mix_name), "race_mix_%s_r%d",
		 strategy_name(me_strategy), round);

	region_fd = setup_nrht_region(mounts[0], region_name, me_strategy);
	if (region_fd < 0) {
		fprintf(stderr, "Test3: setup failed\n");
		fail_count++;
		return;
	}

	/*
     * Pre-insert the name once so deleters have something to delete.
     * This raises the probability of actual insert/delete overlap.
     */
	do_name_offset(region_fd, mix_name, 0xDEAD0000ULL, region_fd);

	if (pipe(result_pipe) < 0) {
		perror("pipe result");
		close(region_fd);
		unlink_region(mounts[0], region_name);
		fail_count++;
		return;
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
			perror("pipe");
			close(region_fd);
			unlink_region(mounts[0], region_name);
			fail_count++;
			return;
		}
	}

	for (i = 0; i < NUM_WORKERS; i++) {
		pids[i] = fork();
		if (pids[i] < 0) {
			perror("fork");
			_exit(1);
		}

		if (pids[i] == 0) {
			int child_fd;
			int ret;
			unsigned char result;
			int j;

			for (j = 0; j < NUM_WORKERS; j++) {
				close(ready_pipes[j][0]);
				close(go_pipes[j][1]);
				if (j != i) {
					close(ready_pipes[j][1]);
					close(go_pipes[j][0]);
				}
			}
			close(result_pipe[0]);
			close(region_fd);

			child_fd =
				open_region(worker_mount(mounts, num_mounts, i),
					    region_name);
			if (child_fd < 0)
				child_fd = open_region(mounts[0], region_name);
			if (child_fd < 0) {
				result = 0xFF;
				write(result_pipe[1], &result, 1);
				_exit(1);
			}

			sync_signal(ready_pipes[i][1]);
			sync_wait(go_pipes[i][0]);

			if (i < inserters) {
				/* Inserter: try to insert (may get EEXIST if another won) */
				ret = do_name_offset(child_fd, mix_name,
						     (__u64)(i + 1) * 4096,
						     child_fd);
			} else {
				/* Deleter: try to delete */
				ret = do_clear_name(child_fd, mix_name);
			}
			/* result byte: 0=success, errno byte otherwise */
			result = (ret == 0) ? 0 : (unsigned char)(errno & 0xFF);

			write(result_pipe[1], &result, 1);
			close(child_fd);
			close(ready_pipes[i][1]);
			close(go_pipes[i][0]);
			close(result_pipe[1]);
			_exit(0);
		}
	}

	/* Parent: close child ends */
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][1]);
		close(go_pipes[i][0]);
	}
	close(result_pipe[1]);

	for (i = 0; i < NUM_WORKERS; i++)
		sync_wait(ready_pipes[i][0]);

	for (i = 0; i < NUM_WORKERS; i++)
		sync_signal(go_pipes[i][1]);

	/* Drain result bytes (we don't assert individual outcomes here) */
	for (i = 0; i < NUM_WORKERS; i++) {
		unsigned char result;

		read(result_pipe[0], &result, 1);
	}

	for (i = 0; i < NUM_WORKERS; i++)
		waitpid(pids[i], NULL, 0);

	close(result_pipe[0]);
	for (i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][0]);
		close(go_pipes[i][1]);
	}

	/*
     * Post-race invariant: lookup must return either success or ENOENT.
     * Any other errno indicates NRHT corruption.
     */
	lookup_ret = do_find_name(region_fd, mix_name, NULL, &found_offset);
	lookup_errno = errno;

	snprintf(
		label, sizeof(label),
		"Test3 [%s] r%d: post-race lookup no corruption (ret=%d errno=%d)",
		strategy_name(me_strategy), round, lookup_ret,
		lookup_ret ? lookup_errno : 0);
	TEST(label, lookup_ret == 0 || lookup_errno == ENOENT);

	if (lookup_ret == 0) {
		/*
         * If found, offset must be one of the inserter offsets or the
         * pre-seeded 0xDEAD0000.
         */
		int valid_offset = (found_offset == 0xDEAD0000ULL);

		for (i = 0; i < inserters && !valid_offset; i++) {
			if (found_offset == (__u64)(i + 1) * 4096)
				valid_offset = 1;
		}
		snprintf(
			label, sizeof(label),
			"Test3 [%s] r%d: found offset 0x%llx is a valid inserter offset",
			strategy_name(me_strategy), round,
			(unsigned long long)found_offset);
		TEST(label, valid_offset);

		do_clear_name(region_fd, mix_name);
	}

	close(region_fd);
	unlink_region(mounts[0], region_name);
}

/* ======================================================================
 * main
 * ====================================================================== */
/* ======================================================================
 * Throughput benchmark mode (--bench)
 *
 * NUM_WORKERS children round-robin across mounts hammer the same shared
 * NRHT region with insert+find+delete cycles of unique names. Each ioctl
 * is timed; parent aggregates samples via shared mmap and reports
 * mean/p50/p99/p999/min/max plus throughput.
 *
 * Exercises the real kernel ME code path (cross-node grant under
 * contention) — no userspace protocol re-implementation.
 * ====================================================================== */

#define BENCH_DEFAULT_ITERS 1000
/*
 * Per-worker warmup iters before the timed loop.
 */
#define BENCH_WARMUP_ITERS 1000

/*
 * Names are generated monotonically (b_..._w<i>_<k>) without cycling —
 * this matches realistic KV-cache-style workloads where every key is
 * fresh. NRHT entry array must therefore be sized to hold all
 * (bench_iters × NUM_WORKERS) names with headroom (see BENCH_NRHT_*
 * below). Otherwise TOMBSTONEs accumulate and the EMPTY scan degrades
 * insert latency over time.
 */

/*
 * NRHT sizing for the benchmark.
 *
 * Shard count is deliberately kept small (4) rather than kernel default
 * (64). Rationale: with 8 workers, 64 shards fragments token state
 * (shard-avg = 0.125 workers) — each insert likely hits a shard whose
 * token lives on a different node, forcing a cross-node transfer.
 * 4 shards = ~2 workers/shard → tokens stay local between consecutive
 * inserts on the same shard, fast-path hits common, p99 ≤ 30 µs
 * rather than 1 ms.
 *
 * Entry/bucket counts sized so (bench_iters + warmup_iters) × NUM_WORKERS
 * fresh names fit with TOMBSTONE churn headroom.
 */
/*
 * NRHT sizing config for run_bench — single config (via CLI flags) or
 * sweep (predefined list via --sweep). Each config picks shard / entry
 * / bucket counts; slot_size is auto-computed with 2× headroom.
 *
 * Defaults tuned to NUM_WORKERS=8 — 8 shards for per-worker "home",
 * 65 k entries (supports ~8 k iters/worker), 2 k buckets/shard.
 */
struct bench_config {
	__u32 max_entries;
	__u32 num_shards;
	__u32 num_buckets;
};

#define BENCH_NRHT_DEFAULT_SHARDS  8
#define BENCH_NRHT_DEFAULT_ENTRIES 65536
#define BENCH_NRHT_DEFAULT_BUCKETS 16384

/*
 * Predefined sweep — varies shard count from serialization-limited (2)
 * up to scatter-limited (64) to trace the locality/parallelism trade-off.
 * Per-shard entries fixed at 8 k, buckets/shard fixed at 2 k.
 */
static const struct bench_config bench_sweep[] = {
	{ .max_entries =   16384, .num_shards =  2, .num_buckets =  4096 },
	{ .max_entries =   32768, .num_shards =  4, .num_buckets =  8192 },
	{ .max_entries =   65536, .num_shards =  8, .num_buckets = 16384 },
	{ .max_entries =  131072, .num_shards = 16, .num_buckets = 32768 },
	{ .max_entries =  262144, .num_shards = 32, .num_buckets = 65536 },
	{ .max_entries =  524288, .num_shards = 64, .num_buckets = 131072 },
};
#define BENCH_SWEEP_COUNT (sizeof(bench_sweep) / sizeof(bench_sweep[0]))

/*
 * compute_slot_size - pick a region file size that fits @cfg with
 * 2× headroom for entries + buckets + headers/ME area + page rounding.
 */
static __u64 compute_slot_size(const struct bench_config *cfg)
{
	__u64 entry_bytes = (__u64)cfg->max_entries * 128;
	__u64 bucket_bytes = (__u64)cfg->num_buckets * 4;
	__u64 needed = 2 * (entry_bytes + bucket_bytes) + 1 * 1024 * 1024;
	__u64 rounded = 4ULL * 1024 * 1024; /* 4 MiB floor */

	while (rounded < needed)
		rounded *= 2;
	return rounded;
}

static inline uint64_t now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

static int cmp_u64(const void *a, const void *b)
{
	uint64_t va = *(const uint64_t *)a;
	uint64_t vb = *(const uint64_t *)b;
	return (va > vb) - (va < vb);
}

struct op_stats {
	uint64_t mean, p50, p99, p999, min, max;
	double throughput;
};

/*
 * poll_stats - snapshot of /sys/fs/marufs/me_poll_stats (aggregated across
 * all mounts + all ME instances). Used by the benchmark to attribute CXL
 * RMB traffic + poll-thread CPU time to each test config.
 */
struct poll_stats {
	uint64_t cycles;
	uint64_t ns_total;
	uint64_t rmb_cb;
	uint64_t rmb_slot;
	uint64_t rmb_membership;
};

/* Write-any-value resets all ME counters across all mounted marufs sbis. */
static void reset_poll_stats(void)
{
	int fd = open("/sys/fs/marufs/me_poll_stats", O_WRONLY);
	if (fd < 0)
		return;
	ssize_t w = write(fd, "0\n", 2);
	(void)w;
	close(fd);
}

static void read_poll_stats(struct poll_stats *out)
{
	memset(out, 0, sizeof(*out));
	FILE *f = fopen("/sys/fs/marufs/me_poll_stats", "r");
	if (!f)
		return;
	char line[512];
	while (fgets(line, sizeof(line), f)) {
		const char *p;
		uint64_t v;

		if ((p = strstr(line, "cycles=")) &&
		    sscanf(p, "cycles=%lu", &v) == 1)
			out->cycles += v;
		if ((p = strstr(line, "ns_total=")) &&
		    sscanf(p, "ns_total=%lu", &v) == 1)
			out->ns_total += v;
		if ((p = strstr(line, "rmb_cb=")) &&
		    sscanf(p, "rmb_cb=%lu", &v) == 1)
			out->rmb_cb += v;
		if ((p = strstr(line, "rmb_slot=")) &&
		    sscanf(p, "rmb_slot=%lu", &v) == 1)
			out->rmb_slot += v;
		if ((p = strstr(line, "rmb_membership=")) &&
		    sscanf(p, "rmb_membership=%lu", &v) == 1)
			out->rmb_membership += v;
	}
	fclose(f);
}

struct bench_result {
	struct bench_config cfg;
	char strategy[16];
	uint64_t wall_ns;
	struct op_stats insert, find, del;
	struct poll_stats poll;
	int valid;
};

static void report_stats(const char *label, uint64_t *samples, int n,
			 uint64_t wall_ns, struct op_stats *out)
{
	if (n <= 0) {
		printf("  %-10s n=0 (no samples)\n", label);
		if (out)
			memset(out, 0, sizeof(*out));
		return;
	}

	qsort(samples, (size_t)n, sizeof(uint64_t), cmp_u64);

	uint64_t sum = 0;
	for (int i = 0; i < n; i++)
		sum += samples[i];

	struct op_stats s = {
		.mean = sum / (uint64_t)n,
		.p50  = samples[n / 2],
		.p99  = samples[(int)((double)(n - 1) * 0.99)],
		.p999 = samples[(int)((double)(n - 1) * 0.999)],
		.min  = samples[0],
		.max  = samples[n - 1],
		.throughput = (double)n * 1e9 / (double)wall_ns,
	};

	printf("  %-10s n=%d  mean=%lu  p50=%lu  p99=%lu  p999=%lu  "
	       "min=%lu  max=%lu  throughput=%.0f ops/s\n",
	       label, n, s.mean, s.p50, s.p99, s.p999, s.min, s.max,
	       s.throughput);

	if (out)
		*out = s;
}

/*
 * run_bench - timed insert/find/delete loop for one ME strategy.
 * @iters is per-worker iteration count; total = NUM_WORKERS * iters.
 */
static void run_bench(char **mounts, int num_mounts, int iters,
		      __u32 me_strategy, const struct bench_config *cfg,
		      struct bench_result *out)
{
	char region_name[64];
	int region_fd;
	int ready_pipes[NUM_WORKERS][2];
	int go_pipes[NUM_WORKERS][2];
	pid_t pids[NUM_WORKERS];
	size_t samples_bytes, first_bytes;
	uint64_t *insert_samples, *find_samples, *delete_samples;
	uint64_t *first_join, *first_insert_max, *warmup_total_ns;
	uint64_t wall_start, wall_elapsed;
	__u64 slot_size = compute_slot_size(cfg);

	printf("\n========================================\n");
	printf("Benchmark [%s]: iters/worker=%d, workers=%d, mounts=%d\n",
	       strategy_name(me_strategy), iters, NUM_WORKERS, num_mounts);
	printf("  NRHT: shards=%u entries=%u buckets=%u slot=%lluMB\n",
	       cfg->num_shards, cfg->max_entries, cfg->num_buckets,
	       (unsigned long long)(slot_size >> 20));
	printf("========================================\n");

	snprintf(region_name, sizeof(region_name), "bench_%s_region",
		 strategy_name(me_strategy));

	region_fd = setup_nrht_region_sized(mounts[0], region_name, me_strategy,
					    slot_size, cfg->max_entries,
					    cfg->num_shards, cfg->num_buckets);
	if (region_fd < 0) {
		fprintf(stderr, "bench: setup failed\n");
		fail_count++;
		return;
	}

	/* Shared-memory sample buffers — children write, parent reads. */
	samples_bytes = (size_t)NUM_WORKERS * (size_t)iters * sizeof(uint64_t);
	insert_samples = mmap(NULL, samples_bytes, PROT_READ | PROT_WRITE,
			      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	find_samples = mmap(NULL, samples_bytes, PROT_READ | PROT_WRITE,
			    MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	delete_samples = mmap(NULL, samples_bytes, PROT_READ | PROT_WRITE,
			      MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (insert_samples == MAP_FAILED || find_samples == MAP_FAILED ||
	    delete_samples == MAP_FAILED) {
		perror("mmap samples");
		close(region_fd);
		unlink_region(mounts[0], region_name);
		fail_count++;
		return;
	}

	/* Per-worker first-op latencies (untimed by steady-state stats) —
	 * captures the one-shot lazy ME instance creation cost separately.
	 */
	first_bytes = (size_t)NUM_WORKERS * sizeof(uint64_t);
	first_join = mmap(NULL, first_bytes, PROT_READ | PROT_WRITE,
			  MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	first_insert_max = mmap(NULL, first_bytes, PROT_READ | PROT_WRITE,
				MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	warmup_total_ns = mmap(NULL, first_bytes, PROT_READ | PROT_WRITE,
			       MAP_SHARED | MAP_ANONYMOUS, -1, 0);
	if (first_join == MAP_FAILED || first_insert_max == MAP_FAILED ||
	    warmup_total_ns == MAP_FAILED) {
		perror("mmap first_samples");
		close(region_fd);
		unlink_region(mounts[0], region_name);
		fail_count++;
		return;
	}

	for (int i = 0; i < NUM_WORKERS; i++) {
		if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
			perror("pipe");
			fail_count++;
			return;
		}
	}

	for (int i = 0; i < NUM_WORKERS; i++) {
		pids[i] = fork();
		if (pids[i] < 0) {
			perror("fork");
			_exit(1);
		}

		if (pids[i] == 0) {
			int child_fd;
			int j;

			for (j = 0; j < NUM_WORKERS; j++) {
				close(ready_pipes[j][0]);
				close(go_pipes[j][1]);
				if (j != i) {
					close(ready_pipes[j][1]);
					close(go_pipes[j][0]);
				}
			}
			close(region_fd);

			{
				const char *attempted =
					worker_mount(mounts, num_mounts, i);
				child_fd = open_region(attempted, region_name);
				if (child_fd < 0) {
					fprintf(stderr,
						"[worker %d] primary open FAILED via %s (%s) — "
						"falling back to %s\n",
						i, attempted, strerror(errno),
						mounts[0]);
					child_fd = open_region(mounts[0],
							       region_name);
				} else {
					fprintf(stderr,
						"[worker %d] opened via %s\n",
						i, attempted);
				}
				if (child_fd < 0) {
					perror("child open_region");
					_exit(1);
				}
			}

			/* Pre-warm (timed separately into first_* arrays) —
			 * excluded from steady-state stats.
			 *
			 * 1) NRHT_JOIN: force marufs_nrht_me_get → create +
			 *    ops->join on this sbi. Membership visible to
			 *    ring; per-shard token not yet transferred.
			 *
			 * 2) Warmup burst: WARMUP_ITERS insert+delete cycles
			 *    with varied names so hashes spread across all
			 *    shards. This circulates the token through every
			 *    shard at least once for this node, warms CXL
			 *    lines (bucket heads, entry arrays, CB, request
			 *    slots) into local CPU cache, and absorbs the
			 *    one-shot cross-node token-transfer latency.
			 *    first_insert[] captures the MAX single-insert
			 *    during warmup (worst-case cold cost).
			 */
			{
				char warmup_name[MARUFS_NAME_MAX + 1];
				uint64_t t0, lat;
				uint64_t warmup_start, warmup_total;
				uint64_t max_insert = 0;

				t0 = now_ns();
				do_nrht_join(child_fd);
				first_join[i] = now_ns() - t0;

				warmup_start = now_ns();
				for (int w = 0; w < BENCH_WARMUP_ITERS; w++) {
					/* Cycle names so TOMBSTONEs in the
					 * same bucket get chain-reused —
					 * otherwise entry array fills up.
					 */
					snprintf(warmup_name,
						 sizeof(warmup_name),
						 "warm_%s_w%d_%d",
						 strategy_name(me_strategy), i,
						 w);

					t0 = now_ns();
					do_name_offset(child_fd, warmup_name, 0,
						       child_fd);
					lat = now_ns() - t0;
					if (lat > max_insert)
						max_insert = lat;

					do_clear_name(child_fd, warmup_name);
				}
				warmup_total = now_ns() - warmup_start;

				first_insert_max[i] = max_insert;
				warmup_total_ns[i] = warmup_total;
			}

			sync_signal(ready_pipes[i][1]);
			sync_wait(go_pipes[i][0]);

			for (int k = 0; k < iters; k++) {
				char name[MARUFS_NAME_MAX + 1];
				uint64_t t0;
				size_t idx = (size_t)i * iters + k;

				/* Cycle names so TOMBSTONE chain_reuse keeps
				 * the NRHT entry array from filling up.
				 */
				/* Incremental unique name per iter — names never
				 * repeat. NRHT must be sized to fit all inserts
				 * (see BENCH_NRHT_* above).
				 */
				snprintf(name, sizeof(name), "b_%s_w%d_%d",
					 strategy_name(me_strategy), i, k);

				/* Keep offset within region file size (SLOT_SIZE=4MB).
				 * Raw (i*iters+k)*4096 quickly exceeds 4MB and
				 * triggers -EINVAL in nrht_insert before ME is
				 * even touched → other nodes never join.
				 */
				__u64 off = (__u64)((i * iters + k) %
						    (SLOT_SIZE / 4096)) *
					    4096;

				t0 = now_ns();
				do_name_offset(child_fd, name, off, child_fd);
				insert_samples[idx] = now_ns() - t0;

				t0 = now_ns();
				do_find_name(child_fd, name, NULL, NULL);
				find_samples[idx] = now_ns() - t0;

				t0 = now_ns();
				do_clear_name(child_fd, name);
				delete_samples[idx] = now_ns() - t0;
			}

			close(child_fd);
			_exit(0);
		}
	}

	for (int i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][1]);
		close(go_pipes[i][0]);
	}

	for (int i = 0; i < NUM_WORKERS; i++)
		sync_wait(ready_pipes[i][0]);

	/* Reset ME poll counters across all sbis, so diff captures only this
	 * bench's steady-state poll traffic.
	 */
	reset_poll_stats();

	wall_start = now_ns();
	for (int i = 0; i < NUM_WORKERS; i++)
		sync_signal(go_pipes[i][1]);

	for (int i = 0; i < NUM_WORKERS; i++)
		waitpid(pids[i], NULL, 0);
	wall_elapsed = now_ns() - wall_start;

	struct poll_stats poll_after;

	read_poll_stats(&poll_after);

	for (int i = 0; i < NUM_WORKERS; i++) {
		close(ready_pipes[i][0]);
		close(go_pipes[i][1]);
	}

	int total = NUM_WORKERS * iters;
	struct op_stats s_ins, s_find, s_del;

	printf("  wall=%.3f ms\n", wall_elapsed / 1e6);
	report_stats("insert", insert_samples, total, wall_elapsed, &s_ins);
	report_stats("find", find_samples, total, wall_elapsed, &s_find);
	report_stats("delete", delete_samples, total, wall_elapsed, &s_del);

	uint64_t avg_cycle_ns =
		poll_after.cycles ? poll_after.ns_total / poll_after.cycles : 0;
	printf("  poll: cycles=%lu ns_total=%lu ns_avg=%lu rmb_cb=%lu rmb_slot=%lu rmb_membership=%lu\n",
	       poll_after.cycles, poll_after.ns_total, avg_cycle_ns,
	       poll_after.rmb_cb, poll_after.rmb_slot,
	       poll_after.rmb_membership);

	if (out) {
		out->cfg = *cfg;
		snprintf(out->strategy, sizeof(out->strategy), "%s",
			 strategy_name(me_strategy));
		out->wall_ns = wall_elapsed;
		out->insert = s_ins;
		out->find = s_find;
		out->del = s_del;
		out->poll = poll_after;
		out->valid = 1;
	}

	/* First-op latencies (excluded from steady-state stats above) —
	 * includes the one-shot marufs_me_create + ops->join cost per
	 * worker's sbi, so worker 0 (same sbi as parent, cache hit) is
	 * cheap while worker 1..N-1 pay the create+join cost.
	 */
	printf("  pre-warm per worker (NRHT_JOIN + %d insert/delete; excluded from steady-state):\n",
	       BENCH_WARMUP_ITERS);
	printf("    %-6s %12s %16s %16s\n", "worker", "join(ns)",
	       "warmup_total(ns)", "max_insert(ns)");
	for (int w = 0; w < NUM_WORKERS; w++)
		printf("    %-6d %12lu %16lu %16lu\n", w, first_join[w],
		       warmup_total_ns[w], first_insert_max[w]);

	munmap(insert_samples, samples_bytes);
	munmap(find_samples, samples_bytes);
	munmap(delete_samples, samples_bytes);
	munmap(first_join, first_bytes);
	munmap(first_insert_max, first_bytes);
	munmap(warmup_total_ns, first_bytes);

	close(region_fd);
	unlink_region(mounts[0], region_name);
}

/*
 * run_all_tests - run the 3 race tests @rounds times for one ME strategy.
 */
static void run_all_tests(char **mounts, int num_mounts, int rounds,
			  __u32 me_strategy)
{
	int r;

	printf("\n========================================\n");
	printf("ME strategy: %s\n", strategy_name(me_strategy));
	printf("========================================\n");

	printf("\n--- Test 1 [%s]: Same-name insert race (1 winner) ---\n",
	       strategy_name(me_strategy));
	for (r = 0; r < rounds; r++)
		run_test1(mounts, num_mounts, r, me_strategy);

	printf("\n--- Test 2 [%s]: Different-name insert race (all win) ---\n",
	       strategy_name(me_strategy));
	for (r = 0; r < rounds; r++)
		run_test2(mounts, num_mounts, r, me_strategy);

	printf("\n--- Test 3 [%s]: Concurrent insert + delete (no corruption) ---\n",
	       strategy_name(me_strategy));
	for (r = 0; r < rounds; r++)
		run_test3(mounts, num_mounts, r, me_strategy);
}

/*
 * parse_strategy - map "order"/"request"/"both" → mask of strategies to run.
 * Returns 0 on success (with *mask filled), -1 on invalid token.
 */
static int parse_strategy(const char *s, unsigned *mask)
{
	if (!s || !strcmp(s, "both")) {
		*mask = (1u << MARUFS_ME_ORDER) | (1u << MARUFS_ME_REQUEST);
		return 0;
	}
	if (!strcmp(s, "order")) {
		*mask = 1u << MARUFS_ME_ORDER;
		return 0;
	}
	if (!strcmp(s, "request")) {
		*mask = 1u << MARUFS_ME_REQUEST;
		return 0;
	}
	return -1;
}

#define MAX_MOUNTS 8

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s [--rounds N] [--strategy order|request|both]\n"
		"       %*s [--bench] [--bench-iters N]\n"
		"       %*s <mount1> <mount2> [... up to %d mounts]\n"
		"  Workers fork across mounts round-robin to simulate N nodes.\n"
		"  --bench: run throughput benchmark (insert/find/delete timing)\n"
		"           instead of correctness races.\n"
		"  Legacy positional form still supported:\n"
		"    %s <mount1> <mount2> [rounds] [strategy]\n",
		prog, (int)strlen(prog), "", (int)strlen(prog), "", MAX_MOUNTS,
		prog);
}

int main(int argc, char *argv[])
{
	char *mounts[MAX_MOUNTS];
	int num_mounts = 0;
	int rounds = DEFAULT_ROUNDS;
	const char *strat_arg = NULL;
	unsigned strategy_mask;
	int bench_mode = 0;
	int bench_iters = BENCH_DEFAULT_ITERS;
	int sweep_mode = 0;
	struct bench_config single_cfg = {
		.max_entries = BENCH_NRHT_DEFAULT_ENTRIES,
		.num_shards  = BENCH_NRHT_DEFAULT_SHARDS,
		.num_buckets = BENCH_NRHT_DEFAULT_BUCKETS,
	};

	/* Two-pass arg scan: accept --flag value AND positional mount paths.
	 * Also keeps backward compat with `m1 m2 [rounds] [strategy]`:
	 * once we have >= 2 mounts, any trailing non-flag arg that parses
	 * as an integer is treated as rounds; else as strategy.
	 */
	for (int i = 1; i < argc; i++) {
		const char *a = argv[i];

		if (!strcmp(a, "--rounds") && i + 1 < argc) {
			rounds = atoi(argv[++i]);
		} else if (!strcmp(a, "--strategy") && i + 1 < argc) {
			strat_arg = argv[++i];
		} else if (!strcmp(a, "--bench")) {
			bench_mode = 1;
		} else if (!strcmp(a, "--bench-iters") && i + 1 < argc) {
			bench_iters = atoi(argv[++i]);
		} else if (!strcmp(a, "--sweep")) {
			sweep_mode = 1;
			bench_mode = 1;
		} else if (!strcmp(a, "--shards") && i + 1 < argc) {
			single_cfg.num_shards = (__u32)atoi(argv[++i]);
		} else if (!strcmp(a, "--entries") && i + 1 < argc) {
			single_cfg.max_entries = (__u32)atoi(argv[++i]);
		} else if (!strcmp(a, "--buckets") && i + 1 < argc) {
			single_cfg.num_buckets = (__u32)atoi(argv[++i]);
		} else if (!strcmp(a, "-h") || !strcmp(a, "--help")) {
			usage(argv[0]);
			return 0;
		} else if (a[0] == '-') {
			fprintf(stderr, "unknown option: %s\n", a);
			usage(argv[0]);
			return 1;
		} else if (num_mounts >= 2 && a[0] != '/') {
			/* legacy positional: 3rd arg = rounds (int), 4th = strategy */
			char *endp;
			long v = strtol(a, &endp, 10);
			if (*endp == '\0')
				rounds = (int)v;
			else
				strat_arg = a;
		} else {
			if (num_mounts >= MAX_MOUNTS) {
				fprintf(stderr, "too many mounts (max %d)\n",
					MAX_MOUNTS);
				return 1;
			}
			mounts[num_mounts++] = (char *)a;
		}
	}

	if (num_mounts < 2) {
		fprintf(stderr, "need at least 2 mounts\n");
		usage(argv[0]);
		return 1;
	}
	if (rounds <= 0)
		rounds = DEFAULT_ROUNDS;

	if (parse_strategy(strat_arg, &strategy_mask) < 0) {
		fprintf(stderr,
			"invalid strategy '%s' (use order|request|both)\n",
			strat_arg);
		return 1;
	}

	setbuf(stdout, NULL);

	if (bench_iters <= 0)
		bench_iters = BENCH_DEFAULT_ITERS;

	printf("========================================\n");
	printf("MARUFS NRHT %s\n",
	       bench_mode ? "Throughput Benchmark" : "Concurrent Race Test");
	printf("========================================\n");
	for (int i = 0; i < num_mounts; i++)
		printf("Mount %d: %s\n", i, mounts[i]);
	if (bench_mode)
		printf("Workers: %d (round-robin across %d mount(s)), "
		       "Iters/worker: %d\n",
		       NUM_WORKERS, num_mounts, bench_iters);
	else
		printf("Workers: %d (round-robin across %d mount(s)), "
		       "Rounds: %d\n",
		       NUM_WORKERS, num_mounts, rounds);
	printf("========================================\n");

	if (bench_mode) {
		size_t n_cfg = sweep_mode ? BENCH_SWEEP_COUNT : 1;
		const struct bench_config *cfgs =
			sweep_mode ? bench_sweep : &single_cfg;
		struct bench_result results[BENCH_SWEEP_COUNT * 2];
		size_t n_results = 0;

		memset(results, 0, sizeof(results));

		for (size_t c = 0; c < n_cfg; c++) {
			if (sweep_mode)
				printf("\n### sweep config %zu/%zu: "
				       "shards=%u entries=%u buckets=%u ###\n",
				       c + 1, n_cfg, cfgs[c].num_shards,
				       cfgs[c].max_entries,
				       cfgs[c].num_buckets);
			if (strategy_mask & (1u << MARUFS_ME_ORDER))
				run_bench(mounts, num_mounts, bench_iters,
					  MARUFS_ME_ORDER, &cfgs[c],
					  &results[n_results++]);
			if (strategy_mask & (1u << MARUFS_ME_REQUEST))
				run_bench(mounts, num_mounts, bench_iters,
					  MARUFS_ME_REQUEST, &cfgs[c],
					  &results[n_results++]);
		}

		/* ── Summary table ──────────────────────────────── */
		printf("\n========================================\n");
		printf("Summary (iters/worker=%d, workers=%d, mounts=%d)\n",
		       bench_iters, NUM_WORKERS, num_mounts);
		printf("========================================\n");
		printf("%-8s %-8s %-8s %-8s %-8s   "
		       "%-s\n",
		       "strat", "shards", "entries", "buckets", "wall_ms",
		       "insert: mean/p50/p99 (ns)    "
		       "find:   mean/p99 (ns)   "
		       "delete: mean/p99 (ns)   "
		       "ops/s");
		for (size_t r = 0; r < n_results; r++) {
			struct bench_result *br = &results[r];

			if (!br->valid)
				continue;
			printf("%-8s %-8u %-8u %-8u %-8.2f   "
			       "%8lu %8lu %8lu   "
			       "%8lu %8lu   "
			       "%8lu %8lu   "
			       "%.0f\n",
			       br->strategy, br->cfg.num_shards,
			       br->cfg.max_entries, br->cfg.num_buckets,
			       br->wall_ns / 1e6,
			       br->insert.mean, br->insert.p50, br->insert.p99,
			       br->find.mean,   br->find.p99,
			       br->del.mean,    br->del.p99,
			       br->insert.throughput);
		}

		/* Poll-thread cost table — CXL RMB counts and ns spent per
		 * ops->poll_cycle() across every mounted sbi. Numbers are the
		 * delta accumulated during each timed run (reset at bench
		 * start). Useful for measuring polling overhead independently
		 * of application throughput.
		 *
		 * Raw totals grow with cycle count; the *_/c columns (per-cycle
		 * rates) are the apples-to-apples efficiency metric when
		 * comparing code versions — a faster poll_cycle lets more
		 * cycles fit into the same wall time, inflating totals even
		 * when per-cycle cost drops.
		 */
		printf("\n--- poll-thread cost (aggregated across all mounts) ---\n");
		printf("%-8s %-8s %-8s %12s %10s %10s %10s %10s %10s %10s\n",
		       "strat", "shards", "entries", "cycles", "ns_avg",
		       "cb/c", "slot/c", "mem/c", "rmb_slot", "rmb_mem");
		for (size_t r = 0; r < n_results; r++) {
			struct bench_result *br = &results[r];

			if (!br->valid)
				continue;
			uint64_t c = br->poll.cycles;
			double ns_avg = c ? (double)br->poll.ns_total / c : 0;
			double cb_pc = c ? (double)br->poll.rmb_cb / c : 0;
			double slot_pc = c ? (double)br->poll.rmb_slot / c : 0;
			double mem_pc =
				c ? (double)br->poll.rmb_membership / c : 0;

			printf("%-8s %-8u %-8u %12lu %10.1f %10.2f %10.2f %10.2f %10lu %10lu\n",
			       br->strategy, br->cfg.num_shards,
			       br->cfg.max_entries, c, ns_avg, cb_pc, slot_pc,
			       mem_pc, br->poll.rmb_slot,
			       br->poll.rmb_membership);
		}
	} else {
		if (strategy_mask & (1u << MARUFS_ME_ORDER))
			run_all_tests(mounts, num_mounts, rounds,
				      MARUFS_ME_ORDER);
		if (strategy_mask & (1u << MARUFS_ME_REQUEST))
			run_all_tests(mounts, num_mounts, rounds,
				      MARUFS_ME_REQUEST);
	}

	printf("\n========================================\n");
	if (bench_mode)
		printf("Benchmark done (failures: %d)\n", fail_count);
	else
		printf("Results: %d passed, %d failed\n", pass_count,
		       fail_count);
	printf("========================================\n");
	return (fail_count > 0) ? 1 : 0;
}
