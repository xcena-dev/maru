// SPDX-License-Identifier: Apache-2.0
/*
 * bench_name_ref.c - Microbenchmark for MARUFS name-ref ioctl operations
 *
 * Measures per-operation latency (ns) with statistics:
 *   mean, median (p50), p99, p999, min, max
 *
 * Modes:
 *   (default)   Sequential access — best-case, cache-warm
 *   --shuffle   Random access order — defeats spatial locality / prefetch
 *   --prefill N Pre-populate N entries before benchmark — longer chains,
 *               larger working set (exceeds L3 if N > ~200K)
 *
 * Usage:
 *   bench_name_ref <mount_path> [options]
 *
 * Options:
 *   -n <iterations>   Number of iterations (default: 1000)
 *   -s <size_mb>      Region size in MB (default: 64)
 *   --shuffle         Randomize access order
 *   --prefill <N>     Pre-populate N background entries
 *
 * Examples:
 *   bench_name_ref /mnt/marufs
 *   bench_name_ref /mnt/marufs -n 5000 --shuffle
 *   bench_name_ref /mnt/marufs -n 1000 --shuffle --prefill 50000
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

#define BATCH_SIZE 32
#define SLOT_SIZE  64

/* ── Timing ──────────────────────────────────────────────────────── */

static inline long long now_ns(void)
{
	struct timespec ts;
	clock_gettime(CLOCK_MONOTONIC, &ts);
	return (long long)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

/* ── Statistics ──────────────────────────────────────────────────── */

static int cmp_ll(const void *a, const void *b)
{
	long long va = *(const long long *)a;
	long long vb = *(const long long *)b;
	return (va > vb) - (va < vb);
}

struct stats {
	long long mean, median, p99, p999, min, max;
};

static struct stats compute_stats(long long *samples, int n)
{
	struct stats s = {0};
	long long sum = 0;

	if (n <= 0)
		return s;

	qsort(samples, (size_t)n, sizeof(long long), cmp_ll);

	for (int i = 0; i < n; i++)
		sum += samples[i];

	s.mean   = sum / n;
	s.median = samples[n / 2];
	s.p99    = samples[(int)((double)(n - 1) * 0.99)];
	s.p999   = samples[(int)((double)(n - 1) * 0.999)];
	s.min    = samples[0];
	s.max    = samples[n - 1];
	return s;
}

static void print_header(void)
{
	printf("  %-40s  %5s  %7s  %7s  %7s  %7s  %7s  %7s\n",
	       "operation", "n", "mean", "p50", "p99", "p999", "min", "max");
	printf("  %-40s  %5s  %7s  %7s  %7s  %7s  %7s  %7s\n",
	       "", "", "(ns)", "(ns)", "(ns)", "(ns)", "(ns)", "(ns)");
}

static void print_stats(const char *label, struct stats *s, int n)
{
	printf("  %-40s  %5d  %7lld  %7lld  %7lld  %7lld  %7lld  %7lld\n",
	       label, n, s->mean, s->median, s->p99, s->p999, s->min, s->max);
}

/* ── Shuffle (Fisher-Yates) ──────────────────────────────────────── */

static void shuffle_int(int *arr, int n)
{
	for (int i = n - 1; i > 0; i--) {
		int j = rand() % (i + 1);
		int tmp = arr[i];
		arr[i] = arr[j];
		arr[j] = tmp;
	}
}

static int *make_order(int n, int do_shuffle)
{
	int *order = malloc((size_t)n * sizeof(int));
	if (!order) return NULL;
	for (int i = 0; i < n; i++)
		order[i] = i;
	if (do_shuffle)
		shuffle_int(order, n);
	return order;
}

/* Helper: create + init NRHT file, returns fd or -1 */
static int create_nrht(const char *mount_path, int pid, __u32 me_strategy)
{
	char path[512];
	struct marufs_nrht_init_req ninit;
	int nfd, ret;

	snprintf(path, sizeof(path), "%s/bench_nrht_%d", mount_path, pid);
	unlink(path);
	nfd = open(path, O_CREAT | O_RDWR, 0644);
	if (nfd < 0) return -1;

	memset(&ninit, 0, sizeof(ninit));
	ninit.max_entries = 0; /* default 8192 */
	ninit.me_strategy = me_strategy;
	ret = ioctl(nfd, MARUFS_IOC_NRHT_INIT, &ninit);
	if (ret != 0) { close(nfd); return -1; }
	return nfd;
}

static const char *strategy_name(__u32 s)
{
	return s == MARUFS_ME_REQUEST ? "request" : "order";
}

/* ── Prefill: populate background entries for cache pressure ─────── */

static void prefill(int nrht_fd, int region_fd, int pid, int count)
{
	struct marufs_name_offset_req bent[BATCH_SIZE];
	struct marufs_batch_name_offset_req breq;
	int done = 0;

	printf("  prefilling %d background entries...", count);
	fflush(stdout);

	while (done < count) {
		int batch = BATCH_SIZE;
		if (done + batch > count)
			batch = count - done;

		for (int j = 0; j < batch; j++) {
			memset(&bent[j], 0, sizeof(bent[j]));
			snprintf(bent[j].name, sizeof(bent[j].name),
				 "pf_%d_%d", pid, done + j);
			bent[j].offset = (__u64)(done + j) * SLOT_SIZE;
			bent[j].target_region_fd = region_fd;
		}

		memset(&breq, 0, sizeof(breq));
		breq.count = (__u32)batch;
		breq.entries = (__u64)(unsigned long)bent;
		ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
		done += batch;
	}
	printf(" done\n");
}

static void prefill_cleanup(int nrht_fd, int pid, int count)
{
	for (int i = 0; i < count; i++) {
		struct marufs_name_offset_req req = {0};
		snprintf(req.name, sizeof(req.name), "pf_%d_%d", pid, i);
		ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
	}
}

/* ── Warmup ──────────────────────────────────────────────────────── */

static void warmup(int nrht_fd, int region_fd, int pid)
{
	for (int i = 0; i < 16; i++) {
		struct marufs_name_offset_req req = {0};
		snprintf(req.name, sizeof(req.name), "_w_%d_%d", pid, i);
		req.target_region_fd = region_fd;
		ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
	}
	for (int i = 0; i < 16; i++) {
		struct marufs_find_name_req freq = {0};
		snprintf(freq.name, sizeof(freq.name), "_w_%d_%d", pid, i);
		ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
	}
	for (int i = 0; i < 16; i++) {
		struct marufs_name_offset_req req = {0};
		snprintf(req.name, sizeof(req.name), "_w_%d_%d", pid, i);
		ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
	}
}

/* ── Cleanup helpers ─────────────────────────────────────────────── */

static void cleanup_single(int nrht_fd, int pid, int n, const char *pfx)
{
	for (int i = 0; i < n; i++) {
		struct marufs_name_offset_req req = {0};
		snprintf(req.name, sizeof(req.name), "%s_%d_%d", pfx, pid, i);
		ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
	}
}

static void cleanup_batch(int nrht_fd, int pid, int n, const char *pfx)
{
	for (int i = 0; i < n; i++) {
		for (int j = 0; j < BATCH_SIZE; j++) {
			struct marufs_name_offset_req req = {0};
			snprintf(req.name, sizeof(req.name),
				 "%s_%d_%d_%d", pfx, pid, i, j);
			ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
		}
	}
}

/* ── Benchmarks (single) ────────────────────────────────────────── */

static struct stats bench_single_insert(int nrht_fd, int region_fd, int pid,
					long long *s, int n, int *order)
{
	for (int i = 0; i < n; i++) {
		int idx = order[i];
		struct marufs_name_offset_req req = {0};
		long long t0;

		t0 = now_ns();
		snprintf(req.name, sizeof(req.name), "si_%d_%d", pid, idx);
		req.offset = (__u64)idx * SLOT_SIZE;
		req.target_region_fd = region_fd;

		ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
		s[i] = now_ns() - t0;
	}
	return compute_stats(s, n);
}

static struct stats bench_single_find(int nrht_fd, int pid,
				      long long *s, int n, int *order)
{
	for (int i = 0; i < n; i++) {
		int idx = order[i];
		struct marufs_find_name_req freq = {0};
		long long t0;

		t0 = now_ns();
		snprintf(freq.name, sizeof(freq.name), "si_%d_%d", pid, idx);

		ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
		s[i] = now_ns() - t0;
	}
	return compute_stats(s, n);
}

static struct stats bench_single_clear(int nrht_fd, int pid,
				       long long *s, int n, int *order)
{
	for (int i = 0; i < n; i++) {
		int idx = order[i];
		struct marufs_name_offset_req req = {0};
		long long t0;

		t0 = now_ns();
		snprintf(req.name, sizeof(req.name), "si_%d_%d", pid, idx);

		ioctl(nrht_fd, MARUFS_IOC_CLEAR_NAME, &req);
		s[i] = now_ns() - t0;
	}
	return compute_stats(s, n);
}

/* ── Benchmarks (batch) ──────────────────────────────────────────── */

static struct stats bench_batch_insert(int nrht_fd, int region_fd, int pid,
				       long long *s, int n, int *order)
{
	struct marufs_name_offset_req bent[BATCH_SIZE];
	struct marufs_batch_name_offset_req breq;

	for (int i = 0; i < n; i++) {
		int idx = order[i];
		long long t0;

		t0 = now_ns();
		for (int j = 0; j < BATCH_SIZE; j++) {
			memset(&bent[j], 0, sizeof(bent[j]));
			snprintf(bent[j].name, sizeof(bent[j].name),
				 "bi_%d_%d_%d", pid, idx, j);
			bent[j].offset =
			    (__u64)(idx * BATCH_SIZE + j) * SLOT_SIZE;
			bent[j].target_region_fd = region_fd;
		}

		memset(&breq, 0, sizeof(breq));
		breq.count = BATCH_SIZE;
		breq.entries = (__u64)(unsigned long)bent;

		ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
		s[i] = now_ns() - t0;
	}
	return compute_stats(s, n);
}

static struct stats bench_batch_find(int nrht_fd, int pid,
				     long long *s, int n, int *order)
{
	struct marufs_find_name_req bent[BATCH_SIZE];
	struct marufs_batch_find_req breq;

	for (int i = 0; i < n; i++) {
		int idx = order[i];
		long long t0;

		t0 = now_ns();
		for (int j = 0; j < BATCH_SIZE; j++) {
			memset(&bent[j], 0, sizeof(bent[j]));
			snprintf(bent[j].name, sizeof(bent[j].name),
				 "bi_%d_%d_%d", pid, idx, j);
		}

		memset(&breq, 0, sizeof(breq));
		breq.count = BATCH_SIZE;
		breq.entries = (__u64)(unsigned long)bent;

		ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME, &breq);
		s[i] = now_ns() - t0;
	}
	return compute_stats(s, n);
}

/* ── Main ────────────────────────────────────────────────────────── */

int main(int argc, char **argv)
{
	const char *mount_path = NULL;
	int iters = 1000;
	int size_mb = 64;
	int do_shuffle = 0;
	int prefill_count = 0;
	__u32 me_strategy = MARUFS_ME_REQUEST;
	char filepath[512];
	char nrht_path[512];
	int fd, nrht_fd, pid = (int)getpid();
	long long *samples;
	struct stats st, st_amort;
	int *order;

	/* Parse args */
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "--shuffle") == 0) {
			do_shuffle = 1;
		} else if (strcmp(argv[i], "--prefill") == 0 && i + 1 < argc) {
			prefill_count = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-n") == 0 && i + 1 < argc) {
			iters = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-s") == 0 && i + 1 < argc) {
			size_mb = atoi(argv[++i]);
		} else if (strcmp(argv[i], "--strategy") == 0 && i + 1 < argc) {
			const char *v = argv[++i];
			if (!strcmp(v, "order"))
				me_strategy = MARUFS_ME_ORDER;
			else if (!strcmp(v, "request"))
				me_strategy = MARUFS_ME_REQUEST;
			else {
				fprintf(stderr,
					"invalid --strategy '%s' (order|request)\n",
					v);
				return 1;
			}
		} else if (argv[i][0] != '-' && !mount_path) {
			mount_path = argv[i];
		} else {
			fprintf(stderr,
				"Usage: %s <mount> [-n iters] [-s size_mb] "
				"[--shuffle] [--prefill N] "
				"[--strategy order|request]\n", argv[0]);
			return 1;
		}
	}

	if (!mount_path) {
		fprintf(stderr,
			"Usage: %s <mount> [-n iters] [-s size_mb] "
			"[--shuffle] [--prefill N] "
			"[--strategy order|request]\n", argv[0]);
		return 1;
	}

	if (iters <= 0 || iters > 100000) {
		fprintf(stderr, "iterations must be 1..100000\n");
		return 1;
	}

	srand((unsigned)pid ^ (unsigned)time(NULL));

	/* Create region file */
	snprintf(filepath, sizeof(filepath), "%s/bench_%d", mount_path, pid);
	fd = open(filepath, O_CREAT | O_RDWR, 0644);
	if (fd < 0) { perror("open"); return 1; }
	if (ftruncate(fd, (__off_t)size_mb * 1024 * 1024) < 0) {
		perror("ftruncate");
		close(fd); unlink(filepath); return 1;
	}

	/* Create NRHT file */
	snprintf(nrht_path, sizeof(nrht_path), "%s/bench_nrht_%d", mount_path, pid);
	nrht_fd = create_nrht(mount_path, pid, me_strategy);
	if (nrht_fd < 0) {
		perror("NRHT init");
		close(fd); unlink(filepath); return 1;
	}

	samples = calloc((size_t)iters, sizeof(long long));
	if (!samples) {
		close(nrht_fd); unlink(nrht_path);
		close(fd); unlink(filepath); return 1;
	}

	printf("=== MARUFS name-ref ioctl microbenchmark ===\n");
	printf("mount=%s  region=%dMB  iterations=%d  batch=%d  pid=%d\n",
	       mount_path, size_mb, iters, BATCH_SIZE, pid);
	printf("mode=%s  prefill=%d  me_strategy=%s\n\n",
	       do_shuffle ? "SHUFFLE (random)" : "SEQUENTIAL (cache-warm)",
	       prefill_count, strategy_name(me_strategy));

	warmup(nrht_fd, fd, pid);

	/* Prefill background entries for cache pressure */
	if (prefill_count > 0)
		prefill(nrht_fd, fd, pid, prefill_count);

	/* ── [1] Single operations ───────────────────────────────── */

	printf("\n[1] Single operations (ns/call)\n");
	print_header();

	order = make_order(iters, do_shuffle);
	if (!order) {
		free(samples);
		close(nrht_fd); unlink(nrht_path);
		close(fd); unlink(filepath); return 1;
	}

	st = bench_single_insert(nrht_fd, fd, pid, samples, iters, order);
	print_stats("NAME_OFFSET insert", &st, iters);

	if (do_shuffle) shuffle_int(order, iters);
	st = bench_single_find(nrht_fd, pid, samples, iters, order);
	print_stats("FIND_NAME lookup", &st, iters);

	if (do_shuffle) shuffle_int(order, iters);
	st = bench_single_clear(nrht_fd, pid, samples, iters, order);
	print_stats("CLEAR_NAME delete", &st, iters);

	free(order);

	/* ── [2] Batch operations (bs=32) ────────────────────────── */

	printf("\n[2] Batch operations, batch_size=%d (ns/call)\n", BATCH_SIZE);
	print_header();

	order = make_order(iters, do_shuffle);
	if (!order) {
		free(samples);
		close(nrht_fd); unlink(nrht_path);
		close(fd); unlink(filepath); return 1;
	}

	st = bench_batch_insert(nrht_fd, fd, pid, samples, iters, order);
	print_stats("BATCH_NAME_OFFSET insert", &st, iters);

	if (do_shuffle) shuffle_int(order, iters);
	st = bench_batch_find(nrht_fd, pid, samples, iters, order);
	print_stats("BATCH_FIND_NAME lookup", &st, iters);

	free(order);

	/* ── [3] Amortized per-entry cost ────────────────────────── */

	printf("\n[3] Amortized per-entry cost (ns/entry, batch=%d vs single)\n",
	       BATCH_SIZE);
	print_header();

	cleanup_batch(nrht_fd, pid, iters, "bi");

	order = make_order(iters, do_shuffle);
	if (!order) {
		free(samples);
		close(nrht_fd); unlink(nrht_path);
		close(fd); unlink(filepath); return 1;
	}

	st = bench_batch_insert(nrht_fd, fd, pid, samples, iters, order);
	st_amort = st;
	st_amort.mean   /= BATCH_SIZE;
	st_amort.median /= BATCH_SIZE;
	st_amort.p99    /= BATCH_SIZE;
	st_amort.p999   /= BATCH_SIZE;
	st_amort.min    /= BATCH_SIZE;
	st_amort.max    /= BATCH_SIZE;
	print_stats("batch insert (per entry)", &st_amort, iters);

	cleanup_batch(nrht_fd, pid, iters, "bi");
	cleanup_single(nrht_fd, pid, iters, "si");

	if (do_shuffle) shuffle_int(order, iters);
	st = bench_single_insert(nrht_fd, fd, pid, samples, iters, order);
	print_stats("single insert (per entry)", &st, iters);

	if (st_amort.mean > 0) {
		printf("\n  -> batch amortized speedup: %.1fx\n",
		       (double)st.mean / (double)st_amort.mean);
	}

	free(order);

	/* ── [4] Throughput ──────────────────────────────────────── */

	printf("\n[4] Throughput\n");

	cleanup_single(nrht_fd, pid, iters, "si");

	{
		long long t0, elapsed;
		double ops;
		int *torder = make_order(iters, do_shuffle);
		if (!torder) {
			free(samples);
			close(nrht_fd); unlink(nrht_path);
			close(fd); unlink(filepath); return 1;
		}

		/* Single insert */
		t0 = now_ns();
		for (int i = 0; i < iters; i++) {
			int idx = torder[i];
			struct marufs_name_offset_req req = {0};
			long long ti = now_ns();
			snprintf(req.name, sizeof(req.name),
				 "tp_%d_%d", pid, idx);
			req.offset = (__u64)idx * SLOT_SIZE;
			req.target_region_fd = fd;
			ioctl(nrht_fd, MARUFS_IOC_NAME_OFFSET, &req);
			samples[i] = now_ns() - ti;
		}
		elapsed = now_ns() - t0;
		ops = (double)iters / ((double)elapsed / 1e9);
		st = compute_stats(samples, iters);
		printf("  NAME_OFFSET insert:       %8.0f ops/sec  p50=%lld p99=%lld p999=%lld ns\n",
		       ops, st.median, st.p99, st.p999);

		/* Single lookup */
		if (do_shuffle) shuffle_int(torder, iters);
		t0 = now_ns();
		for (int i = 0; i < iters; i++) {
			int idx = torder[i];
			struct marufs_find_name_req freq = {0};
			long long ti = now_ns();
			snprintf(freq.name, sizeof(freq.name),
				 "tp_%d_%d", pid, idx);
			ioctl(nrht_fd, MARUFS_IOC_FIND_NAME, &freq);
			samples[i] = now_ns() - ti;
		}
		elapsed = now_ns() - t0;
		ops = (double)iters / ((double)elapsed / 1e9);
		st = compute_stats(samples, iters);
		printf("  FIND_NAME lookup:         %8.0f ops/sec  p50=%lld p99=%lld p999=%lld ns\n",
		       ops, st.median, st.p99, st.p999);

		cleanup_single(nrht_fd, pid, iters, "tp");

		/* Batch insert (entries/sec) */
		{
			struct marufs_name_offset_req bent[BATCH_SIZE];
			struct marufs_batch_name_offset_req breq;

			if (do_shuffle) shuffle_int(torder, iters);
			t0 = now_ns();
			for (int i = 0; i < iters; i++) {
				int idx = torder[i];
				long long ti = now_ns();
				for (int j = 0; j < BATCH_SIZE; j++) {
					memset(&bent[j], 0, sizeof(bent[j]));
					snprintf(bent[j].name,
						 sizeof(bent[j].name),
						 "btp_%d_%d_%d", pid, idx, j);
					bent[j].offset =
					    (__u64)(idx * BATCH_SIZE + j) *
					    SLOT_SIZE;
					bent[j].target_region_fd = fd;
				}
				memset(&breq, 0, sizeof(breq));
				breq.count = BATCH_SIZE;
				breq.entries = (__u64)(unsigned long)bent;
				ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
				samples[i] = now_ns() - ti;
			}
			elapsed = now_ns() - t0;
			ops = (double)(iters * BATCH_SIZE) /
			      ((double)elapsed / 1e9);
			st = compute_stats(samples, iters);
			printf("  BATCH_NAME_OFFSET bs=32:  %8.0f entries/sec  "
			       "p50=%lld p99=%lld p999=%lld ns/batch\n",
			       ops, st.median, st.p99, st.p999);
			printf("                            %8s               "
			       "p50=%lld p99=%lld p999=%lld ns/entry\n",
			       "", st.median / BATCH_SIZE,
			       st.p99 / BATCH_SIZE, st.p999 / BATCH_SIZE);

			cleanup_batch(nrht_fd, pid, iters, "btp");
		}

		/* Batch lookup (entries/sec) */
		{
			struct marufs_name_offset_req bent[BATCH_SIZE];
			struct marufs_batch_name_offset_req breq;

			if (do_shuffle) shuffle_int(torder, iters);
			for (int i = 0; i < iters; i++) {
				int idx = torder[i];
				for (int j = 0; j < BATCH_SIZE; j++) {
					memset(&bent[j], 0, sizeof(bent[j]));
					snprintf(bent[j].name,
						 sizeof(bent[j].name),
						 "bfl_%d_%d_%d", pid, idx, j);
					bent[j].offset =
					    (__u64)(idx * BATCH_SIZE + j) *
					    SLOT_SIZE;
					bent[j].target_region_fd = fd;
				}
				memset(&breq, 0, sizeof(breq));
				breq.count = BATCH_SIZE;
				breq.entries = (__u64)(unsigned long)bent;
				ioctl(nrht_fd, MARUFS_IOC_BATCH_NAME_OFFSET, &breq);
			}

			{
				struct marufs_find_name_req fbent[BATCH_SIZE];
				struct marufs_batch_find_req fbreq;

				if (do_shuffle) shuffle_int(torder, iters);
				t0 = now_ns();
				for (int i = 0; i < iters; i++) {
					int idx = torder[i];
					long long ti = now_ns();
					for (int j = 0; j < BATCH_SIZE; j++) {
						memset(&fbent[j], 0,
						       sizeof(fbent[j]));
						snprintf(fbent[j].name,
							 sizeof(fbent[j].name),
							 "bfl_%d_%d_%d",
							 pid, idx, j);
					}
					fbreq.count = BATCH_SIZE;
					fbreq.entries =
					    (__u64)(unsigned long)fbent;
					ioctl(nrht_fd, MARUFS_IOC_BATCH_FIND_NAME,
					      &fbreq);
					samples[i] = now_ns() - ti;
				}
				elapsed = now_ns() - t0;
				ops = (double)(iters * BATCH_SIZE) /
				      ((double)elapsed / 1e9);
				st = compute_stats(samples, iters);
				printf("  BATCH_FIND_NAME bs=32:    "
				       "%8.0f entries/sec  "
				       "p50=%lld p99=%lld p999=%lld ns/batch\n",
				       ops, st.median, st.p99, st.p999);
				printf("                            "
				       "%8s               "
				       "p50=%lld p99=%lld p999=%lld ns/entry\n",
				       "", st.median / BATCH_SIZE,
				       st.p99 / BATCH_SIZE,
				       st.p999 / BATCH_SIZE);
			}

			cleanup_batch(nrht_fd, pid, iters, "bfl");
		}

		free(torder);
	}

	/* Cleanup */
	if (prefill_count > 0) {
		printf("\n  cleaning up prefill entries...");
		fflush(stdout);
		prefill_cleanup(nrht_fd, pid, prefill_count);
		printf(" done\n");
	}

	free(samples);
	close(nrht_fd);
	unlink(nrht_path);
	close(fd);
	unlink(filepath);

	printf("\nDone.\n");
	return 0;
}
