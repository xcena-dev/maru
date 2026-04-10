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
 * Usage: ./test_nrht_race <mount1> <mount2> [rounds]
 *   mount1/mount2: two mount points on the same CXL device (different node_ids)
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
#include <unistd.h>

#include "../include/marufs_uapi.h"

/* --- Test helpers --- */

static int pass_count;
static int fail_count;

#define TEST(name, expr)                                       \
    do                                                         \
    {                                                          \
        if (expr)                                              \
        {                                                      \
            printf("  PASS: %s\n", name);                      \
            pass_count++;                                      \
        }                                                      \
        else                                                   \
        {                                                      \
            printf("  FAIL: %s (errno=%d: %s)\n", name, errno, \
                   strerror(errno));                           \
            fail_count++;                                      \
        }                                                      \
    } while (0)

#define SLOT_SIZE (2ULL * 1024 * 1024)
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
                        __u32 num_buckets)
{
    struct marufs_nrht_init_req req;

    memset(&req, 0, sizeof(req));
    req.max_entries = max_entries;
    req.num_shards = num_shards;
    req.num_buckets = num_buckets;
    return ioctl(fd, MARUFS_IOC_NRHT_INIT, &req);
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
            strncpy(out_region, req.region_name, MARUFS_NAME_MAX + 1);
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
static int setup_nrht_region(const char *mount, const char *filename)
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
    if (ftruncate(fd, SLOT_SIZE) < 0) {
        perror("ftruncate");
        close(fd);
        return -1;
    }
    if (do_nrht_init(fd, 1024, 4, 256) < 0) {
        perror("NRHT_INIT");
        close(fd);
        return -1;
    }
    /* Allow any process on any node to use NRHT ioctls */
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
static void run_test1(const char *mount1, const char *mount2, int round)
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

    snprintf(region_name, sizeof(region_name), "t1_r%d_region", round);
    snprintf(race_name, sizeof(race_name), "race_same_r%d", round);

    region_fd = setup_nrht_region(mount1, region_name);
    if (region_fd < 0) {
        fprintf(stderr, "Test1: setup failed\n");
        fail_count++;
        return;
    }

    if (pipe(result_pipe) < 0) {
        perror("pipe result");
        close(region_fd);
        unlink_region(mount1, region_name);
        fail_count++;
        return;
    }

    for (i = 0; i < NUM_WORKERS; i++) {
        if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
            perror("pipe");
            close(region_fd);
            unlink_region(mount1, region_name);
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

            child_fd = open_region(mount2, region_name);
            if (child_fd < 0) {
                /* fallback to mount1 if mount2 is same path */
                child_fd = open_region(mount1, region_name);
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
    snprintf(label, sizeof(label), "Test1 r%d: exactly 1 insert wins", round);
    TEST(label, success_count == 1);

    snprintf(label, sizeof(label),
             "Test1 r%d: others get EEXIST (got %d eexist, %d success)",
             round, eexist_count, success_count);
    TEST(label, eexist_count == NUM_WORKERS - 1);

    /* Lookup must succeed */
    lookup_ok = do_find_name(region_fd, race_name, NULL, &found_offset);
    snprintf(label, sizeof(label), "Test1 r%d: name is findable after race",
             round);
    TEST(label, lookup_ok == 0);

    /* offset must be one of the valid child offsets (multiple of 4096) */
    if (lookup_ok == 0) {
        snprintf(label, sizeof(label),
                 "Test1 r%d: found offset is valid child offset", round);
        TEST(label, found_offset % 4096 == 0 &&
                    found_offset < (__u64)NUM_WORKERS * 4096);
    }

    /* Cleanup */
    do_clear_name(region_fd, race_name);
    close(region_fd);
    unlink_region(mount1, region_name);
}

/* ======================================================================
 * Test 2: Different-name insert race
 *
 * NUM_WORKERS children each insert a UNIQUE name.
 * All must succeed; parent verifies each name is findable.
 * ====================================================================== */
static void run_test2(const char *mount1, const char *mount2, int round)
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

    snprintf(region_name, sizeof(region_name), "t2_r%d_region", round);

    region_fd = setup_nrht_region(mount1, region_name);
    if (region_fd < 0) {
        fprintf(stderr, "Test2: setup failed\n");
        fail_count++;
        return;
    }

    if (pipe(result_pipe) < 0) {
        perror("pipe result");
        close(region_fd);
        unlink_region(mount1, region_name);
        fail_count++;
        return;
    }

    for (i = 0; i < NUM_WORKERS; i++) {
        if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
            perror("pipe");
            close(region_fd);
            unlink_region(mount1, region_name);
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

            child_fd = open_region(mount2, region_name);
            if (child_fd < 0)
                child_fd = open_region(mount1, region_name);
            if (child_fd < 0) {
                result = 0xFF;
                write(result_pipe[1], &result, 1);
                _exit(1);
            }

            snprintf(unique_name, sizeof(unique_name),
                     "race_diff_r%d_w%d", round, i);

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
             "Test2 r%d: all %d unique inserts succeed (got %d)",
             round, NUM_WORKERS, success_count);
    TEST(label, success_count == NUM_WORKERS);

    /* Verify each name is findable */
    for (i = 0; i < NUM_WORKERS; i++) {
        char unique_name[64];
        int found;

        snprintf(unique_name, sizeof(unique_name),
                 "race_diff_r%d_w%d", round, i);
        found = do_find_name(region_fd, unique_name, NULL, NULL);
        snprintf(label, sizeof(label),
                 "Test2 r%d: name '%s' findable", round, unique_name);
        TEST(label, found == 0);

        do_clear_name(region_fd, unique_name);
    }

    close(region_fd);
    unlink_region(mount1, region_name);
}

/* ======================================================================
 * Test 3: Concurrent insert + delete
 *
 * Half the workers insert "race_mix_rN", half delete it.
 * After all complete: lookup returns either 0 or ENOENT — no corruption.
 * ====================================================================== */
static void run_test3(const char *mount1, const char *mount2, int round)
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

    snprintf(region_name, sizeof(region_name), "t3_r%d_region", round);
    snprintf(mix_name, sizeof(mix_name), "race_mix_r%d", round);

    region_fd = setup_nrht_region(mount1, region_name);
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
        unlink_region(mount1, region_name);
        fail_count++;
        return;
    }

    for (i = 0; i < NUM_WORKERS; i++) {
        if (pipe(ready_pipes[i]) < 0 || pipe(go_pipes[i]) < 0) {
            perror("pipe");
            close(region_fd);
            unlink_region(mount1, region_name);
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

            child_fd = open_region(mount2, region_name);
            if (child_fd < 0)
                child_fd = open_region(mount1, region_name);
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
                                     (__u64)(i + 1) * 4096, child_fd);
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

    snprintf(label, sizeof(label),
             "Test3 r%d: post-race lookup no corruption (ret=%d errno=%d)",
             round, lookup_ret, lookup_ret ? lookup_errno : 0);
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
        snprintf(label, sizeof(label),
                 "Test3 r%d: found offset 0x%llx is a valid inserter offset",
                 round, (unsigned long long)found_offset);
        TEST(label, valid_offset);

        do_clear_name(region_fd, mix_name);
    }

    close(region_fd);
    unlink_region(mount1, region_name);
}

/* ======================================================================
 * main
 * ====================================================================== */
int main(int argc, char *argv[])
{
    const char *mount1;
    const char *mount2;
    int rounds;
    int r;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <mount1> <mount2> [rounds]\n", argv[0]);
        return 1;
    }

    mount1 = argv[1];
    mount2 = argv[2];
    rounds = argc >= 4 ? atoi(argv[3]) : DEFAULT_ROUNDS;
    if (rounds <= 0)
        rounds = DEFAULT_ROUNDS;

    setbuf(stdout, NULL);

    printf("========================================\n");
    printf("MARUFS NRHT Concurrent Race Test\n");
    printf("========================================\n");
    printf("Mount A: %s\n", mount1);
    printf("Mount B: %s\n", mount2);
    printf("Workers: %d, Rounds: %d\n", NUM_WORKERS, rounds);
    printf("========================================\n");

    printf("\n--- Test 1: Same-name insert race (1 winner) ---\n");
    for (r = 0; r < rounds; r++)
        run_test1(mount1, mount2, r);

    printf("\n--- Test 2: Different-name insert race (all win) ---\n");
    for (r = 0; r < rounds; r++)
        run_test2(mount1, mount2, r);

    printf("\n--- Test 3: Concurrent insert + delete (no corruption) ---\n");
    for (r = 0; r < rounds; r++)
        run_test3(mount1, mount2, r);

    printf("\n========================================\n");
    printf("Results: %d passed, %d failed\n", pass_count, fail_count);
    printf("========================================\n");
    return (fail_count > 0) ? 1 : 0;
}
