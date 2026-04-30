// SPDX-License-Identifier: Apache-2.0
/*
 * test_cross_process.c - Multi-node file visibility test for MARUFS
 *
 * Simulates cross-node scenario using two processes with separate
 * mount points (e.g., /mnt/marufs1 node_id=1, /mnt/marufs2 node_id=2).
 * Process A operates on mount_a, Process B observes via mount_b.
 * Both mount points must be backed by the same CXL shared memory.
 *
 * Step-by-step synchronization via pipe:
 *   Process A (mount_a)             Process B (mount_b)
 *   ────────────────────            ────────────────────
 *   Step 1: create + perm    →     Step 1: stat (verify exists, size=0)
 *   Step 2: ftruncate(4KB)   →     Step 2: open + read (verify size=4KB)
 *   Step 3: mmap + write     →     Step 3: read (verify data=0xAB)
 *   Step 4: unlink            →     Step 4: stat (verify ENOENT)
 *
 * Usage: ./test_cross_process <mount_a> <mount_b>
 *   e.g., ./test_cross_process /mnt/marufs1 /mnt/marufs2
 *
 * For single-mount testing (same path for both):
 *   ./test_cross_process /mnt/marufs /mnt/marufs
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

#include "marufs_uapi.h"

#define TEST_FILE "cross_node_test"
#define TEST_SIZE (2 * 1024 * 1024)  /* 2MB: DEV_DAX requires PMD alignment */
#define PATTERN   0xAB

static int pass_count = 0;
static int fail_count = 0;

#define CHECK(cond, fmt, ...)                                           \
    do {                                                                \
        if (cond) {                                                     \
            printf("  [PASS] " fmt "\n", ##__VA_ARGS__);                \
            pass_count++;                                               \
        } else {                                                        \
            printf("  [FAIL] " fmt "\n", ##__VA_ARGS__);                \
            fail_count++;                                               \
        }                                                               \
    } while (0)

/* Synchronization: write a byte to signal the other process */
static void sync_signal(int fd)
{
    char c = 'G';
    if (write(fd, &c, 1) != 1) {
        perror("sync_signal write");
        exit(1);
    }
}

/* Synchronization: block until the other process signals */
static void sync_wait(int fd)
{
    char c;
    if (read(fd, &c, 1) != 1) {
        perror("sync_wait read");
        exit(1);
    }
}

/*
 * Process A (parent): operates on mount_a
 * Creates, truncates, writes, unlinks
 */
static int run_process_a(const char *filepath_a, int sig_fd, int wait_fd)
{
    int fd;
    void *map;

    printf("\n=== Step 1: Node A creates file ===\n");
    printf("  path: %s\n", filepath_a);
    fd = open(filepath_a, O_CREAT | O_RDWR, 0644);
    CHECK(fd >= 0, "open(O_CREAT) returned fd=%d (errno=%d)", fd, errno);
    if (fd < 0)
        return 1;
    /* Grant read permission to all non-owner nodes */
    {
        struct marufs_perm_req preq = {0};
        int pret;

        preq.perms = MARUFS_PERM_READ;
        pret = ioctl(fd, MARUFS_IOC_PERM_SET_DEFAULT, &preq);
        CHECK(pret == 0, "PERM_SET_DEFAULT(READ) ret=%d errno=%d", pret, errno);
    }
    close(fd);

    sync_signal(sig_fd);  /* -> B: file created + permission set */
    sync_wait(wait_fd);   /* <- B: stat done */

    printf("\n=== Step 2: Node A ftruncate(4KB) ===\n");
    fd = open(filepath_a, O_RDWR);
    CHECK(fd >= 0, "re-open for ftruncate fd=%d", fd);
    if (fd < 0)
        return 1;

    {
        int ret = ftruncate(fd, TEST_SIZE);
        CHECK(ret == 0, "ftruncate(%d) ret=%d errno=%d", TEST_SIZE, ret, errno);
    }
    close(fd);

    sync_signal(sig_fd);  /* -> B: ftruncate done */
    sync_wait(wait_fd);   /* <- B: read done */

    printf("\n=== Step 3: Node A WORM check + mmap attempt ===\n");
    fd = open(filepath_a, O_RDWR);
    CHECK(fd >= 0, "re-open for write/mmap fd=%d", fd);
    if (fd < 0)
        return 1;

    /* WORM: write() must be rejected */
    {
        char wbuf[64];
        ssize_t n;

        memset(wbuf, PATTERN, sizeof(wbuf));
        n = write(fd, wbuf, sizeof(wbuf));
        CHECK(n < 0 && errno == EACCES,
              "WORM write() n=%zd errno=%d (expected -1/EACCES)", n, errno);
    }

    /* mmap: works on DEV_DAX, may fail on other setups */
    map = mmap(NULL, TEST_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED) {
        printf("  [SKIP] mmap failed errno=%d (requires DEV_DAX mode)\n", errno);
        close(fd);
        /* Signal B to verify zero-filled data from ftruncate */
        sync_signal(sig_fd);
        sync_wait(wait_fd);
    } else {
        memset(map, PATTERN, TEST_SIZE);
        __builtin_ia32_sfence();  /* Flush WC buffers to memory */
        printf("  wrote 0x%02X to %d bytes via mmap\n", PATTERN, TEST_SIZE);
        munmap(map, TEST_SIZE);
        close(fd);

        sync_signal(sig_fd);  /* -> B: mmap write done */
        sync_wait(wait_fd);   /* <- B: read verification done */
    }

    printf("\n=== Step 4: Node A unlink ===\n");
    {
        int ret = unlink(filepath_a);
        CHECK(ret == 0, "unlink ret=%d errno=%d", ret, errno);
    }

    sync_signal(sig_fd);  /* -> B: unlink done */
    sync_wait(wait_fd);   /* <- B: verification done */

    return 0;
}

/*
 * Process B (child): observes A's changes via mount_b
 * Each step opens a fresh fd on the mount_b path
 */
static int run_process_b(const char *filepath_b, int sig_fd, int wait_fd)
{
    printf("\n--- Step 1: Node B checks file exists ---\n");
    printf("  path: %s\n", filepath_b);
    sync_wait(wait_fd);   /* <- A: file created */

    {
        struct stat st;
        int ret = stat(filepath_b, &st);
        CHECK(ret == 0, "stat after create: ret=%d errno=%d", ret, errno);
        if (ret == 0)
            CHECK(st.st_size == 0, "initial size=%lld (expected 0)",
                  (long long)st.st_size);
    }

    sync_signal(sig_fd);  /* -> A: stat done */

    printf("\n--- Step 2: Node B reads after ftruncate ---\n");
    sync_wait(wait_fd);   /* <- A: ftruncate done */

    {
        struct stat st;
        int ret = stat(filepath_b, &st);
        CHECK(ret == 0, "stat after ftruncate: ret=%d errno=%d", ret, errno);
        if (ret == 0)
            CHECK(st.st_size == TEST_SIZE,
                  "size after ftruncate=%lld (expected %d)",
                  (long long)st.st_size, TEST_SIZE);
    }

    /* Open with fresh fd on mount_b and verify via mmap */
    {
        int fd;
        void *map;

        fd = open(filepath_b, O_RDONLY);
        CHECK(fd >= 0, "open(O_RDONLY) fd=%d errno=%d", fd, errno);
        if (fd >= 0) {
            map = mmap(NULL, TEST_SIZE, PROT_READ, MAP_SHARED, fd, 0);
            CHECK(map != MAP_FAILED,
                  "mmap(PROT_READ) after ftruncate: %s",
                  map == MAP_FAILED ? strerror(errno) : "ok");
            if (map != MAP_FAILED)
                munmap(map, TEST_SIZE);
            close(fd);
        }
    }

    sync_signal(sig_fd);  /* -> A: read done */

    printf("\n--- Step 3: Node B reads mmap-written data ---\n");
    sync_wait(wait_fd);   /* <- A: mmap write done */

    {
        int fd;
        volatile unsigned char *map;
        int mismatch = 0;

        fd = open(filepath_b, O_RDONLY);
        CHECK(fd >= 0, "open(O_RDONLY) fd=%d errno=%d", fd, errno);
        if (fd >= 0) {
            map = mmap(NULL, TEST_SIZE, PROT_READ, MAP_SHARED, fd, 0);
            CHECK(map != MAP_FAILED,
                  "mmap(PROT_READ) for data verify: %s",
                  map == MAP_FAILED ? strerror(errno) : "ok");

            if (map != MAP_FAILED) {
                int i;
                for (i = 0; i < TEST_SIZE; i++) {
                    if (map[i] != PATTERN) {
                        mismatch++;
                        if (mismatch <= 3)
                            printf("    byte[%d]=0x%02X expected 0x%02X\n",
                                   i, map[i], PATTERN);
                    }
                }
                CHECK(mismatch == 0,
                      "data verification: %d/%d bytes match pattern 0x%02X",
                      TEST_SIZE - mismatch, TEST_SIZE, PATTERN);
                munmap((void *)map, TEST_SIZE);
            }
            close(fd);
        }
    }

    sync_signal(sig_fd);  /* -> A: read verification done */

    printf("\n--- Step 4: Node B verifies file is gone ---\n");
    sync_wait(wait_fd);   /* <- A: unlink done */

    {
        struct stat st;
        int ret = stat(filepath_b, &st);
        CHECK(ret != 0 && errno == ENOENT,
              "stat after unlink: ret=%d errno=%d (expected ENOENT)",
              ret, errno);
    }

    sync_signal(sig_fd);  /* -> A: verification done */

    return 0;
}

int main(int argc, char *argv[])
{
    const char *mount_a, *mount_b;
    char filepath_a[512], filepath_b[512];
    char filename[128];
    int a_to_b[2], b_to_a[2];
    pid_t pid;
    int status;

    if (argc < 3) {
        fprintf(stderr,
                "Usage: %s <mount_a> <mount_b>\n"
                "\n"
                "  mount_a: mount point for Node A (writer)\n"
                "  mount_b: mount point for Node B (reader)\n"
                "\n"
                "Example (dual mount, different node_ids):\n"
                "  %s /mnt/marufs1 /mnt/marufs2\n"
                "\n"
                "Example (same mount, single node test):\n"
                "  %s /mnt/marufs /mnt/marufs\n",
                argv[0], argv[0], argv[0]);
        return 1;
    }

    mount_a = argv[1];
    mount_b = argv[2];

    snprintf(filename, sizeof(filename), "%s_%d", TEST_FILE, getpid());
    snprintf(filepath_a, sizeof(filepath_a), "%s/%s", mount_a, filename);
    snprintf(filepath_b, sizeof(filepath_b), "%s/%s", mount_b, filename);

    /* Cleanup stale files if exist */
    unlink(filepath_a);
    if (strcmp(filepath_a, filepath_b) != 0)
        unlink(filepath_b);

    if (pipe(a_to_b) < 0 || pipe(b_to_a) < 0) {
        perror("pipe");
        return 1;
    }

    printf("========================================\n");
    printf("MARUFS Multi-Node Visibility Test\n");
    printf("========================================\n");
    printf("Node A mount: %s\n", mount_a);
    printf("Node B mount: %s\n", mount_b);
    printf("Node A file:  %s\n", filepath_a);
    printf("Node B file:  %s\n", filepath_b);
    if (strcmp(mount_a, mount_b) == 0)
        printf("WARNING: same mount point — inode cache shared, "
               "cross-node bugs may not reproduce\n");
    printf("========================================\n");

    pid = fork();
    if (pid < 0) {
        perror("fork");
        return 1;
    }

    if (pid == 0) {
        /* Child = Node B (reader) */
        close(a_to_b[1]);
        close(b_to_a[0]);

        run_process_b(filepath_b, b_to_a[1], a_to_b[0]);

        close(a_to_b[0]);
        close(b_to_a[1]);

        printf("\n========================================\n");
        printf("Node B: %d passed, %d failed\n", pass_count, fail_count);
        printf("========================================\n");
        exit(fail_count > 0 ? 1 : 0);
    }

    /* Parent = Node A (writer) */
    close(a_to_b[0]);
    close(b_to_a[1]);

    run_process_a(filepath_a, a_to_b[1], b_to_a[0]);

    close(a_to_b[1]);
    close(b_to_a[0]);

    waitpid(pid, &status, 0);

    /* Cleanup */
    unlink(filepath_a);

    printf("\n========================================\n");
    printf("Node A: %d passed, %d failed\n", pass_count, fail_count);
    if (WIFEXITED(status))
        printf("Node B: exited with code %d\n", WEXITSTATUS(status));

    if (fail_count > 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
        printf("RESULT: FAIL\n");
        printf("========================================\n");
        return 1;
    }

    printf("RESULT: PASS\n");
    printf("========================================\n");
    return 0;
}
