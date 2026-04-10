// SPDX-License-Identifier: Apache-2.0
/*
 * test_mmap_notrunc.c - Verify mmap on uninitialized region returns ENODATA
 *
 * Tests that mmap() on a file that has been created (O_CREAT) but NOT
 * ftruncated (region not initialized, phys_offset=0) correctly returns
 * MAP_FAILED with errno == ENODATA.
 *
 * Usage: ./test_mmap_notrunc <mount>
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

/* --- Test framework --- */

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

/* ================================================================
 * Main
 * ================================================================ */

int main(int argc, char* argv[])
{
    char filepath[512];
    const char* mount;
    int fd, ret;
    void* map;

    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <mount>\n", argv[0]);
        return 1;
    }

    mount = argv[1];
    snprintf(filepath, sizeof(filepath), "%s/notrunc_test_%d", mount,
             getpid());

    printf("=== MARUFS mmap-before-ftruncate Tests ===\n");
    printf("  mount: %s\n\n", mount);

    /* Pre-clean in case a previous run left the file behind */
    unlink(filepath);

    /* ---- [1] mmap on fresh file (no ftruncate) ---- */
    printf("[1] mmap on fresh file (no ftruncate)\n");

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT)", fd >= 0);
    if (fd < 0)
        return 1;

    errno = 0;
    map = mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED, fd, 0);
    TEST("mmap returns MAP_FAILED", map == MAP_FAILED);
    TEST("errno is ENODATA", errno == ENODATA);

    if (map != MAP_FAILED)
        munmap(map, SLOT_SIZE);

    close(fd);
    unlink(filepath);

    /* ---- [2] read on fresh file (no ftruncate) ---- */
    printf("\n[2] read on fresh file (no ftruncate)\n");

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT)", fd >= 0);
    if (fd >= 0)
    {
        char buf[16];
        ssize_t n;

        errno = 0;
        n = read(fd, buf, sizeof(buf));
        /*
         * An uninitialized region has no backing physical memory.
         * The kernel should either return -1/ENODATA or 0 bytes (EOF).
         * Both are acceptable — either signals "no data yet".
         */
        TEST("read returns -1/ENODATA or 0 bytes",
             (n == -1 && errno == ENODATA) || n == 0);

        close(fd);
        unlink(filepath);
    }

    /* ---- [3] mmap after ftruncate works ---- */
    printf("\n[3] mmap after ftruncate succeeds\n");

    fd = open(filepath, O_CREAT | O_RDWR, 0644);
    TEST("open(O_CREAT)", fd >= 0);
    if (fd >= 0)
    {
        ret = ftruncate(fd, (__off_t)SLOT_SIZE);
        TEST("ftruncate", ret == 0);

        if (ret == 0)
        {
            errno = 0;
            map = mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED, fd, 0);
            TEST("mmap succeeds after ftruncate", map != MAP_FAILED);

            if (map != MAP_FAILED)
                munmap(map, SLOT_SIZE);
        }

        close(fd);
        unlink(filepath);
    }

    /* ---- Summary ---- */
    printf("\n============================================\n");
    printf("=== Total: %d passed, %d failed ===\n", pass_count, fail_count);
    printf("============================================\n");

    return (fail_count > 0) ? 1 : 0;
}
