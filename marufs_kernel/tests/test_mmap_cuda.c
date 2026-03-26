// SPDX-License-Identifier: Apache-2.0
/*
 * test_mmap_cuda.c - MARUFS mmap permission & cudaHostRegister tests
 *
 * Validates mmap permission enforcement and GPU DMA readiness:
 *
 *   [1] Owner: mmap(PROT_WRITE) → success
 *   [2] Owner: cudaHostRegister on PROT_WRITE mapping → success (if CUDA)
 *   [3] Reader (READ only): mmap(PROT_READ) → success
 *   [4] Reader (READ only): mmap(PROT_WRITE) → fail (EACCES)
 *   [5] Reader (READ only): cudaHostRegister on PROT_READ → fail (if CUDA)
 *   [6] Reader (READ+WRITE): mmap(PROT_WRITE) → success
 *   [7] Reader (READ+WRITE): cudaHostRegister on PROT_WRITE → success (if CUDA)
 *
 * Usage:
 *   test_mmap_cuda <mount0> <mount1> <peer_node_id>
 *
 * mount0 = owner's mount point (node 0)
 * mount1 = reader's mount point (node 1)
 * peer_node_id = node_id used by mount1
 *
 * CUDA tests are optional — skipped if libcuda.so is not available.
 */

#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "../include/marufs_uapi.h"

/* --- Test framework --- */

static int pass_count;
static int fail_count;
static int skip_count;

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

#define SKIP(name, reason)                         \
    do                                             \
    {                                              \
        printf("  SKIP: %s (%s)\n", name, reason); \
        skip_count++;                              \
    } while (0)

#define SLOT_SIZE (2ULL * 1024 * 1024) /* 2MB */
#define PATTERN_A 0xDEADBEEFu

/* --- CUDA runtime function types (dynamically loaded) --- */

typedef int (*cudaHostRegister_fn)(void* ptr, size_t size, unsigned int flags);
typedef int (*cudaHostUnregister_fn)(void* ptr);
typedef int (*cudaGetDeviceCount_fn)(int* count);
typedef const char* (*cudaGetErrorString_fn)(int error);

static void* cuda_lib;
static cudaHostRegister_fn p_cudaHostRegister;
static cudaHostUnregister_fn p_cudaHostUnregister;
static cudaGetDeviceCount_fn p_cudaGetDeviceCount;
static cudaGetErrorString_fn p_cudaGetErrorString;

#define cudaHostRegisterDefault 0
#define cudaHostRegisterReadOnly 0x08
#define cudaSuccess 0

static int cuda_available(void)
{
    return cuda_lib != NULL;
}

static int cuda_init(void)
{
    int count = 0;

    cuda_lib = dlopen("libcudart.so", RTLD_LAZY);
    if (!cuda_lib)
        cuda_lib = dlopen("libcudart.so.12", RTLD_LAZY);
    if (!cuda_lib)
        cuda_lib = dlopen("libcudart.so.11", RTLD_LAZY);
    if (!cuda_lib)
        return -1;

    p_cudaHostRegister = (cudaHostRegister_fn)dlsym(cuda_lib, "cudaHostRegister");
    p_cudaHostUnregister = (cudaHostUnregister_fn)dlsym(cuda_lib, "cudaHostUnregister");
    p_cudaGetDeviceCount = (cudaGetDeviceCount_fn)dlsym(cuda_lib, "cudaGetDeviceCount");
    p_cudaGetErrorString = (cudaGetErrorString_fn)dlsym(cuda_lib, "cudaGetErrorString");

    if (!p_cudaHostRegister || !p_cudaHostUnregister || !p_cudaGetDeviceCount)
    {
        dlclose(cuda_lib);
        cuda_lib = NULL;
        return -1;
    }

    /* Check for GPU */
    if (p_cudaGetDeviceCount(&count) != cudaSuccess || count == 0)
    {
        dlclose(cuda_lib);
        cuda_lib = NULL;
        return -1;
    }

    printf("  CUDA available: %d GPU(s) detected\n", count);
    return 0;
}

static void cuda_cleanup(void)
{
    if (cuda_lib)
    {
        dlclose(cuda_lib);
        cuda_lib = NULL;
    }
}

/* --- Helper: ioctl grant --- */

static int do_grant(int fd, unsigned int node, unsigned int pid,
                    unsigned int perms)
{
    struct marufs_perm_req preq;

    memset(&preq, 0, sizeof(preq));
    preq.node_id = (__u32)node;
    preq.pid = (__u32)pid;
    preq.perms = (__u32)perms;
    return ioctl(fd, MARUFS_IOC_PERM_GRANT, &preq);
}

/* --- Test: cudaHostRegister on a mapping --- */

static int try_cuda_register(void* map, size_t size, const char* desc)
{
    int ret;

    if (!cuda_available())
    {
        SKIP(desc, "no CUDA");
        return -1; /* skip */
    }

    ret = p_cudaHostRegister(map, size, cudaHostRegisterDefault);
    if (ret == cudaSuccess)
    {
        printf("  PASS: %s (cudaHostRegister OK)\n", desc);
        pass_count++;
        p_cudaHostUnregister(map);
        return 0;
    }
    else
    {
        const char* err = p_cudaGetErrorString ? p_cudaGetErrorString(ret) : "?";
        printf("  FAIL: %s (cudaHostRegister error %d: %s)\n", desc, ret, err);
        fail_count++;
        return ret;
    }
}

static int try_cuda_register_expect_fail(void* map, size_t size, const char* desc)
{
    int ret;

    if (!cuda_available())
    {
        SKIP(desc, "no CUDA");
        return -1;
    }

    ret = p_cudaHostRegister(map, size, cudaHostRegisterDefault);
    if (ret != cudaSuccess)
    {
        const char* err = p_cudaGetErrorString ? p_cudaGetErrorString(ret) : "?";
        printf("  PASS: %s (correctly failed: %s)\n", desc, err);
        pass_count++;
        return 0;
    }
    else
    {
        printf("  FAIL: %s (should have failed but succeeded!)\n", desc);
        fail_count++;
        p_cudaHostUnregister(map);
        return -1;
    }
}

/*
 * try_cuda_register_readonly - test cudaHostRegisterReadOnly (CUDA 11.1+)
 *
 * cudaHostRegisterReadOnly (flag 0x04) hints that the device will only
 * read this memory. If NVIDIA internally skips FOLL_WRITE, this could
 * succeed even on PROT_READ-only mappings (no VM_WRITE).
 *
 * Returns: 0 = success, >0 = cuda error, -1 = skipped
 */
static int try_cuda_register_readonly(void* map, size_t size, const char* desc)
{
    int ret;

    if (!cuda_available())
    {
        SKIP(desc, "no CUDA");
        return -1;
    }

    ret = p_cudaHostRegister(map, size, cudaHostRegisterReadOnly);
    if (ret == cudaSuccess)
    {
        printf("  PASS: %s (%d) (cudaHostRegisterReadOnly OK)\n", desc, ret);
        pass_count++;
        p_cudaHostUnregister(map);
        return 0;
    }
    else
    {
        const char* err = p_cudaGetErrorString ? p_cudaGetErrorString(ret) : "?";
        printf("  INFO: %s (%d) (cudaHostRegisterReadOnly failed: %s)\n", desc, ret, err);
        /* Not counted as FAIL — we're probing behavior */
        skip_count++;
        return ret;
    }
}

/* ================================================================ */

int main(int argc, char* argv[])
{
    const char* mount0;
    const char* mount1;
    unsigned int peer_node;
    char filepath0[512], filepath1[512];
    char filename[64];
    int fd0, fd1, ret;
    __u64 data_size = 128ULL * 1024 * 1024; /* 128MB region */
    void* map;
    pid_t my_pid = getpid();

    if (argc < 4)
    {
        fprintf(stderr,
                "Usage: %s <owner_mount> <reader_mount> <peer_node_id>\n"
                "\n"
                "Example:\n"
                "  %s /mnt/marufs /mnt/marufs2 2\n",
                argv[0], argv[0]);
        return 1;
    }

    mount0 = argv[1];
    mount1 = argv[2];
    peer_node = (unsigned)strtoul(argv[3], NULL, 10);

    snprintf(filename, sizeof(filename), "mmap_cuda_%d", (int)my_pid);
    snprintf(filepath0, sizeof(filepath0), "%s/%s", mount0, filename);
    snprintf(filepath1, sizeof(filepath1), "%s/%s", mount1, filename);
    unlink(filepath0);

    printf("=== MARUFS mmap & cudaHostRegister Permission Tests ===\n");
    printf("  owner mount: %s (this node)\n", mount0);
    printf("  reader mount: %s (node %u)\n", mount1, peer_node);
    printf("  filename: %s\n", filename);
    printf("  pid: %d\n\n", (int)my_pid);

    /* Try to load CUDA runtime */
    printf("[0] CUDA detection\n");
    if (cuda_init() == 0)
        printf("  CUDA runtime loaded successfully\n");
    else
        printf("  CUDA not available — GPU tests will be skipped\n");

    /* ============================================================
     * [1] Owner: create region + mmap(PROT_WRITE)
     * ============================================================ */
    printf("\n[1] Owner: create region + mmap(PROT_WRITE)\n");

    fd0 = open(filepath0, O_CREAT | O_RDWR, 0644);
    TEST("owner open(O_CREAT|O_RDWR)", fd0 >= 0);
    if (fd0 < 0)
        goto out;

    ret = ftruncate(fd0, (__off_t)data_size);
    TEST("owner ftruncate(128MB)", ret == 0);
    if (ret)
        goto out_close0;

    /* Grant READ to peer (no WRITE yet) */
    ret = do_grant(fd0, peer_node, my_pid, MARUFS_PERM_READ);
    TEST("grant READ to peer", ret == 0);

    /* Owner mmap PROT_WRITE */
    map = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd0, 0);
    TEST("owner mmap(PROT_READ|PROT_WRITE)", map != MAP_FAILED);
    if (map == MAP_FAILED)
        goto out_close0;

    /* Write a pattern */
    {
        unsigned int* p = (unsigned int*)map;
        size_t i;
        for (i = 0; i < SLOT_SIZE / sizeof(unsigned int); i++)
            p[i] = PATTERN_A ^ (unsigned int)i;
    }
    TEST("owner write pattern via mmap", 1);

    /* ============================================================
     * [2] Owner: cudaHostRegister on PROT_WRITE mapping
     * ============================================================ */
    printf("\n[2] Owner: cudaHostRegister on PROT_WRITE mapping\n");
    try_cuda_register(map, SLOT_SIZE, "owner cudaHostRegister(PROT_WRITE)");

    __builtin_ia32_sfence();  /* Flush WC buffers to memory */
    munmap(map, SLOT_SIZE);

    /* ============================================================
     * [3] Reader (READ only): mmap(PROT_READ) → success
     * ============================================================ */
    printf("\n[3] Reader (READ perm): mmap(PROT_READ)\n");

    fd1 = open(filepath1, O_RDONLY);
    TEST("reader open(O_RDONLY)", fd1 >= 0);
    if (fd1 < 0)
        goto out_close0;

    map = mmap(NULL, SLOT_SIZE, PROT_READ, MAP_SHARED, fd1, 0);
    TEST("reader mmap(PROT_READ) with READ perm", map != MAP_FAILED);

    if (map != MAP_FAILED)
    {
        /* Verify data */
        unsigned int* p = (unsigned int*)map;
        int ok = (p[0] == (PATTERN_A ^ 0u)) && (p[1] == (PATTERN_A ^ 1u));
        TEST("reader sees owner's pattern", ok);

        /* ============================================================
         * [5] Reader: cudaHostRegister on PROT_READ mapping → fail
         * ============================================================ */
        printf("\n[5] Reader (READ perm): cudaHostRegister on PROT_READ\n");
        try_cuda_register_expect_fail(map, SLOT_SIZE,
                                      "reader cudaHostRegister(PROT_READ, default) fails");

        /* ============================================================
         * [5b] Reader: cudaHostRegisterReadOnly on PROT_READ → probe
         *
         * Key test: does ReadOnly flag skip FOLL_WRITE internally?
         * If YES → reader can do GPU DMA read without WRITE perm
         * If NO  → same catch-22 as default flag
         * ============================================================ */
        printf("\n[5b] Reader (READ perm): cudaHostRegisterReadOnly on PROT_READ\n");
        try_cuda_register_readonly(map, SLOT_SIZE,
                                   "reader cudaHostRegisterReadOnly(PROT_READ)");

        munmap(map, SLOT_SIZE);
    }

    /* ============================================================
     * [4] Reader (READ only): mmap(PROT_WRITE) → EACCES
     *
     * O_RDONLY fd → PROT_WRITE rejected by WORM check (FMODE_WRITE)
     * ============================================================ */
    printf("\n[4] Reader (READ perm): mmap(PROT_WRITE) on O_RDONLY → EACCES\n");

    errno = 0;
    map = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd1, 0);
    TEST("reader mmap(PROT_WRITE) denied on O_RDONLY fd",
         map == MAP_FAILED && errno == EACCES);
    if (map != MAP_FAILED)
        munmap(map, SLOT_SIZE);

    close(fd1);

    /* [4b] Reader open(O_RDWR) with READ-only perm → open succeeds,
     * but mmap(PROT_WRITE) denied (permission enforced on data access) */
    printf("\n[4b] Reader (READ perm): open(O_RDWR) ok, mmap(PROT_WRITE) → EACCES\n");
    errno = 0;
    fd1 = open(filepath1, O_RDWR);
    TEST("reader open(O_RDWR) succeeds (open always allowed)", fd1 >= 0);
    if (fd1 >= 0)
    {
        void* map_rw = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd1, 0);
        TEST("reader mmap(PROT_WRITE) denied (READ-only perm)",
             map_rw == MAP_FAILED && errno == EACCES);
        if (map_rw != MAP_FAILED)
            munmap(map_rw, SLOT_SIZE);
        close(fd1);
    }

    /* ============================================================
     * [6] Grant READ+WRITE → reader mmap(PROT_WRITE) → success
     * ============================================================ */
    printf("\n[6] Reader (READ+WRITE perm): mmap(PROT_WRITE)\n");

    ret = do_grant(fd0, peer_node, my_pid, MARUFS_PERM_READ | MARUFS_PERM_WRITE);
    TEST("grant READ+WRITE to peer", ret == 0);

    fd1 = open(filepath1, O_RDWR);
    TEST("reader re-open(O_RDWR)", fd1 >= 0);
    if (fd1 < 0)
        goto out_close0;

    map = mmap(NULL, SLOT_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd1, 0);
    TEST("reader mmap(PROT_WRITE) with READ+WRITE perm", map != MAP_FAILED);

    if (map != MAP_FAILED)
    {
        /* Verify data readable */
        unsigned int* p = (unsigned int*)map;
        int ok = (p[0] == (PATTERN_A ^ 0u)) && (p[1] == (PATTERN_A ^ 1u));
        TEST("reader sees owner's pattern via WRITE mapping", ok);

        /* ============================================================
         * [7] Reader: cudaHostRegister on PROT_WRITE mapping → success
         * ============================================================ */
        printf("\n[7] Reader (READ+WRITE perm): cudaHostRegister on PROT_WRITE\n");
        try_cuda_register(map, SLOT_SIZE,
                          "reader cudaHostRegister(PROT_WRITE, default) succeeds");

        /* ============================================================
         * [7b] Reader: cudaHostRegisterReadOnly on PROT_WRITE → success
         *
         * Baseline: ReadOnly flag on writable mapping should always work.
         * ============================================================ */
        printf("\n[7b] Reader (READ+WRITE perm): cudaHostRegisterReadOnly on PROT_WRITE\n");
        try_cuda_register_readonly(map, SLOT_SIZE,
                                   "reader cudaHostRegisterReadOnly(PROT_WRITE)");

        munmap(map, SLOT_SIZE);
    }

    close(fd1);

    /* ============================================================
     * Summary
     * ============================================================ */
    printf("\n============================================\n");
    printf("=== Total: %d passed, %d failed, %d skipped ===\n",
           pass_count, fail_count, skip_count);
    printf("============================================\n");

    unlink(filepath0);
    close(fd0);
    cuda_cleanup();
    return fail_count > 0 ? 1 : 0;

out_close0:
    unlink(filepath0);
    close(fd0);
out:
    cuda_cleanup();
    printf("\n=== ABORTED: %d passed, %d failed ===\n", pass_count, fail_count);
    return 1;
}
