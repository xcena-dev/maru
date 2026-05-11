// SPDX-License-Identifier: Apache-2.0
#define _GNU_SOURCE
/*
 * test_postexec_attack.c - Empirical PoC for post-exec privilege retention.
 *
 * Question: after a process gets an ACCESS_GRANT for a marufs region and
 * then execve()s into a different binary, can the new binary reach the
 * region via the inherited fd (i.e., does the cached RAT delegation
 * remain effective for the PID across exec)?
 *
 * Modes:
 *   - Parent (default):  open + ftruncate + mmap + write magic, then
 *                        fork + execve self with --attacker <fd>
 *   - Attacker:          inherited fd → mmap → read magic
 *
 * Exit codes:
 *   0  defended (attacker cannot mmap or fd not inherited)
 *   2  ATTACK SUCCEEDED — region content visible to new binary (vuln)
 *   1+ setup error / unexpected
 *
 * Usage: ./test_postexec_attack <mount>
 */

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#define REGION_SIZE   (2ULL * 1024 * 1024)
#define MAGIC_BYTE    0xAB
#define MAGIC_LEN     16

static int run_attacker(int fd)
{
    fprintf(stderr, "[attacker] entered post-exec, pid=%d, fd=%d "
                    "(same PID as pre-exec — execve preserves PID/start_boottime)\n",
            getpid(), fd);

    /* Step 1: is the fd actually inherited? */
    int rc = fcntl(fd, F_GETFD);
    if (rc < 0)
    {
        fprintf(stderr, "[attacker] fd not inherited (closed): %s\n",
                strerror(errno));
        goto cleanup_defended;
    }
    fprintf(stderr, "[attacker] fd inherited (FD_CLOEXEC=%d)\n",
            (rc & FD_CLOEXEC) ? 1 : 0);

    /* Step 2: try to mmap the region. RAT delegation for
     * (node_id, pid, birth_time) was written when the PRIOR binary
     * mmap'd; PID and start_boottime are unchanged by execve, so if
     * marufs treats the cache as still valid, mmap will succeed
     * without a fresh attestation. */
    errno = 0;
    void* map = mmap(NULL, REGION_SIZE, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED)
    {
        fprintf(stderr, "[attacker] mmap FAILED: %s (defended)\n",
                strerror(errno));
        goto cleanup_defended;
    }

    /* Step 3: read the parent's magic — proof of access */
    unsigned char* p = (unsigned char*)map;
    int matches = 0;
    for (int i = 0; i < MAGIC_LEN; i++)
        if (p[i] == MAGIC_BYTE)
            matches++;

    fprintf(stderr,
            "[attacker] mmap SUCCEEDED at %p, magic match: %d/%d bytes\n",
            map, matches, MAGIC_LEN);

    munmap(map, REGION_SIZE);

    /* Cleanup: unlink the test region file */
    const char* path = getenv("MARUFS_POSTEXEC_PATH");
    if (path)
        unlink(path);
    close(fd);

    if (matches == MAGIC_LEN)
    {
        fprintf(stderr,
                "\n[attacker] *** VULNERABILITY CONFIRMED ***\n"
                "  new binary (post-exec) successfully read region\n"
                "  via inherited fd + cached RAT delegation\n");
        return 2;
    }

    fprintf(stderr,
            "[attacker] mmap'd but content mismatched (partial defense?)\n");
    return 3;

cleanup_defended:
    {
        const char* path = getenv("MARUFS_POSTEXEC_PATH");
        if (path)
            unlink(path);
        close(fd);
    }
    fprintf(stderr, "\n[attacker] *** DEFENDED ***\n");
    return 0;
}

int main(int argc, char* argv[])
{
    /* Attacker mode (post-exec) ---------------------------------- */
    if (argc >= 3 && strcmp(argv[1], "--attacker") == 0)
    {
        int fd = atoi(argv[2]);
        return run_attacker(fd);
    }

    /* Parent mode ------------------------------------------------ */
    if (argc < 2)
    {
        fprintf(stderr,
                "Usage: %s <mount> [--cloexec]\n"
                "  default     : open without O_CLOEXEC (tests Option A — kernel must reject open)\n"
                "  --cloexec   : open with O_CLOEXEC (tests Option A path — fd must auto-close on execve)\n",
                argv[0]);
        return 1;
    }

    int open_flags = O_CREAT | O_RDWR;
    bool use_cloexec = false;
    for (int i = 2; i < argc; i++)
    {
        if (strcmp(argv[i], "--cloexec") == 0)
        {
            open_flags |= O_CLOEXEC;
            use_cloexec = true;
        }
    }

    char path[512];
    snprintf(path, sizeof(path), "%s/postexec_attack_%d", argv[1], getpid());
    unlink(path); /* pre-clean */

    fprintf(stderr,
            "[parent] pid=%d path=%s open_flags=%s\n", getpid(), path,
            use_cloexec ? "O_CREAT|O_RDWR|O_CLOEXEC" : "O_CREAT|O_RDWR (no CLOEXEC)");

    int fd = open(path, open_flags, 0644);
    if (fd < 0)
    {
        fprintf(stderr, "[parent] open: %s\n", strerror(errno));
        return 1;
    }

    if (ftruncate(fd, (__off_t)REGION_SIZE) != 0)
    {
        fprintf(stderr, "[parent] ftruncate: %s\n", strerror(errno));
        close(fd);
        unlink(path);
        return 1;
    }

    /* mmap → triggers ATTEST/ACCESS upcall → RAT delegation entry written */
    void* map =
        mmap(NULL, REGION_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED)
    {
        fprintf(stderr, "[parent] mmap: %s\n", strerror(errno));
        close(fd);
        unlink(path);
        return 1;
    }

    /* Write magic so attacker can verify it can read */
    memset(map, MAGIC_BYTE, MAGIC_LEN);
    msync(map, MAGIC_LEN, MS_SYNC);
    munmap(map, REGION_SIZE);

    fprintf(stderr,
            "[parent] mmap done, RAT delegation should be active for pid=%d\n",
            getpid());

    /* IMPORTANT: do NOT fork. We need execve in-place so PID and
     * start_boottime are preserved — that's what makes this a real
     * post-exec privilege retention test. fork() would change PID
     * which trivially defeats the attack via RAT (pid, birth_time)
     * mismatch. */
    char fd_str[16];
    snprintf(fd_str, sizeof(fd_str), "%d", fd);

    /* Pass cleanup info via env so the post-exec binary knows what to unlink. */
    setenv("MARUFS_POSTEXEC_PATH", path, 1);

    fprintf(stderr,
            "[parent] execve into self --attacker (PID/start_boottime preserved)\n");
    execl("/proc/self/exe", argv[0], "--attacker", fd_str, (char*)NULL);

    /* unreachable on success */
    perror("execve");
    close(fd);
    unlink(path);
    return 1;
}
