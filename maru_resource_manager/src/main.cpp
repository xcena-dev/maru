#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <cstdlib>

#include "log.h"
#include "pool_manager.h"
#include "reaper.h"
#include "uds_server.h"

static volatile std::sig_atomic_t gStop = 0;
static volatile std::sig_atomic_t gRescan = 0;

static void onSignal(int)
{
    gStop = 1;
}
static void onRescan(int)
{
    gRescan = 1;
}

int main(int argc, char **argv)
{
    std::signal(SIGINT, onSignal);
    std::signal(SIGTERM, onSignal);
    std::signal(SIGHUP, onRescan);

    maru::setLogLevel(maru::LogLevel::Debug);

    maru::PoolManager pm;
    int rc = pm.loadPools();
    if (rc != 0)
    {
        std::fprintf(stderr, "maru-resource-manager: failed to load pools: %d\n", rc);
    }

    maru::UdsServer server(pm);
    rc = server.start();
    if (rc != 0)
    {
        std::fprintf(stderr, "maru-resource-manager: failed to start server: %d\n", rc);
        return 1;
    }

    maru::Reaper reaper(pm);
    reaper.start();

    while (!gStop)
    {
        if (gRescan)
        {
            gRescan = 0;
            pm.rescanDevices();
        }
        ::sleep(1);
    }

    reaper.stop();
    server.stop();
    return 0;
}
