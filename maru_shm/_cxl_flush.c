/* SPDX-License-Identifier: Apache-2.0
 * Copyright 2026 XCENA Inc.
 *
 * CPU cache-line flush helper for CXL shared memory (DEV_DAX).
 *
 * DEV_DAX mappings are write-back cached, and msync()/mmap.flush() does not
 * touch CPU caches there (device-dax has no page cache and no fsync op).
 * CXL 2.x has no cross-host cache coherence, so data written by CPU store
 * would otherwise sit in this host's cache, invisible to other hosts sharing
 * the device, and a CPU read leaves a copy that goes stale when another host
 * rewrites the same range. clflush covers both: it writes back dirty lines
 * (writer side) and invalidates clean copies (reader side); mfence orders it
 * against the surrounding loads and stores. clflush is preferred over
 * clflushopt/clwb: available on every x86-64 without CPUID dispatch, and the
 * flushed ranges are tiny (header = one cache line) so throughput is
 * irrelevant.
 */
#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdint.h>

#if defined(__x86_64__) || defined(__i386__)
#include <emmintrin.h>
#define MARU_HAVE_CLFLUSH 1
#else
#define MARU_HAVE_CLFLUSH 0
#endif

#define MARU_CACHE_LINE 64

static PyObject *flush_range(PyObject *self, PyObject *args) {
    Py_buffer view;
    Py_ssize_t offset = 0;
    Py_ssize_t length = -1;

    (void)self;

    if (!PyArg_ParseTuple(args, "y*|nn:flush_range", &view, &offset, &length))
        return NULL;

    if (length < 0)
        length = view.len - offset;

    if (offset < 0 || length < 0 || offset > view.len - length) {
        PyBuffer_Release(&view);
        PyErr_SetString(PyExc_ValueError,
                        "flush_range: offset/length out of bounds");
        return NULL;
    }

#if MARU_HAVE_CLFLUSH
    {
        uintptr_t addr = (uintptr_t)view.buf + (uintptr_t)offset;
        uintptr_t end = addr + (uintptr_t)length;
        uintptr_t line = addr & ~(uintptr_t)(MARU_CACHE_LINE - 1);
        for (; line < end; line += MARU_CACHE_LINE)
            _mm_clflush((const void *)line);
        _mm_mfence();
    }
#endif

    PyBuffer_Release(&view);
    Py_RETURN_NONE;
}

static PyMethodDef cxl_flush_methods[] = {
    {"flush_range", flush_range, METH_VARARGS,
     "flush_range(buffer, offset=0, length=-1)\n\n"
     "Write back and invalidate the CPU cache lines covering\n"
     "buffer[offset:offset+length] (clflush per 64B line, then mfence).\n"
     "No-op on architectures without clflush (see HAVE_CLFLUSH)."},
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef cxl_flush_module = {
    PyModuleDef_HEAD_INIT,
    "_cxl_flush",
    "CPU cache flush for CXL DEV_DAX mappings.",
    -1,
    cxl_flush_methods,
    NULL,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC PyInit__cxl_flush(void) {
    PyObject *m = PyModule_Create(&cxl_flush_module);
    if (m == NULL)
        return NULL;
    if (PyModule_AddIntConstant(m, "HAVE_CLFLUSH", MARU_HAVE_CLFLUSH) < 0) {
        Py_DECREF(m);
        return NULL;
    }
    return m;
}
