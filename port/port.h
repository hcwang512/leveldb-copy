//
// Created by 王洪城 on 2024/4/27.
//

#ifndef LEVELDB_COPY_PORT_H
#define LEVELDB_COPY_PORT_H

#include <string.h>

#if defined(LEVELDB_PLATFORM_POSIX) || defined(LEVELDB_PLATFORM_WINDOWS)
#include "port/port_stdcxx.h"
#elif defined(LEVELDB_PLATFORM_CHROMIUM)
#include "port/port_chromium.h"
#endif

#endif //LEVELDB_COPY_PORT_H
