## test_utils.nim — shared test helpers: timed template + watchdog thread

import std/[times, atomics, os, locks, strformat, strutils]

when defined(nimPanics):
  {.warning: "tests compiled with --panics:on; AssertionDefect is uncatchable and FAIL line won't print. Set `switch(\"panics\", \"off\")` in config.nims.".}

let gTimeoutSec* = block:
  let e = getEnv("BLOCKY_TEST_TIMEOUT", "10")
  try: parseFloat(e) except ValueError: 10.0

var gDeadline: Atomic[float64]
var gLabel: string
var gLock: Lock
initLock(gLock)

# Current test context — read by onUnhandledException when a test crashes.
var gCurrentId: string
var gCurrentDesc: string
var gCurrentT0: float64
var gTestActive: bool

proc watchdog {.thread, gcsafe.} =
  while true:
    sleep(50)
    let dl = gDeadline.load(moRelaxed)
    if dl > 0.0 and epochTime() > dl:
      var msg: string
      withLock(gLock):
        {.gcsafe.}: msg = gLabel & &"\tFAIL\tTIMEOUT (>{gTimeoutSec} s)"
      stderr.writeLine(msg)
      quit(1)

var gWatchdogThread: Thread[void]
createThread(gWatchdogThread, watchdog)

# Hook invoked on any unhandled exception (including AssertionDefect when
# --panics:off). Runs before Nim's default stack-trace-and-quit behavior.
system.onUnhandledException = proc(errorMsg: string) {.nimcall, gcsafe, raises: [].} =
  try:
    {.gcsafe.}:
      let el = epochTime() - gCurrentT0
      stderr.writeLine(gCurrentId & "\tFAIL\t" &
                        formatFloat(el, ffDecimal, 2) & " s\t" & gCurrentDesc)
      stderr.writeLine(errorMsg.strip)
  except:
    discard

template timed*(id, desc: string, body: untyped) =
  block:
    withLock(gLock):
      gLabel = id & " - " & desc
    gCurrentId = id
    gCurrentDesc = desc
    gCurrentT0 = epochTime()
    gDeadline.store(epochTime() + gTimeoutSec, moRelaxed)
    body
    gDeadline.store(0.0, moRelaxed)
    let el = epochTime() - gCurrentT0
    echo id & "\tPASS\t" & formatFloat(el, ffDecimal, 2) & " s\t" & desc
